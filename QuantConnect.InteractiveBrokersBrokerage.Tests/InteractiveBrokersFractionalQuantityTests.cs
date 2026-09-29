/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Tests.Brokerages;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Live tests for fractional share quantities coming from outside Lean. IB refuses to place or modify fractional orders
    /// from the API (10243, 10242), so they are placed by hand in TWS before the run:
    /// - <see cref="FractionalPositionIsLoadedExactly"/>: hold exactly <see cref="AaplPosition"/> AAPL
    /// - <see cref="FractionalOpenOrderIsRebuiltExactly"/>: one open AAPL buy limit of <see cref="FractionalQuantity"/> below the market
    /// - <see cref="FractionalOpenOrderFillIsExact"/>: open AAPL limit orders with fractional quantities near the market, filled during the run
    /// TWS must be logged out during the run: the gateway uses the same IB user. <see cref="InteractiveBrokersFractionalFillTests"/> covers the fill without a gateway.
    /// </summary>
    [TestFixture, Explicit("Live: needs AAPL fractional position and order placed by hand in TWS")]
    public class InteractiveBrokersFractionalQuantityTests
    {
        private const decimal FractionalQuantity = 0.4m;

        // the AAPL position held in the account before the run, bought in TWS
        private const decimal AaplPosition = 0.8m;

        private static readonly TimeSpan FillTimeout = TimeSpan.FromMinutes(10);

        private static readonly FieldInfo ClientField =
            typeof(InteractiveBrokersBrokerage).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic);

        private readonly List<Order> _orders = new();
        private OrderProvider _orderProvider;

        [SetUp]
        public void SetUp()
        {
            _orders.Clear();
            _orderProvider = new OrderProvider(_orders);
        }

        /// <summary>
        /// A fractional position bought outside Lean is loaded with its exact quantity when the algorithm starts.
        /// Covers <c>updatePortfolio</c> / <c>positionMulti</c> -> <c>UpdatePortfolioEventArgs.Position</c> -> <c>CreateHolding</c>.
        /// </summary>
        [Test]
        public void FractionalPositionIsLoadedExactly()
        {
            using var brokerage = CreateBrokerage();

            var holdings = brokerage.GetAccountHoldings()
                .Where(h => h.Symbol.SecurityType == SecurityType.Equity && h.Symbol.Value == "AAPL")
                .ToList();
            Log.Trace($"FractionalPositionIsLoadedExactly(): AAPL holdings: {string.Join(", ", holdings)}");

            Assert.AreEqual(AaplPosition, holdings.Sum(h => h.Quantity), "AAPL position loaded by Lean");
        }

        /// <summary>
        /// A fractional order placed outside Lean is rebuilt with its exact quantity when the algorithm starts.
        /// Covers <c>ConvertOrders</c> (<c>ibOrder.TotalQuantity</c>).
        /// </summary>
        [Test]
        public void FractionalOpenOrderIsRebuiltExactly()
        {
            using var brokerage = CreateBrokerage();

            // match on symbol and type only: a rounded quantity of 0 would also lose the order direction
            var rebuiltOrder = brokerage.GetOpenOrders().SingleOrDefault(o => o.Symbol.Value == "AAPL" && o.Type == OrderType.Limit);
            Assert.IsNotNull(rebuiltOrder, "Place one AAPL buy limit order for 0.4 shares below the market in TWS before the run.");
            Log.Trace($"FractionalOpenOrderIsRebuiltExactly(): rebuilt order: {rebuiltOrder}");

            Assert.AreEqual(FractionalQuantity, rebuiltOrder.Quantity, "Rebuilt open order quantity");
        }

        /// <summary>
        /// Fractional orders placed outside Lean, rebuilt when the algorithm starts, report their fills with the exact quantity.
        /// Needs open AAPL limit orders near the market, so it reaches at least one within <see cref="FillTimeout"/>.
        /// The expected quantities are the exact ones IB sends for each order.
        /// Covers <c>ConvertOrders</c> and the fill (<c>Execution.Shares</c>, <c>Execution.CumQty</c>).
        /// </summary>
        [Test]
        public void FractionalOpenOrderFillIsExact()
        {
            using var brokerage = CreateBrokerage();
            var client = (IB.InteractiveBrokersClient)ClientField.GetValue(brokerage);

            // keep the raw IB orders: their exact quantity is the expected one
            var ibQuantities = new ConcurrentDictionary<int, decimal>();
            EventHandler<IB.OpenOrderEventArgs> onOpenOrder = (_, e) =>
                ibQuantities[e.OrderId] = e.Order.Action == "SELL" ? -e.Order.TotalQuantity : e.Order.TotalQuantity;
            client.OpenOrder += onOpenOrder;
            var rebuiltOrders = brokerage.GetOpenOrders().Where(o => o.Symbol.Value == "AAPL" && o.Type == OrderType.Limit).ToList();
            client.OpenOrder -= onOpenOrder;
            Assert.IsNotEmpty(rebuiltOrders, "Place AAPL limit orders with fractional quantities near the market in TWS before the run.");

            var expectedQuantities = new Dictionary<int, decimal>();
            var orderEvents = new ConcurrentDictionary<int, List<OrderEvent>>();
            foreach (var rebuiltOrder in rebuiltOrders)
            {
                // the transaction handler adds the rebuilt open orders on start, so the fills can find them
                _orderProvider.Add(rebuiltOrder);
                expectedQuantities[rebuiltOrder.Id] = ibQuantities[int.Parse(rebuiltOrder.BrokerId.Single(), CultureInfo.InvariantCulture)];
                orderEvents[rebuiltOrder.Id] = new List<OrderEvent>();
                Log.Trace($"FractionalOpenOrderFillIsExact(): rebuilt order: {rebuiltOrder}, IB quantity: {expectedQuantities[rebuiltOrder.Id]}");
            }

            var allFilled = new ManualResetEventSlim(false);
            brokerage.OrdersStatusChanged += (_, events) =>
            {
                foreach (var orderEvent in events.Where(e => orderEvents.ContainsKey(e.OrderId)))
                {
                    Log.Trace($"FractionalOpenOrderFillIsExact(): {orderEvent}");
                    orderEvents[orderEvent.OrderId].Add(orderEvent);
                }
                if (orderEvents.Values.All(list => list.Any(e => e.Status == OrderStatus.Filled)))
                {
                    allFilled.Set();
                }
            };

            Log.Trace($"FractionalOpenOrderFillIsExact(): waiting up to {FillTimeout.TotalMinutes} minutes for the market to fill the {rebuiltOrders.Count} orders");
            allFilled.Wait(FillTimeout);

            var filledOrders = rebuiltOrders.Where(o => orderEvents[o.Id].Any(e => e.Status is OrderStatus.Filled or OrderStatus.PartiallyFilled)).ToList();
            Log.Trace($"FractionalOpenOrderFillIsExact(): {filledOrders.Count} of {rebuiltOrders.Count} orders got fills");

            Assert.Multiple(() =>
            {
                Assert.IsNotEmpty(filledOrders, "No AAPL order got a fill in time");
                foreach (var rebuiltOrder in rebuiltOrders)
                {
                    Assert.AreEqual(expectedQuantities[rebuiltOrder.Id], rebuiltOrder.Quantity, $"Rebuilt quantity of IB order {rebuiltOrder.BrokerId.Single()}");
                }
                foreach (var filledOrder in filledOrders)
                {
                    var fills = orderEvents[filledOrder.Id].Where(e => e.Status is OrderStatus.Filled or OrderStatus.PartiallyFilled).ToList();
                    if (fills.Last().Status == OrderStatus.Filled)
                    {
                        Assert.AreEqual(expectedQuantities[filledOrder.Id], fills.Sum(e => e.FillQuantity), $"Filled quantity of IB order {filledOrder.BrokerId.Single()}");
                    }
                    Assert.IsTrue(fills.All(e => e.FillQuantity != 0), $"IB order {filledOrder.BrokerId.Single()} has a fill of 0 shares");
                }
            });
        }

        private InteractiveBrokersBrokerage CreateBrokerage()
        {
            var brokerage = new InteractiveBrokersBrokerage(new QCAlgorithm(), _orderProvider, new SecurityProvider());
            brokerage.Connect();
            Assert.IsTrue(brokerage.IsConnected);
            return brokerage;
        }
    }
}
