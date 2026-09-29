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
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Logging;
using QuantConnect.Orders;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Live tests for the open orders the brokerage rebuilds when an algorithm starts
    /// </summary>
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersOpenOrderPropertiesTests
    {
        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new NUnitLogHandler();
        }

        [TestCase(OrderType.Limit, 100)]
        [TestCase(OrderType.ComboLimit, 0.05)]
        public void GetOpenOrdersAfterRestartKeepsOutsideRegularTradingHours(OrderType orderType, decimal limitPrice)
        {
            // Arrange
            var orders = CreateOrders(orderType, limitPrice);
            var brokerageOrderId = PlaceOrders(orders);

            // the algorithm restarts: the brokerage rebuilds the open orders it finds at IB
            var orderProvider = new OrderProvider();
            using var brokerage = CreateBrokerage(orderProvider);

            // Act
            var rebuiltOrders = new List<Order>();
            foreach (var order in brokerage.GetOpenOrders())
            {
                if (order.BrokerId.Contains(brokerageOrderId))
                {
                    orderProvider.Add(order);
                    rebuiltOrders.Add(order);
                }
            }

            // the legs of a combo order share the brokerage order
            if (rebuiltOrders.Count > 0)
            {
                brokerage.CancelOrder(rebuiltOrders[0]);
            }

            // Assert
            Assert.That(rebuiltOrders, Has.Count.EqualTo(orders.Count));
            foreach (var order in rebuiltOrders)
            {
                Assert.That(order.Properties, Is.InstanceOf<InteractiveBrokersOrderProperties>());
                Assert.That(((InteractiveBrokersOrderProperties)order.Properties).OutsideRegularTradingHours, Is.True);
            }
        }

        /// <summary>
        /// Creates a resting buy order with the outside regular trading hours flag: SPY or a SPXW call spread
        /// </summary>
        private static List<Order> CreateOrders(OrderType orderType, decimal limitPrice)
        {
            var orderProperties = new InteractiveBrokersOrderProperties { OutsideRegularTradingHours = true, TimeInForce = TimeInForce.Day };
            if (orderType == OrderType.Limit)
            {
                return new List<Order> { new LimitOrder(Symbols.SPY, 1, limitPrice, DateTime.UtcNow, properties: orderProperties) };
            }

            // IB ignores the flag on a SPY option combo (warning 2109) and keeps it on an index option combo
            var expiry = new DateTime(2026, 10, 2);
            var longCall = Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.European, OptionRight.Call, 7700m, expiry);
            var shortCall = Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.European, OptionRight.Call, 7750m, expiry);
            var groupOrderManager = new GroupOrderManager(legCount: 2, quantity: 1, limitPrice: limitPrice);
            return new List<Order>
            {
                new ComboLimitOrder(longCall, 1, limitPrice, DateTime.UtcNow, groupOrderManager, properties: orderProperties),
                new ComboLimitOrder(shortCall, -1, limitPrice, DateTime.UtcNow, groupOrderManager, properties: orderProperties)
            };
        }

        /// <summary>
        /// Places the orders, waits until IB has them and stops the brokerage like the end of an algorithm does
        /// </summary>
        /// <returns>The brokerage id the orders share</returns>
        private static string PlaceOrders(List<Order> orders)
        {
            var orderProvider = new OrderProvider();
            using var brokerage = CreateBrokerage(orderProvider);

            var pendingOrderIds = new HashSet<int>();
            foreach (var order in orders)
            {
                orderProvider.Add(order);
                pendingOrderIds.Add(order.Id);
            }

            using var submittedEvent = new ManualResetEvent(false);
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                lock (pendingOrderIds)
                {
                    foreach (var orderEvent in orderEvents)
                    {
                        if (orderEvent.Status == OrderStatus.Submitted)
                        {
                            pendingOrderIds.Remove(orderEvent.OrderId);
                        }
                    }

                    if (pendingOrderIds.Count == 0)
                    {
                        submittedEvent.Set();
                    }
                }
            };

            foreach (var order in orders)
            {
                Assert.That(brokerage.PlaceOrder(order), Is.True, $"The brokerage refused the order: {order}");
            }
            Assert.That(submittedEvent.WaitOne(TimeSpan.FromSeconds(30)), Is.True, "OrderStatus Submitted was not encountered within the timeout");

            return orders[0].BrokerId[0];
        }

        private static InteractiveBrokersBrokerage CreateBrokerage(OrderProvider orderProvider)
        {
            var brokerage = new InteractiveBrokersBrokerage(new QCAlgorithm(), orderProvider, new SecurityProvider());
            brokerage.Connect();
            return brokerage;
        }
    }
}
