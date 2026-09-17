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
using System.Linq;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine.DataFeeds;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Places real cryptocurrency orders on the paper account, including the "Cryptocurrency order confirmation"
    /// disclosure IBAutomater has to confirm. Requests no history, so it stays clear of IB's pacing limits.
    /// </summary>
    [TestFixture, Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersCryptoOrderTests
    {
        private static readonly Symbol _btcusd = Symbol.Create("BTCUSD", SecurityType.Crypto, Market.Coinbase);
        private static readonly TimeSpan _orderTimeout = TimeSpan.FromSeconds(90);

        /// <summary>
        /// The buy is sized by cash, so it fills a different quantity than requested and must still close as
        /// filled. An approximate price is enough, so no crypto data subscription is needed. Sells back what it bought.
        /// </summary>
        [Test]
        public void BuysAtTheMarketForTheCashAmountAndSellsBack()
        {
            const decimal referencePrice = 77000m;
            const decimal cashAmount = 20m;

            var algorithm = new AlgorithmStub();
            algorithm.SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage);
            var security = algorithm.AddCrypto(_btcusd.Value);
            security.SetMarketPrice(new Tick(DateTime.UtcNow, security.Symbol, referencePrice, referencePrice));

            var orderProvider = new OrderProvider();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, orderProvider, new SecurityProvider());
            var session = Listen(brokerage, "CryptoMarket");
            brokerage.Connect();

            var buy = new MarketOrder(security.Symbol, cashAmount / referencePrice, DateTime.UtcNow);
            orderProvider.Add(buy);
            var buyEvents = PlaceAndWaitForFinalStatus(brokerage, session, buy);

            var fills = buyEvents.Where(x => x.Status is OrderStatus.Filled or OrderStatus.PartiallyFilled).ToList();
            Assert.IsNotEmpty(fills, $"no fill, statuses [{string.Join(",", buyEvents.Select(x => x.Status))}]");
            Assert.AreEqual(OrderStatus.Filled, buyEvents.Last().Status, "IB reported the cash spent, the order has to close as filled");

            var filledQuantity = fills.Sum(x => x.FillQuantity);
            var filledValue = fills.Sum(x => x.FillQuantity * x.FillPrice);
            Log.Trace($"CryptoMarket: bought {filledQuantity} for {filledValue} USD");
            Assert.Greater(filledQuantity, 0);
            // the cash is truncated to whole cents
            Assert.LessOrEqual(filledValue, cashAmount + 0.01m);

            foreach (var holding in brokerage.GetAccountHoldings().Where(x => x.Symbol.SecurityType == SecurityType.Crypto))
            {
                Log.Trace($"CryptoMarket: holding {holding.Symbol.Value} quantity={holding.Quantity} averagePrice={holding.AveragePrice}");
            }

            var sell = new MarketOrder(security.Symbol, -filledQuantity, DateTime.UtcNow);
            orderProvider.Add(sell);
            var sellEvents = PlaceAndWaitForFinalStatus(brokerage, session, sell);
            Assert.AreEqual(OrderStatus.Filled, sellEvents.LastOrDefault()?.Status, $"statuses [{string.Join(",", sellEvents.Select(x => x.Status))}]");
            Assert.AreEqual(-filledQuantity, sellEvents.Where(x => x.Status is OrderStatus.Filled or OrderStatus.PartiallyFilled).Sum(x => x.FillQuantity));
        }

        /// <summary>
        /// Priced just above the market so it fills at the ask: IB cancels a crypto buy limit further than
        /// 10 dollars or 0.25% from the best ask, so a resting order cannot be used here
        /// </summary>
        [Test]
        public void BuysWithAMarketableLimitOrderAndSellsBack()
        {
            const decimal quantity = 0.0002m;

            var algorithm = new AlgorithmStub();
            algorithm.SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage);
            algorithm.AddCrypto(_btcusd.Value);

            var orderProvider = new OrderProvider();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, orderProvider, new SecurityProvider());
            var session = Listen(brokerage, "CryptoLimit");
            brokerage.Connect();

            var price = WaitForPrice(brokerage, _btcusd, TimeSpan.FromSeconds(30));
            Assert.Greater(price, 0, $"no price for {_btcusd.Value}");

            // 0.1% above the reference, inside IB's band and above the ask, rounded to IB's tick
            var tick = SymbolPropertiesDatabase.FromDataFolder()
                .GetSymbolProperties(Market.InteractiveBrokers, _btcusd, SecurityType.Crypto, Currencies.USD)
                .MinimumPriceVariation;
            var limitPrice = (price * 1.001m).DiscretelyRoundBy(tick, MidpointRounding.ToPositiveInfinity);
            Log.Trace($"CryptoLimit: price={price}, limit={limitPrice}, tick={tick}");

            var buy = new LimitOrder(_btcusd, quantity, limitPrice, DateTime.UtcNow);
            orderProvider.Add(buy);
            var buyEvents = PlaceAndWaitForFinalStatus(brokerage, session, buy);
            Assert.AreEqual(OrderStatus.Filled, buyEvents.LastOrDefault()?.Status, $"statuses [{string.Join(",", buyEvents.Select(x => x.Status))}]");
            Assert.AreEqual(quantity, buyEvents.Where(x => x.Status is OrderStatus.Filled or OrderStatus.PartiallyFilled).Sum(x => x.FillQuantity));

            var sell = new MarketOrder(_btcusd, -quantity, DateTime.UtcNow);
            orderProvider.Add(sell);
            var sellEvents = PlaceAndWaitForFinalStatus(brokerage, session, sell);
            Assert.AreEqual(OrderStatus.Filled, sellEvents.LastOrDefault()?.Status, $"statuses [{string.Join(",", sellEvents.Select(x => x.Status))}]");
        }

        /// <summary>
        /// Cancels the crypto orders IB still holds, such as a buy queued while the venue was closed
        /// </summary>
        [Test]
        public void CancelsOpenCryptoOrders()
        {
            var algorithm = new AlgorithmStub();
            algorithm.SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage);
            algorithm.AddCrypto(_btcusd.Value);

            var orderProvider = new OrderProvider();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, orderProvider, new SecurityProvider());
            var session = Listen(brokerage, "CryptoCleanup");
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders().Where(x => x.Symbol.SecurityType == SecurityType.Crypto).ToList();
            Log.Trace($"CryptoCleanup: {openOrders.Count} open crypto orders");
            foreach (var order in openOrders)
            {
                Log.Trace($"CryptoCleanup: cancelling {order.Type} {order.Symbol.Value} quantity={order.Quantity} brokerIds=[{string.Join(",", order.BrokerId)}]");
                orderProvider.Add(order);
                Assert.IsTrue(brokerage.CancelOrder(order), "CancelOrder returned false");
            }

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (DateTime.UtcNow < deadline && session.CanceledCount < openOrders.Count)
            {
                Thread.Sleep(250);
            }
            // IB does not acknowledge cancels while its crypto venue is closed, run this when it is open
            Assert.AreEqual(openOrders.Count, session.CanceledCount, "not every open crypto order was cancelled");
        }

        private sealed class LiveSession
        {
            public readonly List<OrderEvent> Events = [];
            // IB holds crypto orders placed while its venue is closed and says so with warning 399
            public volatile string VenueClosedMessage;

            public int CanceledCount
            {
                get
                {
                    lock (Events)
                    {
                        return Events.Count(x => x.Status == OrderStatus.Canceled);
                    }
                }
            }
        }

        private static LiveSession Listen(InteractiveBrokersBrokerage brokerage, string tag)
        {
            var session = new LiveSession();
            brokerage.Message += (_, message) =>
            {
                Log.Trace($"{tag}: {message.Type} {message.Code}: {message.Message}");
                if (message.Code == "399" && message.Message.Contains("will not be placed at the exchange until"))
                {
                    session.VenueClosedMessage = message.Message;
                }
            };
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                foreach (var orderEvent in orderEvents)
                {
                    Log.Trace($"{tag}: {orderEvent.Status} quantity={orderEvent.FillQuantity} price={orderEvent.FillPrice} " +
                        $"fee={orderEvent.OrderFee} {orderEvent.Message}");
                    lock (session.Events)
                    {
                        session.Events.Add(orderEvent);
                    }
                }
            };

            return session;
        }

        /// <summary>
        /// Places the order and returns its events once one is final or the timeout elapses. An order IB
        /// holds because its venue is closed is cancelled and the test is inconclusive.
        /// </summary>
        private static List<OrderEvent> PlaceAndWaitForFinalStatus(InteractiveBrokersBrokerage brokerage, LiveSession session, Order order)
        {
            int first;
            lock (session.Events)
            {
                first = session.Events.Count;
            }

            Assert.IsTrue(brokerage.PlaceOrder(order), "PlaceOrder returned false");

            var deadline = DateTime.UtcNow + _orderTimeout;
            while (true)
            {
                if (session.VenueClosedMessage != null)
                {
                    brokerage.CancelOrder(order);
                    Thread.Sleep(3000);
                    Assert.Inconclusive($"IB holds crypto orders until its venue reopens, run this when it is open: {session.VenueClosedMessage}");
                }

                lock (session.Events)
                {
                    var orderEvents = session.Events.Skip(first).ToList();
                    if (orderEvents.Any(x => x.Status is OrderStatus.Filled or OrderStatus.Invalid or OrderStatus.Canceled) || DateTime.UtcNow >= deadline)
                    {
                        return orderEvents;
                    }
                }
                Thread.Sleep(250);
            }
        }

        /// <summary>
        /// Any tick price is taken, and without the crypto data subscription none arrives: the price IB marks
        /// the account's holding at is the fallback
        /// </summary>
        private static decimal WaitForPrice(InteractiveBrokersBrokerage brokerage, Symbol symbol, TimeSpan timeout)
        {
            var config = new SubscriptionDataConfig(typeof(Tick), symbol, Resolution.Tick, TimeZones.Utc, TimeZones.Utc, true, false, false);
            var enumerator = brokerage.Subscribe(config, (_, _) => { });
            try
            {
                var deadline = DateTime.UtcNow + timeout;
                while (DateTime.UtcNow < deadline)
                {
                    if (enumerator.MoveNext() && enumerator.Current is Tick tick)
                    {
                        var price = tick.AskPrice > 0 ? tick.AskPrice : tick.Price;
                        if (price > 0)
                        {
                            Log.Trace($"CryptoLimit: {tick.TickType} price={tick.Price} bid={tick.BidPrice} ask={tick.AskPrice}");
                            return price;
                        }
                    }
                    else
                    {
                        Thread.Sleep(100);
                    }
                }
            }
            finally
            {
                brokerage.Unsubscribe(config);
            }

            var holding = brokerage.GetAccountHoldings().FirstOrDefault(x => x.Symbol.Value == symbol.Value);
            if (holding != null && holding.MarketPrice > 0)
            {
                Log.Trace($"CryptoLimit: no tick received, using the holding's market price {holding.MarketPrice}");
                return holding.MarketPrice;
            }

            return 0;
        }
    }
}
