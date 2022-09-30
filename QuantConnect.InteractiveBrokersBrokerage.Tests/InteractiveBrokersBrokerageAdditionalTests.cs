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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using IBApi;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine.DataFeeds;
using Order = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersBrokerageAdditionalTests
    {
        private readonly List<Order> _orders = new List<Order>();

        [SetUp]
        public void Setup()
        {
            Log.LogHandler = new NUnitLogHandler();
        }

        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0)]
        [TestCase(OrderType.ComboLimit, 250, 0, 0, 0)] // limit price that will never fill
        [TestCase(OrderType.ComboLegLimit, 0, 350, 10, -1)]
        public void SendComboOrder(OrderType orderType, decimal comboLimitPrice, decimal underlyingLimitPrice, decimal callLimitPrice, decimal putLimitPrice)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio, new AggregationManager(), TestGlobals.MapFileProvider);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var order in openOrders)
            {
                brokerage.CancelOrder(order);
            }

            var orderProperties = new InteractiveBrokersOrderProperties
            {
                GuaranteedComboRouting = true
            };
            var group = new GroupOrderManager(legCount: orderType != OrderType.ComboLegLimit ? 3 : 2, quantity: 2);

            var comboOrderUnderlying = BuildOrder(orderType, Symbols.SPY, 100, comboLimitPrice, group, algo.Transactions.GetIncrementOrderId(),
                underlyingLimitPrice, orderProperties);

            var callSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call,
                        380, new DateTime(2022, 11, 4)); //new DateTime(2022, 09, 28))
            var comboOrderCall = BuildOrder(orderType, callSymbol, 1, comboLimitPrice, group, algo.Transactions.GetIncrementOrderId(),
                callLimitPrice, orderProperties);

            using var manualResetEvent = new ManualResetEvent(false);
            var events = new List<OrderEvent>();
            var orders = new List<Order> { comboOrderUnderlying, comboOrderCall };

            if (orderType != OrderType.ComboLegLimit)
            {
                var putSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Put,
                            360, new DateTime(2022, 11, 4)); //new DateTime(2022, 09, 28))
                var comboOrderPut = BuildOrder(orderType, putSymbol, 1, comboLimitPrice, group, algo.Transactions.GetIncrementOrderId(),
                    putLimitPrice, orderProperties);
                orders.Add(comboOrderPut);
            }

            brokerage.OrderStatusChanged += (_, orderEvent) =>
            {
                events.Add(orderEvent);

                foreach (var order in orders)
                {
                    if (orderEvent.OrderId == order.Id)
                    {
                        // update the order like the BTH would do
                        order.Status = orderEvent.Status;
                    }
                }

                if (orders.All(o => o.Status.IsClosed()) || orderType == OrderType.ComboLimit && orders.All(o => o.Status == OrderStatus.Submitted))
                {
                    manualResetEvent.Set();
                }
            };

            foreach (var order in orders)
            {
                group.OrderIds.Add(order.Id);
                orderProvider.Add(order);
                var response = brokerage.PlaceOrder(order);
            }

            Assert.IsTrue(manualResetEvent.WaitOne(TimeSpan.FromSeconds(60)));
            if (orderType == OrderType.ComboLimit)
            {
                Assert.AreEqual(3, events.Count);
                Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Submitted));
            }
            else
            {
                Assert.AreEqual(9, events.Count);
                Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Submitted));
                Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.PartiallyFilled));
                Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Filled));
            }
        }

        private Order BuildOrder(OrderType orderType, Symbol symbol, decimal quantity, decimal comboLimitPrice,
            GroupOrderManager group, int id, decimal legLimitPrice, IOrderProperties orderProperties)
        {
            var limitPrice = comboLimitPrice;
            if (legLimitPrice != 0)
            {
                limitPrice = legLimitPrice;
            }
            var request = new SubmitOrderRequest(orderType, symbol.SecurityType, symbol, quantity, 0,
                limitPrice, 0, DateTime.UtcNow, string.Empty, orderProperties, groupOrderManager: group);
            request.SetOrderId(id);
            return Order.CreateOrder(request);
        }

        [Test(Description = "Requires an existing IB connection with the same user credentials.")]
        public void ThrowsWhenExistingSessionDetected()
        {
            Assert.Throws<Exception>(() => GetBrokerage());
        }

        [Test]
        public void TestRateLimiting()
        {
            using (var brokerage = GetBrokerage())
            {
                Assert.IsTrue(brokerage.IsConnected);

                var method = brokerage.GetType().GetMethod("GetContractDetails", BindingFlags.NonPublic | BindingFlags.Instance);

                var contract = new Contract
                {
                    Symbol = "EUR",
                    Exchange = "IDEALPRO",
                    SecType = "CASH",
                    Currency = Currencies.USD
                };
                var parameters = new object[] { contract };

                var result = Parallel.For(1, 100, x =>
                {
                    var stopwatch = Stopwatch.StartNew();
                    var value = (ContractDetails)method.Invoke(brokerage, parameters);
                    stopwatch.Stop();
                    Log.Trace($"{DateTime.UtcNow:O} Response time: {stopwatch.Elapsed}");
                });
                while (!result.IsCompleted) Thread.Sleep(1000);
            }
        }

        [Test]
        public void GetsHistoryWithMultipleApiCalls()
        {
            using (var brokerage = GetBrokerage())
            {
                Assert.IsTrue(brokerage.IsConnected);

                // request a week of historical data (including a weekend)
                var request = new HistoryRequest(
                    new DateTime(2018, 2, 1, 9, 30, 0).ConvertToUtc(TimeZones.NewYork),
                    new DateTime(2018, 2, 7, 16, 0, 0).ConvertToUtc(TimeZones.NewYork),
                    typeof(TradeBar),
                    Symbols.SPY,
                    Resolution.Minute,
                    SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                    TimeZones.NewYork,
                    null,
                    false,
                    false,
                    DataNormalizationMode.Raw,
                    TickType.Trade);

                var history = brokerage.GetHistory(request).ToList();

                // check if data points are in chronological order
                var previousEndTime = DateTime.MinValue;
                foreach (var bar in history)
                {
                    Assert.IsTrue(bar.EndTime > previousEndTime);

                    previousEndTime = bar.EndTime;
                }

                // should return 5 days of data (Thu-Fri-Mon-Tue-Wed)
                // each day has 390 minute bars for equities
                Assert.AreEqual(5 * 390, history.Count);
            }
        }

        [Test]
        public void GetHistoryDoesNotThrowError504WhenDisconnected()
        {
            using (var brokerage = GetBrokerage())
            {
                Assert.IsTrue(brokerage.IsConnected);

                brokerage.Disconnect();
                Assert.IsFalse(brokerage.IsConnected);

                var hasError = false;
                brokerage.Message += (s, e) =>
                {
                    // ErrorCode: 504 - Not connected
                    if (e.Code == "504")
                    {
                        hasError = true;
                    }
                };

                var request = new HistoryRequest(
                    new DateTime(2021, 1, 1).ConvertToUtc(TimeZones.NewYork),
                    new DateTime(2021, 1, 27).ConvertToUtc(TimeZones.NewYork),
                    typeof(TradeBar),
                    Symbols.SPY,
                    Resolution.Daily,
                    SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                    TimeZones.NewYork,
                    null,
                    false,
                    false,
                    DataNormalizationMode.Raw,
                    TickType.Trade);

                var history = brokerage.GetHistory(request).ToList();

                Assert.AreEqual(0, history.Count);

                Assert.IsFalse(hasError);
            }
        }

        [Test, TestCaseSource(nameof(GetHistoryData))]
        public void GetHistory(
            Symbol symbol,
            Resolution resolution,
            DateTimeZone exchangeTimeZone,
            DateTimeZone dataTimeZone,
            DateTime endTimeInExchangeTimeZone,
            TimeSpan historyTimeSpan,
            bool includeExtendedMarketHours,
            int expectedCount)
        {
            using var brokerage = GetBrokerage();
            Assert.IsTrue(brokerage.IsConnected);

            var request = new HistoryRequest(
                endTimeInExchangeTimeZone.ConvertToUtc(exchangeTimeZone).Subtract(historyTimeSpan),
                endTimeInExchangeTimeZone.ConvertToUtc(exchangeTimeZone),
                typeof(TradeBar),
                symbol,
                resolution,
                SecurityExchangeHours.AlwaysOpen(exchangeTimeZone),
                dataTimeZone,
                null,
                includeExtendedMarketHours,
                false,
                DataNormalizationMode.Raw,
                TickType.Trade);

            var history = brokerage.GetHistory(request).ToList();

            // check if data points are in chronological order
            var previousEndTime = DateTime.MinValue;
            foreach (var bar in history)
            {
                Assert.IsTrue(bar.EndTime > previousEndTime);

                previousEndTime = bar.EndTime;
            }

            Log.Trace($"History count: {history.Count}");

            Assert.AreEqual(expectedCount, history.Count);
        }

        private static TestCaseData[] GetHistoryData()
        {
            TestGlobals.Initialize();
            var futureSymbolUsingCents = Symbols.CreateFutureSymbol("LE", new DateTime(2021, 12, 31));
            var futureOptionSymbolUsingCents = Symbols.CreateFutureOptionSymbol(futureSymbolUsingCents, OptionRight.Call, 1.23m, new DateTime(2021, 12, 3));

            var futureSymbol = Symbol.CreateFuture("NQ", Market.CME, new DateTime(2021, 9, 17));
            var optionSymbol = Symbol.CreateOption("AAPL", Market.USA, OptionStyle.American, OptionRight.Call, 145, new DateTime(2021, 8, 20));

            var delistedEquity = Symbol.Create("AAA.1", SecurityType.Equity, Market.USA);
            return new[]
            {
                // 30 min RTH today + 60 min RTH yesterday
                new TestCaseData(Symbols.SPY, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(19), false, 5400),

                // 30 min RTH + 30 min ETH
                new TestCaseData(Symbols.SPY, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(1), true, 3600),

                // daily
                new TestCaseData(futureSymbolUsingCents, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 9, 20, 0, 0, 0), TimeSpan.FromDays(10), true, 6),
                // hourly
                new TestCaseData(futureOptionSymbolUsingCents, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 9, 20, 0, 0, 0), TimeSpan.FromDays(10), true, 11),

                // 60 min
                new TestCaseData(futureSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.Utc,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(1), false, 3600),

                // 60 min - RTH flag ignored, no ETH market hours
                new TestCaseData(futureSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.Utc,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(1), true, 3600),

                // 30 min today + 60 min yesterday
                new TestCaseData(optionSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(19), false, 5400),

                // 30 min today + 60 min yesterday - RTH flag ignored, no ETH market hours
                new TestCaseData(optionSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(19), true, 5400),

                // delisted asset
                new TestCaseData(delistedEquity, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(19), false, 0),
            };
        }

        private InteractiveBrokersBrokerage GetBrokerage()
        {
            // grabs account info from configuration
            var securityProvider = new SecurityProvider();
            securityProvider[Symbols.USDJPY] = new Security(
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                new SubscriptionDataConfig(
                    typeof(TradeBar),
                    Symbols.USDJPY,
                    Resolution.Minute,
                    TimeZones.NewYork,
                    TimeZones.NewYork,
                    false,
                    false,
                    false
                ),
                new Cash(Currencies.USD, 0, 1m),
                SymbolProperties.GetDefault(Currencies.USD),
                ErrorCurrencyConverter.Instance,
                RegisteredSecurityDataTypesProvider.Null,
                new SecurityCache()
            );

            var brokerage = new InteractiveBrokersBrokerage(
                new QCAlgorithm(),
                new OrderProvider(_orders),
                securityProvider,
                new AggregationManager(),
                TestGlobals.MapFileProvider);
            brokerage.Connect();

            return brokerage;
        }
    }
}
