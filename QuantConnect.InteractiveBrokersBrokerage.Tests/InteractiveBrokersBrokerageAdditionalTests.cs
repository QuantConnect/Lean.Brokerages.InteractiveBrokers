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
using System.Globalization;
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
using QuantConnect.Python;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine.DataFeeds;
using QuantConnect.Util;
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
            PythonInitializer.Initialize();
        }

        [TearDown]
        public void TearDown()
        {
            PythonInitializer.Shutdown();
        }

        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Buy, OrderDirection.Sell)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Sell, OrderDirection.Sell)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Sell, OrderDirection.Buy)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Buy, OrderDirection.Buy)]
        [TestCase(OrderType.ComboLimit, 250, 0, 0, 0, OrderDirection.Buy, OrderDirection.Buy)] // limit price that will never fill
        [TestCase(OrderType.ComboLegLimit, 0, 350, 1, -1, OrderDirection.Buy, OrderDirection.Buy)]
        public void SendComboOrder(OrderType orderType, decimal comboLimitPrice, decimal underlyingLimitPrice, decimal callLimitPrice, decimal putLimitPrice, OrderDirection comboDirection, OrderDirection callDirection)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio, new AggregationManager(), TestGlobals.MapFileProvider);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var order in openOrders)
            {
                brokerage.CancelOrder(order);
            }

            var optionsExpiration = new DateTime(2023, 7, 21);
            var orderProperties = new InteractiveBrokersOrderProperties();
            var group = new GroupOrderManager(1, legCount: 2, quantity: comboDirection == OrderDirection.Buy ? 2 : -2);

            var underlying = Symbols.SPY;

            var callSymbol = Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, OptionRight.Call, 430, optionsExpiration);
            var comboOrderCall = BuildOrder(orderType, callSymbol, callDirection == OrderDirection.Buy ? 1 : -1, comboLimitPrice, group,
                callLimitPrice, orderProperties, algo.Transactions);

            using var manualResetEvent = new ManualResetEvent(false);
            var events = new List<OrderEvent>();
            var orders = new List<Order> { comboOrderCall };

            var putSymbol = Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, OptionRight.Call, 432, optionsExpiration);
            var comboOrderPut = BuildOrder(orderType, putSymbol, -1, comboLimitPrice, group,
                putLimitPrice, orderProperties, algo.Transactions);
            orders.Add(comboOrderPut);

            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                events.AddRange(orderEvents);

                foreach (var order in orders)
                {
                    foreach (var orderEvent in orderEvents)
                    {
                        if (orderEvent.OrderId == order.Id)
                        {
                            // update the order like the BTH would do
                            order.Status = orderEvent.Status;
                        }
                    }

                    if (orders.All(o => o.Status.IsClosed()) || orders.All(o => o.Status == OrderStatus.Submitted))
                    {
                        manualResetEvent.Set();
                    }
                }
            };

            foreach (var order in orders)
            {
                group.OrderIds.Add(order.Id);
                orderProvider.Add(order);
                var response = brokerage.PlaceOrder(order);
            }

            Assert.IsTrue(manualResetEvent.WaitOne(TimeSpan.FromSeconds(60)));
        }

        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Buy, OrderDirection.Sell)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Sell, OrderDirection.Sell)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Sell, OrderDirection.Buy)]
        [TestCase(OrderType.ComboMarket, 0, 0, 0, 0, OrderDirection.Buy, OrderDirection.Buy)]
        [TestCase(OrderType.ComboLimit, 250, 0, 0, 0, OrderDirection.Buy, OrderDirection.Buy)] // limit price that will never fill

        [TestCase(OrderType.ComboLegLimit, 0, 350, 1, -1, OrderDirection.Buy, OrderDirection.Buy)]
        public void SendComboOrderWithUnderlying(OrderType orderType, decimal comboLimitPrice, decimal underlyingLimitPrice, decimal callLimitPrice, decimal putLimitPrice, OrderDirection comboDirection, OrderDirection callDirection)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio, new AggregationManager(), TestGlobals.MapFileProvider);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var order in openOrders)
            {
                brokerage.CancelOrder(order);
            }

            var optionsExpiration = new DateTime(2023, 7, 21);
            var orderProperties = new InteractiveBrokersOrderProperties();
            var group = new GroupOrderManager(1, legCount: orderType != OrderType.ComboLegLimit ? 3 : 2, quantity: comboDirection == OrderDirection.Buy ? 2 : -2);

            var comboOrderUnderlying = BuildOrder(orderType, Symbols.SPY, 100, comboLimitPrice, group,
                underlyingLimitPrice, orderProperties, algo.Transactions);

            var callSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call,
                        440, optionsExpiration);
            var comboOrderCall = BuildOrder(orderType, callSymbol, callDirection == OrderDirection.Buy ? 1 : -1, comboLimitPrice, group,
                callLimitPrice, orderProperties, algo.Transactions);

            using var manualResetEvent = new ManualResetEvent(false);
            var events = new List<OrderEvent>();
            var orders = new List<Order> { comboOrderUnderlying, comboOrderCall };

            if (orderType != OrderType.ComboLegLimit)
            {
                var putSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Put,
                            442, optionsExpiration);
                var comboOrderPut = BuildOrder(orderType, putSymbol, 1, comboLimitPrice, group,
                    putLimitPrice, orderProperties, algo.Transactions);
                orders.Add(comboOrderPut);
            }

            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                events.AddRange(orderEvents);

                foreach (var order in orders)
                {
                    foreach (var orderEvent in orderEvents)
                    {
                        if (orderEvent.OrderId == order.Id)
                        {
                            // update the order like the BTH would do
                            order.Status = orderEvent.Status;
                        }
                    }
                }

                if (orders.All(o => o.Status.IsClosed()) || (orderType == OrderType.ComboLimit || orderType == OrderType.ComboLegLimit) && orders.All(o => o.Status == OrderStatus.Submitted))
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
            if (orderType == OrderType.ComboLimit || orderType == OrderType.ComboLegLimit)
            {
                Assert.AreEqual(orders.Count, events.Count);
                Assert.IsTrue(events.All(oe => oe.Status == OrderStatus.Submitted));
            }
            else
            {
                // The orders could haven been filled in partial fills (expect 9 events: 3 submitted, and 6 partial fills)
                // or in a single fill (expect 6 events: 3 submitted, and 3 filled)
                if (events.Any(oe => oe.Status == OrderStatus.PartiallyFilled))
                {
                    Assert.AreEqual(9, events.Count);
                    Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Submitted));
                    Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.PartiallyFilled));
                    Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Filled));
                }
                else
                {
                    Assert.AreEqual(6, events.Count);
                    Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Submitted));
                    Assert.AreEqual(3, events.Count(oe => oe.Status == OrderStatus.Filled));
                }

                foreach (var order in orders)
                {
                    Assert.AreEqual(
                        order.Quantity,
                        events.Select(oe => oe.OrderId == order.Id && oe.Status.IsFill() ? oe.FillQuantity : 0).Sum());
                }
            }
        }

        // NOTEs:
        // - The initial stop price should be far enough from current market price in order to trigger at least one stop price update
        // - Stop price and trailing amount should be updated when the tests are run with real time data
        [TestCase(-100, 450, 0.05, false)]
        [TestCase(-100, 450, 0.0001, true)]
        [TestCase(100, 460, 0.05, false)]
        [TestCase(100, 460, 0.0001, true)]
        public void SendTrailingStopOrder(decimal quantity, decimal stopPrice, decimal trailingAmount, bool trailingAsPercentage)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio, new AggregationManager(), TestGlobals.MapFileProvider);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var o in openOrders)
            {
                brokerage.CancelOrder(o);
            }

            var symbol = Symbols.SPY;
            var orderProperties = new InteractiveBrokersOrderProperties();
            var request = new SubmitOrderRequest(OrderType.TrailingStop, symbol.SecurityType, symbol, quantity, stopPrice, 0, 0,
                trailingAmount, trailingAsPercentage, DateTime.UtcNow, string.Empty, orderProperties);
            algo.Transactions.SetOrderId(request);
            var order = Order.CreateOrder(request);
            var trailingStopOrder = (TrailingStopOrder)order;
            var prevStopPrice = trailingStopOrder.StopPrice;

            using var fillEvent = new ManualResetEvent(false);
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                if (orderEvents.Single().Status.IsClosed())
                {
                    fillEvent.Set();
                }
            };

            using var stopPriceUpdateEvent = new AutoResetEvent(false);
            var updatedStopPrice = prevStopPrice;
            brokerage.OrderUpdated += (_, e) =>
            {
                updatedStopPrice = e.TrailingStopPrice;
                stopPriceUpdateEvent.Set();
            };

            orderProvider.Add(order);
            var response = brokerage.PlaceOrder(order);

            // Assert that we get stop price updates
            var triggeredEventIndex = 0;
            var stopPriceUpdated = false;
            while (true)
            {
                triggeredEventIndex = WaitHandle.WaitAny(new WaitHandle[] { stopPriceUpdateEvent, fillEvent }, TimeSpan.FromSeconds(60));
                if (triggeredEventIndex != 0)
                {
                    break;
                }

                stopPriceUpdated = true;
                Assert.AreNotEqual(prevStopPrice, updatedStopPrice);
            };

            Assert.IsTrue(stopPriceUpdated);

            if (triggeredEventIndex == WaitHandle.WaitTimeout)
            {
                Log.Trace("Timeout waiting for order fill");
            }
            else
            {
                Log.Trace("The order was filled");
            }
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

        [TestCase("0.00:01:01.000", Resolution.Tick, "100 S")]
        [TestCase("1.00:00:00.000", Resolution.Tick, "2000 S")]
        [TestCase("1.00:01:00.000", Resolution.Tick, "2000 S")]
        [TestCase("758.00:01:00.000", Resolution.Tick, "2000 S")]
        [TestCase("0.00:01:01.000", Resolution.Second, "100 S")]
        [TestCase("1.00:00:00.000", Resolution.Second, "2000 S")]
        [TestCase("1.00:01:00.000", Resolution.Second, "2000 S")]
        [TestCase("758.00:01:00.000", Resolution.Second, "2000 S")]
        [TestCase("0.00:01:01.000", Resolution.Minute, "121 S")]
        [TestCase("1.00:00:00.000", Resolution.Minute, "2 D")]
        [TestCase("1.00:01:00.000", Resolution.Minute, "2 D")]
        [TestCase("758.00:01:00.000", Resolution.Minute, "7 D")]
        [TestCase("0.00:01:01.000", Resolution.Hour, "1 D")]
        [TestCase("1.00:00:00.000", Resolution.Hour, "2 D")]
        [TestCase("1.00:01:00.000", Resolution.Hour, "2 D")]
        [TestCase("758.00:01:00.000", Resolution.Hour, "6 M")]
        [TestCase("0.00:01:01.000", Resolution.Daily, "1 D")]
        [TestCase("1.00:00:00.000", Resolution.Daily, "2 D")]
        [TestCase("1.00:01:00.000", Resolution.Daily, "2 D")]
        [TestCase("758.00:01:00.000", Resolution.Daily, "2 Y")]
        public void Duration(string timeSpan, Resolution resolution, string expected)
        {
            var span = TimeSpan.ParseExact(timeSpan, "d\\.hh\\:mm\\:ss\\.fff", CultureInfo.InvariantCulture);
            var result = InteractiveBrokersBrokerage.GetDuration(resolution, span);

            Assert.AreEqual(expected, result);
        }

        [Test, TestCaseSource(nameof(HistoryDuration))]
        public void HistoryDurationTest(
            Symbol symbol,
            Resolution resolution,
            DateTimeZone exchangeTimeZone,
            DateTimeZone dataTimeZone,
            TimeSpan historyTimeSpan)
        {
            var endTimeInExchangeTimeZone = DateTime.UtcNow.Date.AddDays(-1).AddHours(13);

            var result = GetHistory(symbol, resolution, exchangeTimeZone, dataTimeZone, endTimeInExchangeTimeZone, historyTimeSpan, false);

            Assert.GreaterOrEqual(result.Count, 5);
        }

        [Test, TestCaseSource(nameof(HistoryData))]
        public void GetHistoryData(
            Symbol symbol,
            Resolution resolution,
            DateTimeZone exchangeTimeZone,
            DateTimeZone dataTimeZone,
            DateTime endTimeInExchangeTimeZone,
            TimeSpan historyTimeSpan,
            bool includeExtendedMarketHours,
            int expectedCount)
        {
            var result = GetHistory(symbol, resolution, exchangeTimeZone, dataTimeZone, endTimeInExchangeTimeZone, historyTimeSpan, includeExtendedMarketHours);

            Assert.AreEqual(expectedCount, result.Count);
        }

        private List<BaseData> GetHistory(
            Symbol symbol,
            Resolution resolution,
            DateTimeZone exchangeTimeZone,
            DateTimeZone dataTimeZone,
            DateTime endTimeInExchangeTimeZone,
            TimeSpan historyTimeSpan,
            bool includeExtendedMarketHours)
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

            var start = DateTime.UtcNow;
            var history = brokerage.GetHistory(request).ToList();

            Log.Trace($"Resolution: {request.Resolution}. History count: {history.Count}. Took: {DateTime.UtcNow - start}");

            // allow some time for the gateway to shutdown
            brokerage.DisposeSafely();
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // check if data points are in chronological order
            var previousEndTime = DateTime.MinValue;
            foreach (var bar in history)
            {
                Assert.IsTrue(bar.EndTime > previousEndTime);

                previousEndTime = bar.EndTime;
            }

            return history;
        }

        private static TestCaseData[] HistoryData()
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

        private static TestCaseData[] HistoryDuration()
        {
            TestGlobals.Initialize();

            List<TestCaseData> result = new();
            foreach (var resolution in Enum.GetValues(typeof(Resolution)).Cast<Resolution>())
            {
                var resSpan = resolution.ToTimeSpan();
                if (resolution == Resolution.Tick)
                {
                    continue;
                }
                result.Add(new TestCaseData(Symbols.SPY, resolution, TimeZones.NewYork, TimeZones.NewYork, resSpan * 9));
                result.Add(new TestCaseData(Symbols.SPY, resolution, TimeZones.NewYork, TimeZones.NewYork, resSpan * 99));
                result.Add(new TestCaseData(Symbols.SPY, resolution, TimeZones.NewYork, TimeZones.NewYork, resSpan * 999));
                result.Add(new TestCaseData(Symbols.SPY, resolution, TimeZones.NewYork, TimeZones.NewYork, resSpan * 10000));
                result.Add(new TestCaseData(Symbols.SPY, resolution, TimeZones.NewYork, TimeZones.NewYork, resSpan * 100000));
            }
            return result.ToArray();
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

        private Order BuildOrder(OrderType orderType, Symbol symbol, decimal legRatio, decimal comboLimitPrice,
            GroupOrderManager group, decimal legLimitPrice, IOrderProperties orderProperties, SecurityTransactionManager securityTransactionManager)
        {
            var limitPrice = comboLimitPrice;
            if (legLimitPrice != 0)
            {
                limitPrice = legLimitPrice;
            }
            var request = new SubmitOrderRequest(orderType, symbol.SecurityType, symbol, legRatio * group.Quantity, 0,
                limitPrice, 0, DateTime.UtcNow, string.Empty, orderProperties, groupOrderManager: group);
            securityTransactionManager.SetOrderId(request);
            return Order.CreateOrder(request);
        }
    }
}
