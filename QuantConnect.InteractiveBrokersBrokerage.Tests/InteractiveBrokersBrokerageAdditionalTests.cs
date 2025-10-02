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
using System.Text;
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
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.IBAutomater;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.TransactionHandlers;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Python;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine;
using QuantConnect.Tests.Engine.DataFeeds;
using QuantConnect.Util;
using Order = QuantConnect.Orders.Order;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using QuantConnect.Securities.IndexOption;
using static QuantConnect.Brokerages.InteractiveBrokers.InteractiveBrokersAccountData;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersBrokerageAdditionalTests
    {
        private readonly List<Order> _orders = new List<Order>();

        private readonly SymbolPropertiesDatabase _symbolPropertiesDatabase = SymbolPropertiesDatabase.FromDataFolder();

        private InteractiveBrokersSymbolMapper _symbolMapper = new InteractiveBrokersSymbolMapper(Composer.Instance.GetPart<IMapFileProvider>());

        private InteractiveBrokersBrokerage _ib;

        [OneTimeSetUp]
        public void Setup()
        {
            Log.LogHandler = new NUnitLogHandler();
            PythonInitializer.Initialize();
            _ib = new InteractiveBrokersBrokerage();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            PythonInitializer.Shutdown();
        }

        [TestCase(-500, 623.794, 100, 622.181, -400, 624.19725, "B")]
        [TestCase(100, 210.101, -200, 210.044, -100, 209.987, "B")]
        [TestCase(-500, 623.794, -500, 623.794, -500, 623.794, "A")] // double A is ignored
        public void MergeHoldingMergesOppositeSignedAAPLPositions(decimal holdingPositionQuantity, decimal holdingAvgPrice, decimal incomePositionQuantity,
            decimal incomeAvgPrice, decimal expectedNewPositionQuantity, decimal expectedAvgPrice, string incomingAccount)
        {
            var aapl = Symbols.AAPL;

            var holdings = new Dictionary<Symbol, MergedHoldings>();
            holdings[aapl] = new();

            holdings[aapl].Merge(new Holding
            {
                Symbol = aapl,
                Quantity = holdingPositionQuantity,
                AveragePrice = holdingAvgPrice
            }, "A");

            var incoming = new Holding
            {
                Symbol = aapl,
                Quantity = incomePositionQuantity,
                AveragePrice = incomeAvgPrice
            };

            InteractiveBrokersBrokerage.MergeHolding(holdings, incoming, incomingAccount);

            var merged = holdings[aapl];

            Assert.AreEqual(expectedNewPositionQuantity, merged.Holding.Quantity);
            Assert.AreEqual(expectedAvgPrice, merged.Holding.AveragePrice);
            Assert.AreEqual(aapl, merged.Holding.Symbol);
        }

        [Test]
        public void LoginFailOnInvalidUserName()
        {
            var originalUserName = Config.Get("ib-user-name");
            Config.Set("ib-user-name", "User name with invalid characters");

            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();

            var exception = Assert.Throws<Exception>(() =>
            {
                using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio);
            });

            StringAssert.Contains(ErrorCode.LoginFailed.ToString(), exception.Message);

            Config.Set("ib-user-name", originalUserName);
        }

        [TestCase(OrderType.ComboMarket, 0, OrderDirection.Buy, OrderDirection.Buy, OrderDirection.Buy, true, SecurityType.Option)]
        [TestCase(OrderType.ComboLimit, 10, OrderDirection.Buy, OrderDirection.Buy, OrderDirection.Buy, false, SecurityType.Option)]

        [TestCase(OrderType.ComboLimit, 25, OrderDirection.Buy, OrderDirection.Buy, OrderDirection.Buy, false, SecurityType.Equity)]
        [TestCase(OrderType.ComboMarket, 0, OrderDirection.Buy, OrderDirection.Buy, OrderDirection.Buy, false, SecurityType.Equity)]
        [TestCase(OrderType.ComboMarket, 0, OrderDirection.Sell, OrderDirection.Buy, OrderDirection.Buy, false, SecurityType.Equity)]
        public void ComboStockOrder(OrderType orderType, decimal comboLimitPrice, OrderDirection comboDirection, OrderDirection callDirection, OrderDirection secondCallDirection, bool addUnderlying, SecurityType securityType)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio);
            brokerage.Connect();

            var orders = CreateOrders(orderType, comboLimitPrice, comboDirection, callDirection, secondCallDirection, addUnderlying, securityType);
            var events = new List<OrderEvent>();
            using var manualResetEvent = new ManualResetEvent(false);
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

                    if (orders.All(o => o.Status.IsClosed()) || orderType == OrderType.ComboLimit && orders.All(o => o.Status == OrderStatus.Submitted))
                    {
                        manualResetEvent.Set();
                    }
                }
            };
            foreach (var order in orders)
            {
                orderProvider.Add(order);
                Assert.IsTrue(brokerage.PlaceOrder(order));
            }
            Assert.IsTrue(manualResetEvent.WaitOne(TimeSpan.FromSeconds(60)));
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
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio);
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
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var order in openOrders)
            {
                brokerage.CancelOrder(order);
            }

            var optionsExpiration = new DateTime(2024, 9, 6);
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

        [TestCase(OrderType.ComboMarket)]
        [TestCase(OrderType.ComboLimit)]
        [TestCase(OrderType.ComboLegLimit)]
        public void UpdateComboOrder(OrderType orderType)
        {
            var algo = new AlgorithmStub();
            var orderProvider = new OrderProvider();
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);
            using var brokerage = new InteractiveBrokersBrokerage(algo, orderProvider, algo.Portfolio);
            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var order in openOrders)
            {
                brokerage.CancelOrder(order);
            }

            var optionsExpiration = new DateTime(2023, 12, 29);
            var orderProperties = new InteractiveBrokersOrderProperties();
            var comboLimitPrice = orderType == OrderType.ComboLimit ? 400m : 0m;
            var group = new GroupOrderManager(1, legCount: 2, quantity: 2, limitPrice: comboLimitPrice);

            var underlying = Symbols.SPY;

            var symbol1 = Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, OptionRight.Call, 475m, optionsExpiration);
            var leg1 = BuildOrder(orderType, symbol1, -1, comboLimitPrice, group, orderType == OrderType.ComboLegLimit ? 490m : 0m,
                orderProperties, algo.Transactions);

            var symbol2 = Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, OptionRight.Call, 480m, optionsExpiration);
            var leg2 = BuildOrder(orderType, symbol2, +1, comboLimitPrice, group, orderType == OrderType.ComboLegLimit ? 460m : 0m,
                orderProperties, algo.Transactions);

            var orders = new List<Order> { leg1, leg2 };

            using var submittedEvent = new ManualResetEvent(false);
            EventHandler<List<OrderEvent>> handleSubmission = (_, orderEvents) =>
            {
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

                    if (orders.All(o => o.Status == OrderStatus.Submitted))
                    {
                        submittedEvent.Set();
                    }
                }
            };
            brokerage.OrdersStatusChanged += handleSubmission;

            foreach (var order in orders)
            {
                group.OrderIds.Add(order.Id);
                orderProvider.Add(order);
                brokerage.PlaceOrder(order);
            }

            Assert.IsTrue(submittedEvent.WaitOne(TimeSpan.FromSeconds(20)));

            brokerage.OrdersStatusChanged -= handleSubmission;

            using var updatedEvent = new ManualResetEvent(false);
            EventHandler<List<OrderEvent>> handleUpdate = (_, orderEvents) =>
            {
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

                    if (orders.All(o => o.Status == OrderStatus.UpdateSubmitted))
                    {
                        updatedEvent.Set();
                    }
                }
            };
            brokerage.OrdersStatusChanged += handleUpdate;

            // Update order quantity
            orders[0].ApplyUpdateOrderRequest(new UpdateOrderRequest(
                DateTime.UtcNow,
                orders[0].Id,
                new UpdateOrderFields { Quantity = group.Quantity * 2 }));

            // Update global limit price
            if (orderType == OrderType.ComboLimit)
            {
                orders[0].ApplyUpdateOrderRequest(new UpdateOrderRequest(
                    DateTime.UtcNow,
                    orders[0].Id,
                    new UpdateOrderFields { LimitPrice = 450m }));
            }

            foreach (var order in orders)
            {
                // Update leg limit price
                if (orderType == OrderType.ComboLegLimit)
                {
                    var legLimitOrder = (ComboLegLimitOrder)order;
                    legLimitOrder.ApplyUpdateOrderRequest(new UpdateOrderRequest(
                        DateTime.UtcNow,
                        order.Id,
                        new UpdateOrderFields
                        {
                            LimitPrice = legLimitOrder.LimitPrice + Math.Sign(order.Quantity) * 5m
                        }));
                }

                brokerage.UpdateOrder(order);
            }

            Assert.IsTrue(updatedEvent.WaitOne(TimeSpan.FromSeconds(20)));

            brokerage.OrdersStatusChanged -= handleUpdate;
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
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);

            var algorithm = new AlgorithmStub();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, algorithm.Transactions, algorithm.Portfolio);

            var orderProcesor = new BrokerageTransactionHandler();
            orderProcesor.Initialize(algorithm, brokerage, new TestResultHandler());
            algorithm.Transactions.SetOrderProcessor(orderProcesor);

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
            algorithm.Transactions.SetOrderId(request);
            var order = Order.CreateOrder(request);
            var trailingStopOrder = (TrailingStopOrder)order;
            var prevStopPrice = trailingStopOrder.StopPrice;

            // Track fill events
            using var fillEvent = new ManualResetEvent(false);
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                if (orderEvents.Single().Status.IsClosed())
                {
                    fillEvent.Set();
                }
            };

            // Track stop price updates
            using var stopPriceUpdateEvent = new AutoResetEvent(false);
            var updatedStopPrice = prevStopPrice;
            brokerage.OrderUpdated += (_, e) =>
            {
                updatedStopPrice = e.TrailingStopPrice;
                stopPriceUpdateEvent.Set();
            };

            algorithm.AddEquity("SPY");
            algorithm.SetFinishedWarmingUp();

            // Place order
            orderProcesor.AddOrder(request);
            Thread.Sleep(1000);
            order = orderProcesor.GetOpenOrders().Single();

            // Assert that we get stop price updates
            var triggeredEventIndex = 0;
            var stopPriceUpdated = false;
            while ((triggeredEventIndex = WaitHandle.WaitAny(new WaitHandle[] { stopPriceUpdateEvent, fillEvent }, TimeSpan.FromSeconds(60))) == 0)
            {
                stopPriceUpdated = true;
                Assert.AreNotEqual(prevStopPrice, updatedStopPrice);
                var orderCurrentStopPrice = ((TrailingStopOrder)algorithm.Transactions.GetOpenOrders().Single()).StopPrice;
                Assert.AreEqual(updatedStopPrice, orderCurrentStopPrice);
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

        // NOTEs:
        // - The initial stop price should be far enough from current market price in order to trigger at least one stop price update
        // - Stop price and trailing amount should be updated when the tests are run with real time data
        [Test]
        public void SendUpdateAndCancelTrailingStopOrder()
        {
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);

            var algorithm = new AlgorithmStub();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, algorithm.Transactions, algorithm.Portfolio);

            var orderProcesor = new BrokerageTransactionHandler();
            orderProcesor.Initialize(algorithm, brokerage, new TestResultHandler());
            algorithm.Transactions.SetOrderProcessor(orderProcesor);

            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var o in openOrders)
            {
                brokerage.CancelOrder(o);
            }

            var symbol = Symbols.SPY;
            var orderProperties = new InteractiveBrokersOrderProperties();
            var request = new SubmitOrderRequest(OrderType.TrailingStop, symbol.SecurityType, symbol, -100, stopPrice: 400, 0, 0,
                // Trailing amount is set to 0.15% of the current market price, for SPY now it ensures it won't be triggered
                trailingAmount: 0.15m, trailingAsPercentage: true, DateTime.UtcNow, string.Empty, orderProperties);
            algorithm.Transactions.SetOrderId(request);
            var order = Order.CreateOrder(request);
            var trailingStopOrder = (TrailingStopOrder)order;
            var prevStopPrice = trailingStopOrder.StopPrice;

            // Track fill events and other status updates
            using var orderPlacedEvent = new ManualResetEvent(false);
            using var fillEvent = new ManualResetEvent(false);
            using var statusUpdateEvent = new ManualResetEvent(false);
            using var cancelEvent = new ManualResetEvent(false);
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                var orderEvent = orderEvents[0];
                Console.WriteLine($"ORDER EVENT: Status: {orderEvent.Status}");
                switch (orderEvent.Status)
                {
                    case OrderStatus.Submitted:
                        orderPlacedEvent.Set();
                        break;
                    case OrderStatus.UpdateSubmitted:
                        statusUpdateEvent.Set();
                        break;
                    case OrderStatus.Filled:
                        fillEvent.Set();
                        break;
                    case OrderStatus.Canceled:
                        cancelEvent.Set();
                        break;
                }
            };

            algorithm.AddEquity("SPY");
            algorithm.SetFinishedWarmingUp();

            // Place order
            orderProcesor.AddOrder(request);
            Thread.Sleep(1000);
            order = orderProcesor.GetOpenOrders().Single();

            orderPlacedEvent.WaitOneAssertFail(5000, "Failed to submit trailing stop order");

            // Update order
            var updateRequest = new UpdateOrderRequest(DateTime.UtcNow, order.Id, new UpdateOrderFields { TrailingAmount = 0.1m });
            order.ApplyUpdateOrderRequest(updateRequest);
            brokerage.UpdateOrder(order);

            // Wait for the update to be applied
            statusUpdateEvent.WaitOneAssertFail(5000, "Failed to update trailing amount");

            var brokerageOrders = brokerage.GetOpenOrders();
            var brokerageOrder = brokerageOrders.Where(o => o.BrokerId.Contains(order.BrokerId[0])).Single();
            Assert.AreEqual(0.1m, ((TrailingStopOrder)brokerageOrder).TrailingAmount);

            // Cancel order
            brokerage.CancelOrder(order);

            cancelEvent.WaitOneAssertFail(5000, "Failed to cancel trailing stop order");

            brokerageOrders = brokerage.GetOpenOrders();
            var canceledBrokerageOrder = brokerageOrders.FirstOrDefault(o => o.BrokerId.Contains(order.BrokerId[0]));
            Assert.IsNull(canceledBrokerageOrder);
        }

        [TestCase(-100, 450, 445)]
        [TestCase(100, 450, 455)]
        public void SendStopLimitOrder(decimal quantity, decimal stopPrice, decimal limitPrice)
        {
            // wait for the previous run to finish, avoid any race condition
            Thread.Sleep(2000);

            var algorithm = new AlgorithmStub();
            using var brokerage = new InteractiveBrokersBrokerage(algorithm, algorithm.Transactions, algorithm.Portfolio);

            var orderProcesor = new BrokerageTransactionHandler();
            orderProcesor.Initialize(algorithm, brokerage, new TestResultHandler());
            algorithm.Transactions.SetOrderProcessor(orderProcesor);

            brokerage.Connect();

            var openOrders = brokerage.GetOpenOrders();
            foreach (var o in openOrders)
            {
                brokerage.CancelOrder(o);
            }

            var symbol = Symbols.SPY;
            var orderProperties = new InteractiveBrokersOrderProperties();
            var request = new SubmitOrderRequest(OrderType.StopLimit, symbol.SecurityType, symbol, quantity, stopPrice, limitPrice, 0, 0, false,
                DateTime.UtcNow, string.Empty, orderProperties);
            algorithm.Transactions.SetOrderId(request);
            var order = Order.CreateOrder(request);

            var submittedStatusReceived = false;

            // Track fill events
            using var fillEvent = new ManualResetEvent(false);
            brokerage.OrdersStatusChanged += (_, orderEvents) =>
            {
                var orderEvent = orderEvents[0];

                if (orderEvent.Status == OrderStatus.Submitted)
                {
                    submittedStatusReceived = true;
                }
                else if (orderEvent.Status.IsClosed())
                {
                    fillEvent.Set();
                }
            };

            // Track stop trigger
            using var stopTriggeredEvent = new AutoResetEvent(false);
            var stopTriggered = false;
            brokerage.OrderUpdated += (_, e) =>
            {
                stopTriggered = e.StopTriggered;
            };

            algorithm.AddEquity("SPY");
            algorithm.SetFinishedWarmingUp();

            // Place order
            orderProcesor.AddOrder(request);
            order = orderProcesor.GetOpenOrders().Single();

            var filled = fillEvent.WaitOne(TimeSpan.FromSeconds(60));

            Assert.IsTrue(submittedStatusReceived);

            if (filled)
            {
                Assert.IsTrue(stopTriggered);
                var stopLimitOrder = (StopLimitOrder)algorithm.Transactions.GetOrders().Single();
                Assert.IsTrue(stopLimitOrder.StopTriggered);
            }
            else
            {
                Assert.Fail("Order did not fill within 60 seconds, try with different parameters");
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

                var history = brokerage.GetHistory(request);

                Assert.IsNull(history);

                Assert.IsFalse(hasError);
            }
        }

        private static TestCaseData[] UnsupportedHistoryTestCases => new[]
        {
            // Canonicals are not supported
            new TestCaseData(Symbol.CreateCanonicalOption(Symbols.AAPL), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbols.CreateFuturesCanonicalSymbol("ES"), Resolution.Daily, TickType.Trade),
            // Unsupported markets
            new TestCaseData(Symbol.Create("SPY", SecurityType.Equity, Market.India), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbol.Create("EURUSD", SecurityType.Forex, Market.Binance), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbol.CreateOption(Symbols.SPY, Market.India, OptionStyle.American, OptionRight.Call, 100m, new DateTime(2024, 12, 12)), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbol.CreateOption(Symbols.SPX, Market.India, OptionStyle.American, OptionRight.Call, 100m, new DateTime(2024, 12, 12)), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbol.Create("SPX", SecurityType.Index, Market.India), Resolution.Daily, TickType.Trade),
            new TestCaseData(Symbol.Create("IBUS500", SecurityType.Cfd, Market.FXCM), Resolution.Daily, TickType.Trade),
            // Unsupported resolution
            new TestCaseData(Symbols.SPY, Resolution.Tick, TickType.Trade),
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Tick, TickType.Trade),
            new TestCaseData(Symbols.USDJPY, Resolution.Tick, TickType.Trade),
            new TestCaseData(Symbols.SPX, Resolution.Tick, TickType.Trade),
            new TestCaseData(Symbols.Future_ESZ18_Dec2018, Resolution.Tick, TickType.Trade),
            // Unsupported tick type
            new TestCaseData(Symbols.SPY, Resolution.Tick, TickType.OpenInterest),
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Tick, TickType.OpenInterest),
            new TestCaseData(Symbols.USDJPY, Resolution.Tick, TickType.OpenInterest),
            new TestCaseData(Symbols.SPX, Resolution.Tick, TickType.OpenInterest),
            new TestCaseData(Symbols.Future_ESZ18_Dec2018, Resolution.Tick, TickType.OpenInterest),
            new TestCaseData(Symbol.Create("IBUS500", SecurityType.Cfd, Market.InteractiveBrokers), Resolution.Daily, TickType.Trade),
        };

        [TestCaseSource(nameof(UnsupportedHistoryTestCases))]
        public void GetHistoryReturnsNullForUnsupportedCases(Symbol symbol, Resolution resolution, TickType tickType)
        {
            using (var brokerage = GetBrokerage())
            {
                Assert.IsTrue(brokerage.IsConnected);

                var request = new HistoryRequest(
                    new DateTime(2021, 1, 1).ConvertToUtc(TimeZones.NewYork),
                    new DateTime(2021, 1, 27).ConvertToUtc(TimeZones.NewYork),
                    typeof(TradeBar),
                    symbol,
                    resolution,
                    SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                    TimeZones.NewYork,
                    null,
                    false,
                    false,
                    DataNormalizationMode.Raw,
                    tickType);

                var history = brokerage.GetHistory(request);

                Assert.IsNull(history);
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

        [Test]
        public void IgnoresSecurityNotFoundErrorOnExpiredContractsHistoricalRequests()
        {
            using var brokerage = GetBrokerage();
            Assert.IsTrue(brokerage.IsConnected);

            var messages = new List<BrokerageMessageEvent>();
            void onMessage(object sender, BrokerageMessageEvent e)
            {
                messages.Add(e);
            }

            brokerage.Message += onMessage;

            var request = new HistoryRequest(
                new DateTime(2023, 09, 04, 9, 30, 0).ConvertToUtc(TimeZones.NewYork),
                new DateTime(2023, 09, 14, 16, 0, 0).ConvertToUtc(TimeZones.NewYork),
                typeof(TradeBar),
                Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new DateTime(2023, 09, 15)),
                Resolution.Minute,
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                TimeZones.NewYork,
                null,
                false,
                false,
                DataNormalizationMode.Raw,
                TickType.Trade);

            var history = brokerage.GetHistory(request).ToList();

            Assert.AreEqual(0, history.Count);

            Assert.IsFalse(messages.Any(x => x.Type == BrokerageMessageType.Error), string.Join("\n", messages.Select(x => x.Message)));

            Console.WriteLine(string.Join("\n", messages.Select(x => x.Message)));

            brokerage.Message -= onMessage;
        }

        [Test]
        public void ShouldSkipTickOnlyOnceWithinSkipWindow()
        {
            InteractiveBrokersBrokerage._nextNdxMarketOpenSkipTime = default;

            var symbol = Symbol.Create("NDX", SecurityType.Index, Market.USA);

            var ndxSecurityExchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            // Tick before market open on 2025-05-06 - should not skip
            var firstLocalDateTime = DateTime.Parse("2025-05-06T09:29:30");
            var firstResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, firstLocalDateTime);
            Assert.IsFalse(firstResult, "Expected no skip on first tick before market open");

            // First tick after market open on 2025-05-06 - should skip
            var secondLocalDateTime = DateTime.Parse("2025-05-06T09:30:17");
            var secondResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, secondLocalDateTime);
            Assert.IsTrue(secondResult, "Expected skip after market open");

            // Tick before market open on 2025-05-07 - should not skip
            var thirdLocalDateTime = DateTime.Parse("2025-05-07T09:29:28");
            var thirdResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, thirdLocalDateTime);
            Assert.IsFalse(thirdResult, "Expected no skip before market open on next day");

            // First tick after market open on 2025-05-07 - should skip
            var fourthLocalDateTime = DateTime.Parse("2025-05-07T09:30:02");
            var fourthResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, fourthLocalDateTime);
            Assert.IsTrue(fourthResult, "Expected skip on next day after market open");
        }

        [TestCase("2025-05-06T13:29:59", false, Description = "Tick just before market open")]
        [TestCase("2025-05-06T13:30:00", true, Description = "Tick at exact market open time")]
        [TestCase("2025-05-06T13:30:15", true, Description = "Tick 15 seconds after market open")]
        [TestCase("2025-05-06T13:30:30", false, Description = "Tick 30 seconds after market open")]
        [TestCase("2025-05-06T13:31:00", false, Description = "Tick 1 minute after market open � outside skip window")]
        [TestCase("2025-05-06T13:32:00", false, Description = "Tick well after skip window")]
        [TestCase("2025-05-06T17:31:00", false, Description = "Tick at 1:31 PM EST � unrelated time")]
        [TestCase("2025-05-06T21:31:00", false, Description = "Tick at market close")]
        public void ShouldSkipTickMarketClosed(string utcTimeString, bool expectedResult)
        {
            InteractiveBrokersBrokerage._nextNdxMarketOpenSkipTime = default;

            var symbol = Symbol.Create("NDX", SecurityType.Index, Market.USA);

            var ndxSecurityExchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            var DateTimeUtcNow = DateTime.Parse(utcTimeString).ConvertFromUtc(ndxSecurityExchangeHours.TimeZone);

            var result = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, DateTimeUtcNow);

            Assert.AreEqual(expectedResult, result);
        }

        [Test]
        public void ShouldSkipTickOnlyOnce()
        {
            InteractiveBrokersBrokerage._nextNdxMarketOpenSkipTime = default;

            var symbol = Symbol.Create("NDX", SecurityType.Index, Market.USA);

            var ndxSecurityExchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            // Tick before market open on 2025-05-06 - should not skip
            var firstLocalDateTime = DateTime.Parse("2025-05-06T09:30:00");
            var firstResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, firstLocalDateTime);
            Assert.IsTrue(firstResult, "Expected no skip on first tick before market open");

            // First tick after market open on 2025-05-06 - should skip
            var secondLocalDateTime = DateTime.Parse("2025-05-06T09:30:17");
            var secondResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, secondLocalDateTime);
            Assert.IsFalse(secondResult, "Expected skip after market open");
        }

        [Test]
        public void ShouldSkipTickOnlyOnceFromFridayToMonday()
        {
            InteractiveBrokersBrokerage._nextNdxMarketOpenSkipTime = default;

            var symbol = Symbol.Create("NDX", SecurityType.Index, Market.USA);

            var ndxSecurityExchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

            var fridayPreMarketDateTime = DateTime.Parse("2025-05-09T09:25:00");
            var fridayPreMarketResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, fridayPreMarketDateTime);

            Assert.IsFalse(fridayPreMarketResult);

            var fridayMarketOpenDateTimeFirstTick = DateTime.Parse("2025-05-09T09:30:01");
            var fridayMarketOpenFirstTickResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, fridayMarketOpenDateTimeFirstTick);

            Assert.IsTrue(fridayMarketOpenFirstTickResult);

            var fridayMarketOpenDateTimeSecondTick = DateTime.Parse("2025-05-09T09:30:01");
            var fridayMarketOpenSecondTickResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, fridayMarketOpenDateTimeSecondTick);

            Assert.IsFalse(fridayMarketOpenSecondTickResult);

            var sundayDateTime = DateTime.Parse("2025-05-11T09:30:01");
            var sundayResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, sundayDateTime);

            Assert.IsFalse(sundayResult);

            var mondayPreMarketDateTime = DateTime.Parse("2025-05-12T09:25:00");
            var mondayPreMarketResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, mondayPreMarketDateTime);

            Assert.IsFalse(mondayPreMarketResult);

            var mondayMarketOpenDateTimeFirstTick = DateTime.Parse("2025-05-12T09:30:01");
            var mondayMarketOpenFirstTickResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, mondayMarketOpenDateTimeFirstTick);

            Assert.IsTrue(mondayMarketOpenFirstTickResult);

            var mondayMarketOpenDateTimeSecondTick = DateTime.Parse("2025-05-12T09:30:01");
            var mondayMarketOpenSecondTickResult = InteractiveBrokersBrokerage.ShouldSkipTick(ndxSecurityExchangeHours, mondayMarketOpenDateTimeFirstTick);

            Assert.IsFalse(mondayMarketOpenSecondTickResult);
        }

        private static IEnumerable<TestCaseData> MarketOnOPenBoundaryRejectionTestData
        {
            get
            {
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-07-15T15:59:59.900", false, 0).SetDescription("Summer EDT - Before safe window - no wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-07-15T16:00:00.000", true, 5000).SetDescription("Summer EDT - Exactly at boundary - should wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-07-15T16:00:00.400", true, 4600).SetDescription("Summer EDT - Within safe buffer - should wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-07-15T16:00:05.000", false, 0).SetDescription("Summer EDT - After safe window - no wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-01-15T15:59:59.900", false, 0).SetDescription("Winter EST - Before safe window - no wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-01-15T16:00:00.000", true, 5000).SetDescription("Winter EST - Exactly at boundary - should wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-01-15T16:00:00.400", true, 4600).SetDescription("Winter EST - Within safe buffer - should wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-01-15T16:00:05.000", false, 0).SetDescription("Winter EST - After safe window - no wait");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-09-05T20:00:00.000", false, 0).SetDescription("Friday after regular market hours");
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-09-18T08:00:00.000", false, 0);
                yield return new TestCaseData(Symbols.AAPL, OrderType.MarketOnOpen, "2025-09-18T08:29:00.000", false, 0);

                yield return new TestCaseData(Symbols.BTCUSD, OrderType.MarketOnOpen, "2025-09-05T20:00:00.000", false, 0).SetDescription("Doesn't support SecurityType");
                yield return new TestCaseData(Symbols.SGX, OrderType.MarketOnOpen, "2025-09-05T20:00:00.000", false, 0).SetDescription("Doesn't support Market");
                yield return new TestCaseData(Symbols.AAPL, OrderType.Market, "2025-09-05T20:00:00.000", false, 0).SetDescription("Doesn't support OrderType");
            }
        }

        [Test, TestCaseSource(nameof(MarketOnOPenBoundaryRejectionTestData))]
        public void AvoidMarketOnOpenBoundaryRejection(Symbol symbol, OrderType orderType, string nyTimeString, bool expectDelay, int expectedDelayMs)
        {
            var nyTime = DateTime.Parse(nyTimeString);

            var result = _ib.TryAvoidMarketOnOpenBoundaryRejection(symbol, orderType, nyTime, out var delay);

            Assert.AreEqual(expectDelay, result, "Unexpected boolean result for delay requirement");
            Assert.AreEqual(expectedDelayMs, (int)delay.TotalMilliseconds, "Unexpected delay duration");
        }

        [Test]
        public void GetTradingClassReturnsTradingClassFromCache()
        {
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var future = Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new DateTime(2025, 09, 19));
            var index = Symbol.Create("SPX", SecurityType.Index, Market.USA);
            var symbols = new Symbol[]
            {
                future,
                Symbol.CreateOption(future, future.ID.Market, SecurityType.FutureOption.DefaultOptionStyle(), OptionRight.Put, 6725m, future.ID.Date),
                Symbol.CreateOption(future, future.ID.Market, SecurityType.FutureOption.DefaultOptionStyle(), OptionRight.Call, 6725m, future.ID.Date),
                Symbol.CreateOption(future, future.ID.Market, SecurityType.FutureOption.DefaultOptionStyle(), OptionRight.Put, 6750m, future.ID.Date),
                Symbol.CreateOption(future, future.ID.Market, SecurityType.FutureOption.DefaultOptionStyle(), OptionRight.Call, 6750m, future.ID.Date),

                Symbol.CreateOption(index, index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Call, 6300m, new(2025,08,15)),
                Symbol.CreateOption(index, index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Call, 6350m, new(2025,08,15)),
                Symbol.CreateOption(index, index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Put, 6395m, new(2025,08,15)),

                Symbol.CreateOption(index, "SPXW", index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Call, 6355m, new(2025,08,15)),
                Symbol.CreateOption(index, "SPXW", index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Put, 6320m, new(2025,08,15)),
                Symbol.CreateOption(index, "SPXW", index.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Put, 6310m, new(2025,08,15)),
            };

            var stopwatch = Stopwatch.StartNew();
            foreach (var symbol in symbols)
            {
                var contract = CreateContract(symbol);
                var tradingClass = ib._contractSpecificationService.GetTradingClass(contract, symbol);

                Assert.IsFalse(string.IsNullOrEmpty(tradingClass), $"Trading class should not be null or empty for symbol {symbol}");

                if (symbol.HasCanonical())
                {
                    Assert.IsTrue(ib._contractSpecificationService._tradingClassByCanonicalSymbol.TryGetValue(symbol.Canonical, out var contractSpecification),
                        $"Cache should contain canonical for symbol {symbol}");
                    Assert.IsNotEmpty(contractSpecification.TradingClass);
                    Assert.Greater(contractSpecification.MinTick, 0m);
                }
                else
                {
                    Assert.IsFalse(ib._contractSpecificationService._tradingClassByCanonicalSymbol.ContainsKey(symbol.Canonical));
                }
            }
            stopwatch.Stop();
            Log.Trace($"Test.GetTradingClassReturnsTradingClassFromCache: performance test elapsed time: {stopwatch.ElapsedMilliseconds} ms");

            ib.Disconnect();
        }

        private Contract CreateContract(Symbol symbol)
        {
            var securityType = InteractiveBrokersBrokerage.ConvertSecurityType(symbol.SecurityType);
            var ibSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(
                symbol.ID.Market,
                symbol,
                symbol.SecurityType,
                Currencies.USD);

            var contract = new Contract
            {
                Symbol = ibSymbol,
                Exchange = InteractiveBrokersBrokerage.GetSymbolExchange(symbol.SecurityType, symbol.ID.Market),
                SecType = securityType,
                Currency = symbolProperties.QuoteCurrency
            };

            if (symbol.ID.SecurityType.IsOption())
            {
                // Subtract a day from Index Options, since their last trading date
                // is on the day before the expiry.
                var lastTradeDate = symbol.ID.Date;
                if (symbol.SecurityType == SecurityType.IndexOption)
                {
                    lastTradeDate = IndexOptionSymbol.GetLastTradingDate(symbol.ID.Symbol, symbol.ID.Date);
                }
                contract.LastTradeDateOrContractMonth = lastTradeDate.ToStringInvariant(DateFormat.EightCharacter);

                contract.Right = symbol.ID.OptionRight == OptionRight.Call ? IB.RightType.Call : IB.RightType.Put;

                contract.Strike = Convert.ToDouble(symbol.ID.StrikePrice);

                contract.Symbol = ibSymbol;
                contract.Multiplier = symbolProperties.ContractMultiplier.ToStringInvariant();
            }

            return contract;
        }

        [Test, Explicit("Long-running test (~10 minutes). Compares LEAN and IB API primary exchanges for up to 1000 equity symbols.")]
        public void GetEquityPrimaryExchangeShouldMatchBetweenLeanAndIB()
        {
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var tickers = QuantConnect.Algorithm.CSharp.StressSymbols.StockSymbols.ToList();

            var totalCount = default(int);
            var equalCount = default(int);

            var logBuilder = new StringBuilder($"***** GetEquityPrimaryExchange Test ({tickers.Count} tickers) *****");
            var logBuilder2 = new StringBuilder("***** MissMatched Symbols *****");

            foreach (var ticker in tickers)
            {
                totalCount++;

                var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);

                var contract = CreateContract(symbol);

                var leanPrimaryExchange = ib.GetPrimaryExchange(contract, symbol);

                var ibPrimaryExchange = ib.GetContractDetails(contract, symbol.Value)?.Contract.PrimaryExch;

                if (ibPrimaryExchange == null)
                {
                    totalCount--;
                    logBuilder.AppendLine($"[SKIP] Contract not found for {symbol}");
                    continue;
                }

                if (totalCount == 1000)
                {
                    logBuilder.AppendLine("Symbol processing limit reached (1000). Stopping test.");
                    break;
                }

                Assert.IsNotNull(leanPrimaryExchange);

                bool isEqual = string.Equals(leanPrimaryExchange, ibPrimaryExchange, StringComparison.InvariantCultureIgnoreCase);
                if (isEqual)
                {
                    logBuilder.AppendLine($"[RESULT] {symbol.Value} | LEAN = {leanPrimaryExchange} | IB API = {ibPrimaryExchange} | Match = {isEqual}");
                    equalCount++;
                }
                else
                {
                    logBuilder2.AppendLine($"[RESULT] {symbol.Value} | LEAN = {leanPrimaryExchange} | IB API = {ibPrimaryExchange} | Match = {isEqual}");
                }
            }

            logBuilder.AppendLine("----- Test Summary -----");
            logBuilder.AppendLine($"Processed: {totalCount} | Matches: {equalCount} | Mismatches: {totalCount - equalCount}");

            Log.Trace(logBuilder.ToString());
            Log.Trace(logBuilder2.ToString());
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
                symbol.SecurityType != SecurityType.Cfd && symbol.SecurityType != SecurityType.Forex ? typeof(TradeBar) : typeof(QuoteBar),
                symbol,
                resolution,
                SecurityExchangeHours.AlwaysOpen(exchangeTimeZone),
                dataTimeZone,
                null,
                includeExtendedMarketHours,
                false,
                DataNormalizationMode.Raw,
                symbol.SecurityType != SecurityType.Cfd && symbol.SecurityType != SecurityType.Forex ? TickType.Trade : TickType.Quote);

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

            var forexSymbol = Symbol.Create("EURUSD", SecurityType.Forex, Market.Oanda);

            var indexCfdSymbol = Symbol.Create("IBUS500", SecurityType.Cfd, Market.InteractiveBrokers);
            var equityCfdSymbol = Symbol.Create("SPY", SecurityType.Cfd, Market.InteractiveBrokers);
            var forexCfdSymbol = Symbol.Create("EURUSD", SecurityType.Cfd, Market.InteractiveBrokers);
            // Londong Gold
            var metalCfdSymbol1 = Symbol.Create("XAUUSD", SecurityType.Cfd, Market.InteractiveBrokers);
            // Londong Silver
            var metalCfdSymbol2 = Symbol.Create("XAGUSD", SecurityType.Cfd, Market.InteractiveBrokers);

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

                new TestCaseData(forexSymbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 8),

                // delisted asset
                new TestCaseData(delistedEquity, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2021, 8, 6, 10, 0, 0), TimeSpan.FromHours(19), false, 0),

                // Index Cfd:
                // daily
                new TestCaseData(indexCfdSymbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 7),
                // hourly
                new TestCaseData(indexCfdSymbol, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(5), false, 75),
                // minute
                new TestCaseData(indexCfdSymbol, Resolution.Minute, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromMinutes(60 * 8), false, 420),
                // second
                new TestCaseData(indexCfdSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 22, 0, 0), TimeSpan.FromMinutes(60), false, 3600),

                // Equity Cfd:
                // daily
                new TestCaseData(equityCfdSymbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 8),
                // hourly
                new TestCaseData(equityCfdSymbol, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(5), false, 48),
                // minute
                new TestCaseData(equityCfdSymbol, Resolution.Minute, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromMinutes(60 * 8), false, 240),
                // second: only 1 RTH from 19 to 20
                new TestCaseData(equityCfdSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 22, 0, 0), TimeSpan.FromMinutes(3 * 60), false, 3600),

                // Forex Cfd:
                // daily
                new TestCaseData(forexCfdSymbol, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 8),
                // hourly
                new TestCaseData(forexCfdSymbol, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(5), false, 79),
                // minute
                new TestCaseData(forexCfdSymbol, Resolution.Minute, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromMinutes(60 * 8), false, 465),
                // second
                new TestCaseData(forexCfdSymbol, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 22, 0, 0), TimeSpan.FromMinutes(60), false, 3600),

                // Metal Cfd:
                // daily
                new TestCaseData(metalCfdSymbol1, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 8),
                new TestCaseData(metalCfdSymbol2, Resolution.Daily, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(10), true, 8),
                // hourly
                new TestCaseData(metalCfdSymbol1, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(5), false, 75),
                new TestCaseData(metalCfdSymbol2, Resolution.Hour, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromDays(5), false, 75),
                // minute
                new TestCaseData(metalCfdSymbol1, Resolution.Minute, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromMinutes(60 * 8), false, 420),
                new TestCaseData(metalCfdSymbol2, Resolution.Minute, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 0, 0, 0), TimeSpan.FromMinutes(60 * 8), false, 420),
                // second
                new TestCaseData(metalCfdSymbol1, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 22, 0, 0), TimeSpan.FromMinutes(60), false, 3600),
                new TestCaseData(metalCfdSymbol2, Resolution.Second, TimeZones.NewYork, TimeZones.NewYork,
                    new DateTime(2023, 12, 21, 22, 0, 0), TimeSpan.FromMinutes(60), false, 3600),
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
                securityProvider);
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

        private List<Order> CreateOrders(OrderType orderType, decimal comboLimitPrice, OrderDirection comboDirection, OrderDirection callDirection, OrderDirection secondCallDirection, bool addUnderlying, SecurityType securityType)
        {
            var algo = new AlgorithmStub();
            var group = new GroupOrderManager(1, legCount: 2, quantity: comboDirection == OrderDirection.Buy ? 2 : -2);

            var symbols = GetSymbols(securityType);
            var comboOrderCall = BuildOrder(orderType, symbols[1], callDirection == OrderDirection.Buy ? 1 : -1, comboLimitPrice, group, 0, null, algo.Transactions);
            var orders = new List<Order> { comboOrderCall };

            if (!addUnderlying)
            {
                var comboOrderPut = BuildOrder(orderType, symbols[2], secondCallDirection == OrderDirection.Buy ? 1 : -1, comboLimitPrice, group,
                    0, null, algo.Transactions);
                orders.Add(comboOrderPut);
            }
            else
            {
                orders.Add(BuildOrder(orderType, symbols[0], -100, comboLimitPrice, group, 0, null, algo.Transactions));
            }
            return orders;
        }

        private Symbol[] GetSymbols(SecurityType securityType)
        {
            if (securityType == SecurityType.Equity)
            {
                return [Symbols.SPY, Symbols.IBM, Symbols.AAPL];
            }
            else if (securityType == SecurityType.Option)
            {
                var underlying = Symbols.SPY;
                var optionsExpiration = new DateTime(2025, 12, 19);
                var symbol1 = Symbol.CreateOption(underlying, underlying.ID.Market, OptionStyle.American, OptionRight.Call, 430, optionsExpiration);
                var symbol2 = Symbol.CreateOption(underlying, underlying.ID.Market, OptionStyle.American, OptionRight.Call, 435, optionsExpiration);
                return [underlying, symbol1, symbol2];
            }
            else if (securityType == SecurityType.IndexOption)
            {
                var underlying = Symbols.SPX;
                var optionsExpiration = new DateTime(2025, 7, 18);
                var symbol1 = Symbol.CreateOption(underlying, "SPXW", underlying.ID.Market, OptionStyle.European, OptionRight.Call, 5505m, optionsExpiration);
                var symbol2 = Symbol.CreateOption(underlying, underlying.ID.Market, OptionStyle.European, OptionRight.Call, 5500m, optionsExpiration);
                return [underlying, symbol1, symbol2];
            }
            else if (securityType == SecurityType.FutureOption)
            {
                var underlying = Symbol.CreateFuture("ES", Market.CME, new DateTime(2025, 12, 19));
                var symbol1 = Symbol.CreateOption(underlying, Market.CME, OptionStyle.American, OptionRight.Call, 6000m, new DateTime(2025, 12, 19));
                var symbol2 = Symbol.CreateOption(underlying, Market.CME, OptionStyle.American, OptionRight.Call, 6100m, new DateTime(2025, 12, 19));
                return [underlying, symbol1, symbol2];
            }
            else if (securityType == SecurityType.Future)
            {
                var underlying = Symbol.CreateFuture("ES", Market.CME, new DateTime(2025, 9, 19));
                var underlying2 = Symbol.CreateFuture("ES", Market.CME, new DateTime(2025, 12, 19));
                return [null, underlying2, underlying];
            }
            throw new NotImplementedException();
        }
    }
}
