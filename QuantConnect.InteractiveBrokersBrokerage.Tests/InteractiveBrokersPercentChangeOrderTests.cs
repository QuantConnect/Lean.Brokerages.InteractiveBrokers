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
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Util;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// IB sizes a Financial Advisor group order with the PctChange method from the accounts' positions, ignoring the order quantity.
    /// The size IB gives it comes in the open order, before its executions, and is passed on to Lean as an order update.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersPercentChangeOrderTests
    {
        private const int IbOrderId = 605;

        private static readonly MethodInfo HandleOpenOrderMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod("HandleOpenOrder", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly MethodInfo EmitOrderFillMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod("EmitOrderFill", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly PropertyInfo OrderQuantityProperty = typeof(Order).GetProperty(nameof(Order.Quantity));

        private OrderProvider _orderProvider;
        private InteractiveBrokersBrokerage _brokerage;
        private List<OrderUpdateEvent> _orderUpdates;
        private List<OrderEvent> _orderEvents;

        [SetUp]
        public void SetUp()
        {
            _orderProvider = new OrderProvider();
            _brokerage = new InteractiveBrokersBrokerage();
            var client = new IB.InteractiveBrokersClient(new EReaderMonitorSignal());
            SetPrivateFieldValue(client, "<ClientSocket>k__BackingField", new ConnectedClientSocket(client));
            SetPrivateFieldValue(_brokerage, "_client", client);
            SetPrivateFieldValue(_brokerage, "_orderProvider", _orderProvider);
            SetPrivateFieldValue(_brokerage, "_symbolMapper", new InteractiveBrokersSymbolMapper(Composer.Instance.GetPart<IMapFileProvider>()));

            _orderUpdates = new List<OrderUpdateEvent>();
            _orderEvents = new List<OrderEvent>();
            _brokerage.OrderUpdated += (_, e) => _orderUpdates.Add(e);
            _brokerage.OrdersStatusChanged += (_, events) => _orderEvents.AddRange(events);
        }

        [TearDown]
        public void TearDown()
        {
            _brokerage.Dispose();
        }

        [TestCase(1, 3, 3)]
        [TestCase(1, 2.4, 2.4)]
        [TestCase(10, 3, 3)]
        [TestCase(-1, 3, -3)]
        [TestCase(-10, 0.4, -0.4)]
        public void OpenOrderUpdatesTheQuantityToTheSizeIbGaveIt(decimal leanQuantity, decimal ibQuantity, decimal expectedQuantity)
        {
            var order = AddOrder(leanQuantity, "PctChange");

            HandleOpenOrder(ibQuantity, "PctChange");

            Assert.AreEqual(1, _orderUpdates.Count);
            Assert.AreEqual(order.Id, _orderUpdates[0].OrderId);
            Assert.AreEqual(expectedQuantity, _orderUpdates[0].Quantity);
        }

        [TestCase(3, "PctChange", 3)]
        [TestCase(-3, "PctChange", 3)]
        [TestCase(1, "PctChange", 0)]
        [TestCase(1, "Equal", 3)]
        [TestCase(1, "", 3)]
        public void OpenOrderLeavesTheQuantityAsIs(decimal leanQuantity, string faMethod, decimal ibQuantity)
        {
            AddOrder(leanQuantity, faMethod);

            HandleOpenOrder(ibQuantity, faMethod);

            Assert.IsEmpty(_orderUpdates);
        }

        /// <summary>
        /// Buy 1 that IB sizes at 2.4: a fill of 1 leaves the order open, the fill reaching 2.4 closes it
        /// </summary>
        [Test]
        public void FillsOfALargerSizeAreFilledOnlyOnceIbFilledItsSize()
        {
            var order = AddOrder(1, "PctChange");
            HandleOpenOrderAndApplyTheUpdate(order, 2.4m);

            EmitOrderFill(order, shares: 1, cumulativeQuantity: 1);
            EmitOrderFill(order, shares: 1.4m, cumulativeQuantity: 2.4m);

            Assert.AreEqual(2, _orderEvents.Count);
            Assert.AreEqual(OrderStatus.PartiallyFilled, _orderEvents[0].Status);
            Assert.AreEqual(1, _orderEvents[0].FillQuantity);
            StringAssert.EndsWith(" - 1.4 remaining", _orderEvents[0].Message);
            Assert.AreEqual(OrderStatus.Filled, _orderEvents[1].Status);
            Assert.AreEqual(1.4m, _orderEvents[1].FillQuantity);
        }

        /// <summary>
        /// Sell 10 that IB sizes at 3: the fill of 3 closes the order
        /// </summary>
        [Test]
        public void FillOfASmallerSizeIsFilled()
        {
            var order = AddOrder(-10, "PctChange");
            HandleOpenOrderAndApplyTheUpdate(order, 3);

            EmitOrderFill(order, shares: 3, cumulativeQuantity: 3);

            Assert.AreEqual(1, _orderEvents.Count);
            Assert.AreEqual(OrderStatus.Filled, _orderEvents[0].Status);
            Assert.AreEqual(-3, _orderEvents[0].FillQuantity);
        }

        private Order AddOrder(decimal quantity, string faMethod)
        {
            var properties = new InteractiveBrokersOrderProperties { FaGroup = "TestGroup", FaMethod = faMethod, FaPercentage = 300 };
            var order = new LimitOrder(Symbols.AAPL, quantity, 300m, new DateTime(2026, 11, 17), properties: properties);
            // assigns the Lean order id
            _orderProvider.Add(order);
            order.BrokerId.Add(IbOrderId.ToStringInvariant());
            return order;
        }

        private void HandleOpenOrder(decimal ibQuantity, string faMethod)
        {
            var contract = new Contract { Symbol = "AAPL", SecType = IB.SecurityType.Stock, Exchange = "SMART", Currency = "USD" };
            var ibOrder = new IBApi.Order { OrderId = IbOrderId, TotalQuantity = ibQuantity, FaGroup = "TestGroup", FaMethod = faMethod, FaPercentage = "300" };
            var orderState = new OrderState { Status = IB.OrderStatus.Submitted };
            HandleOpenOrderMethod.Invoke(_brokerage, new object[] { null, new IB.OpenOrderEventArgs(IbOrderId, contract, ibOrder, orderState) });
        }

        /// <summary>
        /// The open order, then the quantity update applied to the order as Lean's transaction handler does
        /// </summary>
        private void HandleOpenOrderAndApplyTheUpdate(Order order, decimal ibQuantity)
        {
            HandleOpenOrder(ibQuantity, "PctChange");
            OrderQuantityProperty.SetValue(order, _orderUpdates.Single().Quantity.Value);
        }

        private void EmitOrderFill(Order order, decimal shares, decimal cumulativeQuantity)
        {
            var contract = new Contract { Symbol = "AAPL", SecType = IB.SecurityType.Stock, Exchange = "SMART", Currency = "USD" };
            var execId = $"000{_orderEvents.Count}";
            var execution = new Execution
            {
                OrderId = IbOrderId,
                ExecId = execId,
                Side = order.Direction == OrderDirection.Buy ? "BOT" : "SLD",
                Shares = shares,
                CumQty = cumulativeQuantity,
                Price = 300
            };
            var commissionReport = new CommissionAndFeesReport { ExecId = execId, CommissionAndFees = 1, Currency = "USD" };
            // the order as the brokerage gets it for each execution
            var leanOrder = _orderProvider.GetOrdersByBrokerageId(IbOrderId.ToStringInvariant()).Single();
            EmitOrderFillMethod.Invoke(_brokerage, new object[] { leanOrder, new IB.ExecutionDetailsEventArgs(1, contract, execution), commissionReport, false });
        }

        private static void SetPrivateFieldValue(object instance, string name, object value)
        {
            instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(instance, value);
        }

        /// <summary>
        /// An IB client socket that reports itself connected, without a gateway
        /// </summary>
        private class ConnectedClientSocket : EClientSocket
        {
            public ConnectedClientSocket(EWrapper wrapper) : base(wrapper, new EReaderMonitorSignal())
            {
                isConnected = true;
            }
        }
    }
}
