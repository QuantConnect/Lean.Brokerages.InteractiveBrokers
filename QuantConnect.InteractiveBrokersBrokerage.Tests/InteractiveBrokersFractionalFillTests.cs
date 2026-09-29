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
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Util;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Fills of fractional quantities keep the exact IB quantity. IB refuses to place or modify fractional orders from the API
    /// (10243, 10242), so a live fill can't be driven by a test; the IB execution is fed to the private <c>EmitOrderFill</c> instead.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersFractionalFillTests
    {
        private static readonly FieldInfo SymbolMapperField =
            typeof(InteractiveBrokersBrokerage).GetField("_symbolMapper", BindingFlags.Instance | BindingFlags.NonPublic);

        private static readonly MethodInfo EmitOrderFillMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod("EmitOrderFill", BindingFlags.Instance | BindingFlags.NonPublic);

        [TestCase(0.4, 0.4, 0.4, 0.4, OrderStatus.Filled)]
        [TestCase(1, 0.4, 0.4, 0.4, OrderStatus.PartiallyFilled)]
        [TestCase(1, 0.6, 1, 0.6, OrderStatus.Filled)]
        [TestCase(10.6, 10.6, 10.6, 10.6, OrderStatus.Filled)]
        [TestCase(-0.4, 0.4, 0.4, -0.4, OrderStatus.Filled)]
        public void FractionalFillKeepsExactQuantity(decimal orderQuantity, decimal shares, decimal cumulativeQuantity, decimal expectedFillQuantity, OrderStatus expectedStatus)
        {
            var brokerage = new InteractiveBrokersBrokerage();
            SymbolMapperField.SetValue(brokerage, new InteractiveBrokersSymbolMapper(Composer.Instance.GetPart<IMapFileProvider>()));

            var order = new LimitOrder(Symbols.AAPL, orderQuantity, 300m, new DateTime(2026, 9, 29));
            // assigns the Lean order id
            new OrderProvider().Add(order);
            order.BrokerId.Add("-3");

            var contract = new Contract { Symbol = "AAPL", SecType = IB.SecurityType.Stock, Exchange = "SMART", Currency = "USD" };
            var execution = new Execution
            {
                OrderId = -3,
                ExecId = "0001",
                Side = orderQuantity > 0 ? "BOT" : "SLD",
                Shares = shares,
                CumQty = cumulativeQuantity,
                Price = 330.75
            };
            var commissionReport = new CommissionAndFeesReport { ExecId = "0001", CommissionAndFees = 1, Currency = "USD" };

            var orderEvents = new List<OrderEvent>();
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            EmitOrderFillMethod.Invoke(brokerage, new object[] { order, new IB.ExecutionDetailsEventArgs(1, contract, execution), commissionReport, false });

            Assert.AreEqual(1, orderEvents.Count);
            Assert.AreEqual(expectedFillQuantity, orderEvents[0].FillQuantity);
            Assert.AreEqual(expectedStatus, orderEvents[0].Status);
        }
    }
}
