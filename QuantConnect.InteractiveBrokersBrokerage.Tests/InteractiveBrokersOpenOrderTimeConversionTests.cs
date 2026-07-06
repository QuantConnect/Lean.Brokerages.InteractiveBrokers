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
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Util;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using LeanOrder = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Hermetic tests for the open-order timestamp behavior. Orders rebuilt by <c>GetOpenOrders</c>
    /// during live setup carry no submission time from IB, so the brokerage stamps the discovery time
    /// (<see cref="DateTime.UtcNow"/>, rounded down to the minute) instead of <see cref="DateTime.MinValue"/>,
    /// keeping time-based cancel/age logic working after a restart. The private <c>ConvertOrder</c>
    /// overload is invoked via reflection (the assembly exposes its internals to this test project) so the
    /// test runs in CI without a live IB Gateway / TWS.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersOpenOrderTimeConversionTests
    {
        private static readonly FieldInfo SymbolMapperField =
            typeof(InteractiveBrokersBrokerage).GetField("_symbolMapper", BindingFlags.Instance | BindingFlags.NonPublic);

        private static readonly MethodInfo ConvertOrderMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod(
                "ConvertOrder",
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                types: new[]
                {
                    typeof(string), typeof(string), typeof(int), typeof(double), typeof(OrderType),
                    typeof(decimal), typeof(double), typeof(double), typeof(double), typeof(Contract),
                    typeof(GroupOrderManager), typeof(OrderState)
                },
                modifiers: null);

        [TestCase(OrderType.Limit)]
        [TestCase(OrderType.MarketOnOpen)]
        public void OpenOrderTimeIsDiscoveryTimeNotMinValue(OrderType orderType)
        {
            var brokerage = CreateOfflineBrokerage();

            var before = DateTime.UtcNow;
            var order = InvokeConvert(brokerage, orderType, limitPrice: 100.00);
            var after = DateTime.UtcNow;

            Assert.AreNotEqual(default(DateTime), order.Time, "open order time must not be DateTime.MinValue");
            // stamped with the discovery time, rounded down to the minute
            Assert.AreEqual(0, order.Time.Second, "time must be rounded down to the minute");
            Assert.AreEqual(0, order.Time.Millisecond, "time must be rounded down to the minute");
            Assert.GreaterOrEqual(order.Time, before.AddMinutes(-1));
            Assert.LessOrEqual(order.Time, after);
        }

        private static LeanOrder InvokeConvert(InteractiveBrokersBrokerage brokerage, OrderType orderType, double limitPrice = 0.0)
        {
            var contract = new Contract
            {
                Symbol = "SPY", SecType = IB.SecurityType.Stock, Exchange = "SMART", Currency = "USD"
            };
            var orderState = new OrderState { Status = "Submitted" };

            return (LeanOrder)ConvertOrderMethod.Invoke(brokerage, new object[]
            {
                IB.TimeInForce.Day,   // timeInForce
                string.Empty,         // goodTillDate
                1,                    // ibOrderId
                0.0,                  // auxPrice
                orderType,            // orderType
                10m,                  // quantity
                limitPrice,           // limitPrice
                0.0,                  // trailingStopPrice
                0.0,                  // trailingPercentage
                contract,             // contract
                null,                 // groupOrderManager
                orderState            // orderState
            });
        }

        /// <summary>
        /// Builds an <see cref="InteractiveBrokersBrokerage"/> with just enough state for the inbound
        /// <c>ConvertOrder</c> to run without opening a connection to TWS / IB Gateway. Only the symbol
        /// mapper is required (price normalization uses a static symbol-properties database).
        /// </summary>
        private static InteractiveBrokersBrokerage CreateOfflineBrokerage()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            SymbolMapperField.SetValue(brokerage,
                new InteractiveBrokersSymbolMapper(Composer.Instance.GetPart<IMapFileProvider>()));
            return brokerage;
        }
    }
}
