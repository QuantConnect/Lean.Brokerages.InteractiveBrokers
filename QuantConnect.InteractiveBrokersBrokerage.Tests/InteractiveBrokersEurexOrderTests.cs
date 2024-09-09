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

using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using System.Linq;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersEurexOrderTests : BrokerageTests
    {
        private static Symbol EuroStoxx50FutureSymbol = TestUtils.GetFutureContracts(Symbol.Create("FESX", SecurityType.Future, Market.EUREX), 1).Single();

        protected override Symbol Symbol => EuroStoxx50FutureSymbol;
        protected override SecurityType SecurityType => Symbol.SecurityType;

        private static TestCaseData[] EuroStoxx50FuturesOrderTest()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(EuroStoxx50FutureSymbol)),
                new TestCaseData(new LimitOrderTestParameters(EuroStoxx50FutureSymbol, 10000m, 10m)),
                new TestCaseData(new StopMarketOrderTestParameters(EuroStoxx50FutureSymbol, 10000m, 10m)),
                new TestCaseData(new StopLimitOrderTestParameters(EuroStoxx50FutureSymbol, 10000m, 10m)),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(EuroStoxx50FutureSymbol, 10000m, 10m)),
            };
        }

        #region EuroStoxx50 Futures

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void CancelOrders(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void LongFromZero(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void CloseFromLong(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void ShortFromZero(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void CloseFromShort(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void ShortFromLong(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(EuroStoxx50FuturesOrderTest))]
        public override void LongFromShort(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }

        #endregion

        protected override bool IsAsync()
        {
            return true;
        }

        protected override decimal GetAskPrice(Symbol symbol)
        {
            return 1m;
        }

        protected override IBrokerage CreateBrokerage(IOrderProvider orderProvider, ISecurityProvider securityProvider)
        {
            return new InteractiveBrokersBrokerage(new QCAlgorithm(), orderProvider, securityProvider);
        }

        protected override void DisposeBrokerage(IBrokerage brokerage)
        {
            if (brokerage != null)
            {
                brokerage.Disconnect();
                brokerage.Dispose();
            }
        }
    }
}
