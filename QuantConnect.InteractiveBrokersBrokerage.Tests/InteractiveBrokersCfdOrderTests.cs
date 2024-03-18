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

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersCfdOrderTests : BrokerageTests
    {
        private static Symbol IndexCfdSymbol = Symbol.Create("SPX500USD", SecurityType.Cfd, Market.Oanda);
        private static Symbol EquityCfdSymbol = Symbol.Create("AAPL", SecurityType.Cfd, Market.Oanda);
        private static Symbol ForexCfdSymbol = Symbol.Create("AUDUSD", SecurityType.Cfd, Market.Oanda);

        protected override Symbol Symbol => IndexCfdSymbol;
        protected override SecurityType SecurityType => SecurityType.Cfd;

        private static TestCaseData[] IndexCfdOrderTest()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(IndexCfdSymbol)),
                new TestCaseData(new LimitOrderTestParameters(IndexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopMarketOrderTestParameters(IndexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopLimitOrderTestParameters(IndexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(IndexCfdSymbol, 10000m, 0.01m)),
            };
        }

        private static TestCaseData[] EquityCfdOrderTest()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(EquityCfdSymbol)),
                new TestCaseData(new LimitOrderTestParameters(EquityCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopMarketOrderTestParameters(EquityCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopLimitOrderTestParameters(EquityCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(EquityCfdSymbol, 10000m, 0.01m)),
            };
        }

        private static TestCaseData[] ForexCfdOrderTest()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(ForexCfdSymbol)),
                new TestCaseData(new LimitOrderTestParameters(ForexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopMarketOrderTestParameters(ForexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopLimitOrderTestParameters(ForexCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(ForexCfdSymbol, 10000m, 0.01m)),
            };
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void CancelOrdersIndexCfd(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void LongFromZeroIndexCfd(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void CloseFromLongIndexCfd(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void ShortFromZeroIndexCfd(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void CloseFromShortIndexCfd(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void ShortFromLongIndexCfd(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(IndexCfdOrderTest))]
        public void LongFromShortIndexCfd(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void CancelOrdersEquityCfd(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void LongFromZeroEquityCfd(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void CloseFromLongEquityCfd(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void ShortFromZeroEquityCfd(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void CloseFromShortEquityCfd(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void ShortFromLongEquityCfd(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(EquityCfdOrderTest))]
        public void LongFromShortEquityCfd(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void CancelOrdersForexCfd(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void LongFromZeroForexCfd(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void CloseFromLongForexCfd(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void ShortFromZeroForexCfd(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void CloseFromShortForexCfd(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void ShortFromLongForexCfd(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(ForexCfdOrderTest))]
        public void LongFromShortForexCfd(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }

        // TODO: Add tests to get holdings after placing orders

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
