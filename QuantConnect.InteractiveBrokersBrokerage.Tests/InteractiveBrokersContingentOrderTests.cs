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
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Brokerages.InteractiveBrokers;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersContingentOrderTests : BrokerageTests
    {
        protected override Symbol Symbol => Symbols.SPY;
        protected override SecurityType SecurityType => SecurityType.Equity;

        private static readonly OrderTestParameters Limit = new LimitOrderTestParameters(Symbols.SPY, 5000m, 100m);
        private static readonly OrderTestParameters OtherLimit = new LimitOrderTestParameters(Symbols.SPY, 5100m, 90m);
        private static readonly OrderTestParameters Stop = new StopMarketOrderTestParameters(Symbols.SPY, 5000m, 50m);
        private static readonly OrderTestParameters Market = new MarketOrderTestParameters(Symbols.SPY);

        /// <summary>
        /// Resting sets: the prices are far from the market
        /// </summary>
        private static TestCaseData[] RestingOrders()
        {
            return new[]
            {
                new TestCaseData(ContingentOrderTestParameters.OneCancelsOther(Limit, OtherLimit)),
                new TestCaseData(ContingentOrderTestParameters.OneUpdatesOther(Limit, OtherLimit)),
                new TestCaseData(ContingentOrderTestParameters.OneTriggersOther(Limit, OtherLimit)),
                new TestCaseData(ContingentOrderTestParameters.Bracket(Limit, OtherLimit, Stop))
            };
        }

        /// <summary>
        /// Sets where the first order fills right away
        /// </summary>
        private static TestCaseData[] TriggeredOrders()
        {
            return new[]
            {
                new TestCaseData(ContingentOrderTestParameters.OneTriggersOther(Market, Limit)),
                new TestCaseData(ContingentOrderTestParameters.Bracket(Market, Limit, Stop))
            };
        }

        [Test, TestCaseSource(nameof(RestingOrders))]
        public override void ContingentOrdersCancel(ContingentOrderTestParameters parameters)
        {
            base.ContingentOrdersCancel(parameters);
        }

        [Test, TestCaseSource(nameof(RestingOrders))]
        public override void ContingentOrdersUpdate(ContingentOrderTestParameters parameters)
        {
            base.ContingentOrdersUpdate(parameters);
        }

        [Test, TestCaseSource(nameof(TriggeredOrders))]
        public override void ContingentOrdersTrigger(ContingentOrderTestParameters parameters)
        {
            base.ContingentOrdersTrigger(parameters);
        }

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
