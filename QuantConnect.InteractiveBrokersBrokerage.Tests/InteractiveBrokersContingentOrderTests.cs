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
using System.Linq;
using NUnit.Framework;
using QuantConnect.Orders;
using QuantConnect.Algorithm;
using System.Collections.Generic;
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
                new TestCaseData(ContingentOrderTestParameters.Bracket(Limit, OtherLimit, Stop)),
                new TestCaseData(ComboOneCancelsOther(0.10m, 0.05m)),
                new TestCaseData(ComboOneTriggersOther(0.10m, 3.5m))
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
                new TestCaseData(ContingentOrderTestParameters.Bracket(Market, Limit, Stop)),
                new TestCaseData(ComboOneTriggersOther(null, 3.5m))
            };
        }

        /// <summary>
        /// Two combo buy orders of a SPY call spread where the first one to fill cancels the other
        /// </summary>
        private static ContingentOrderTestParameters ComboOneCancelsOther(decimal firstLimitPrice, decimal secondLimitPrice)
        {
            return new($"{ContingencyType.OneCancelsOther} combo limit {firstLimitPrice}, combo limit {secondLimitPrice}", quantity =>
            {
                var first = CreateCallSpread(quantity, firstLimitPrice);
                var second = CreateCallSpread(quantity, secondLimitPrice);
                OrderContingency.Relate(ContingencyType.OneCancelsOther, first.Concat(second));
                return [.. first, .. second];
            });
        }

        /// <summary>
        /// A combo buy order of a SPY call spread which once filled triggers the combo sell order which exits it, a market parent if no limit price is given
        /// </summary>
        private static ContingentOrderTestParameters ComboOneTriggersOther(decimal? parentLimitPrice, decimal exitLimitPrice)
        {
            return new($"{ContingencyType.OneTriggersOther} combo {(parentLimitPrice.HasValue ? $"limit {parentLimitPrice}" : "market")} -> combo limit {exitLimitPrice}", quantity =>
            {
                var parent = CreateCallSpread(quantity, parentLimitPrice);
                var exit = CreateCallSpread(-quantity, exitLimitPrice);
                OrderContingency.Trigger(parent, exit);
                return [.. parent, .. exit];
            });
        }

        /// <summary>
        /// The legs of a SPY call spread combo order, a combo market order if no limit price is given
        /// </summary>
        private static List<Order> CreateCallSpread(decimal quantity, decimal? limitPrice)
        {
            var legs = new[] { (Strike: 765m, Ratio: 1), (Strike: 770m, Ratio: -1) };
            var groupOrderManager = new GroupOrderManager(legs.Length, quantity, limitPrice ?? 0m);
            return legs.Select(leg =>
            {
                var option = Symbol.CreateOption(Symbols.SPY, QuantConnect.Market.USA, OptionStyle.American, OptionRight.Call, leg.Strike, new DateTime(2026, 10, 2));
                return limitPrice.HasValue
                    ? (Order)new ComboLimitOrder(option, leg.Ratio * quantity, limitPrice.Value, DateTime.UtcNow, groupOrderManager)
                    : new ComboMarketOrder(option, leg.Ratio * quantity, DateTime.UtcNow, groupOrderManager);
            }).ToList();
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

        [Test, TestCaseSource(nameof(RestingOrders))]
        public override void ContingentOrdersGetOpenOrders(ContingentOrderTestParameters parameters)
        {
            base.ContingentOrdersGetOpenOrders(parameters);
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
