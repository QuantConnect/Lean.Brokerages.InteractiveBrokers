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
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Securities.Option;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersCfdOrderTests : BrokerageTests
    {
        private static Symbol IndexCfdSymbol = Symbol.Create("IBUS500", SecurityType.Cfd, Market.InteractiveBrokers);
        private static Symbol EquityCfdSymbol = Symbol.Create("AAPL", SecurityType.Cfd, Market.InteractiveBrokers);
        private static Symbol ForexCfdSymbol = Symbol.Create("AUDUSD", SecurityType.Cfd, Market.InteractiveBrokers);
        private static Symbol MetalCfdSymbol = Symbol.Create("XAUUSD", SecurityType.Cfd, Market.InteractiveBrokers);

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

        private static TestCaseData[] MetalCfdOrderTest()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(MetalCfdSymbol)),
                new TestCaseData(new LimitOrderTestParameters(MetalCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopMarketOrderTestParameters(MetalCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new StopLimitOrderTestParameters(MetalCfdSymbol, 10000m, 0.01m)),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(MetalCfdSymbol, 10000m, 0.01m)),
            };
        }

        #region Index CFDs

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

        #endregion

        #region Equity CFDs

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

        #endregion

        #region Forex CFDs

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

        #endregion

        #region Metal CFDs

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void CancelOrdersMetalCfd(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void LongFromZeroMetalCfd(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void CloseFromLongMetalCfd(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void ShortFromZeroMetalCfd(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void CloseFromShortMetalCfd(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void ShortFromLongMetalCfd(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(MetalCfdOrderTest))]
        public void LongFromShortMetalCfd(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }

        [Test]
        public void PlaceMarketOnOpen()
        {
            PlaceOrderWaitForStatus(new MarketOnOpenOrderTestParameters(EquityCfdSymbol).CreateLongOrder(1), Orders.OrderStatus.Submitted);
        }

        [Test]
        public void PlaceMarketOnClose()
        {
            PlaceOrderWaitForStatus(new MarketOnCloseOrderTestParameters(EquityCfdSymbol).CreateLongOrder(1), Orders.OrderStatus.Submitted);
        }

        #endregion

        // TODO: Add tests to get holdings after placing orders

        [Test]
        public void ComboLimitOrderIndex()
        {
            var spx = Symbol.Create("SPX", SecurityType.Index, Market.USA);
            var spxCanonical = Symbol.CreateCanonicalOption(spx, targetOption: "SPXW");
            var parameters = new ComboLimitOrderTestParameters(
                OptionStrategies.BullPutSpread(spxCanonical, 7510m, 7500m, new DateTime(2026, 02, 20)),
                askPrice: 0.75m,
                bidPrice: -0.12m);
            base.CancelComboOrders(parameters);
        }

        private static IEnumerable<TestCaseData> ComboLegLimitParameters
        {
            get
            {
                var spx = Symbol.Create("SPX", SecurityType.Index, Market.USA);
                var spxCanonical = Symbol.CreateCanonicalOption(spx, targetOption: "SPXW");

                var legLimitOrdersPriceAboveThree = CreateLegLimitOrders(OptionStrategies.BullPutSpread(spxCanonical, 6980m, 6970m, new DateTime(2026, 01, 29)), 1, 7.35m, 8.45m);
                yield return new TestCaseData(legLimitOrdersPriceAboveThree).SetDescription("ComboLegLimitOrderIndex_BullPutSpread_PriceGreaterThree");

                var legLimitOrdersPriceBellowThree = CreateLegLimitOrders(OptionStrategies.BullPutSpread(spxCanonical, 7510m, 7500m, new DateTime(2026, 02, 20)), 1, 0.77m, 0.78m);
                yield return new TestCaseData(legLimitOrdersPriceBellowThree).SetDescription("ComboLegLimitOrderIndex_BearCallSpread_PriceBellowThree");

            }
        }

        [TestCaseSource(nameof(ComboLegLimitParameters))]
        public void ComboLegLimitOrderIndexOption(List<ComboLegLimitOrder> comboLegLimitOrders)
        {
            foreach (var order in comboLegLimitOrders)
            {
                OrderProvider.Add(order);
                Brokerage.PlaceOrder(order);
            }

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

        private static List<ComboLegLimitOrder> CreateLegLimitOrders(OptionStrategy strategy, decimal quantity, params decimal[] limitPrices)
        {
            var targetOption = strategy.CanonicalOption?.Canonical.ID.Symbol;

            var legs = new List<Leg>(strategy.UnderlyingLegs);

            foreach (var optionLeg in strategy.OptionLegs)
            {
                var option = Symbol.CreateOption(
                    strategy.Underlying,
                    targetOption,
                    strategy.Underlying.ID.Market,
                    strategy.Underlying.SecurityType.DefaultOptionStyle(),
                    optionLeg.Right,
                    optionLeg.Strike,
                    optionLeg.Expiration);

                legs.Add(new Leg { Symbol = option, OrderPrice = optionLeg.OrderPrice, Quantity = optionLeg.Quantity });
            }

            var groupOrderManager = new GroupOrderManager(1, legs.Count, quantity);

            var orders = new List<ComboLegLimitOrder>();
            for (int i = 0; i < limitPrices.Length; i++)
            {
                orders.Add(CreateComboLimitOrder(legs[i], limitPrices[i], groupOrderManager));
            }

            return orders;
        }

        private static ComboLegLimitOrder CreateComboLimitOrder(Leg leg, decimal limitPrice, GroupOrderManager groupOrderManager)
        {
            return new ComboLegLimitOrder(
                leg.Symbol,
                ((decimal)leg.Quantity).GetOrderLegGroupQuantity(groupOrderManager),
                limitPrice,
                DateTime.UtcNow,
                groupOrderManager)
            {
                Status = OrderStatus.New
            };
        }
    }
}
