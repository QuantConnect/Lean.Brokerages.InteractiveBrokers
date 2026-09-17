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
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine.DataFeeds;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersCryptoTests
    {
        // the brokerage model's default crypto market
        private static readonly Symbol _btcusd = Symbol.Create("BTCUSD", SecurityType.Crypto, Market.Coinbase);
        private static readonly DateTime _orderTime = new(2024, 1, 3);

        [Test]
        public void MapsSecurityTypeToTheIBCryptoContract()
        {
            Assert.AreEqual(IB.SecurityType.Crypto, InteractiveBrokersBrokerage.ConvertSecurityType(SecurityType.Crypto));
            Assert.AreEqual(SecurityType.Crypto, InteractiveBrokersBrokerage.ConvertSecurityType(IB.SecurityType.Crypto, "BTC"));
        }

        [Test]
        public void RoutesCryptoToPaxos()
        {
            Assert.AreEqual("PAXOS", InteractiveBrokersBrokerage.GetSymbolExchange(SecurityType.Crypto, Market.InteractiveBrokers));
        }

        [TestCase("BTCUSD")]
        [TestCase("ETHUSD")]
        public void MapsLeanAndBrokerageSymbols(string ticker)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var symbol = Symbol.Create(ticker, SecurityType.Crypto, Market.InteractiveBrokers);

            Assert.AreEqual(ticker, mapper.GetBrokerageSymbol(symbol));

            var leanSymbol = mapper.GetLeanSymbol(ticker, SecurityType.Crypto, Market.InteractiveBrokers);
            Assert.AreEqual(ticker, leanSymbol.Value);
            Assert.AreEqual(SecurityType.Crypto, leanSymbol.ID.SecurityType);
            Assert.AreEqual(Market.InteractiveBrokers, leanSymbol.ID.Market);
        }

        // IB splits the pair into base currency (contract symbol) and quote currency
        [TestCase("BTCUSD", "BTC")]
        [TestCase("ETHUSD", "ETH")]
        [TestCase("PAXGUSD", "PAXG")]
        public void CreatesTheCryptoContract(string ticker, string expectedSymbol)
        {
            using var brokerage = CreateBrokerage();

            var contract = CreateContract(brokerage, Symbol.Create(ticker, SecurityType.Crypto, Market.InteractiveBrokers));

            Assert.AreEqual(expectedSymbol, contract.Symbol);
            Assert.AreEqual(Currencies.USD, contract.Currency);
            Assert.AreEqual(IB.SecurityType.Crypto, contract.SecType);
            Assert.AreEqual("PAXOS", contract.Exchange);
        }

        // on the default crypto market, matching the algorithm's securities
        [Test]
        public void MapsTheCryptoContractBackToTheLeanSymbol()
        {
            using var brokerage = CreateBrokerage();

            var symbol = MapSymbol(brokerage, new Contract
            {
                Symbol = "BTC",
                SecType = IB.SecurityType.Crypto,
                Currency = Currencies.USD,
                Exchange = "PAXOS"
            });

            Assert.AreEqual(_btcusd, symbol);
        }

        [Test]
        public void SubscribesToCrypto()
        {
            Assert.IsTrue(CanSubscribe(_btcusd));
        }

        // only the pairs the database lists for IB, which are the ones IB answers a contract for
        [TestCase("BTCUSD", true)]
        [TestCase("ETHUSD", true)]
        [TestCase("SOLUSD", true)]
        [TestCase("BTCEUR", false)]   // IB quotes crypto against US dollars only
        [TestCase("ETHBTC", false)]   // no crypto quoted pairs either
        [TestCase("ZRXUSD", false)]   // a crypto market pair IB does not list
        public void SubscribesOnlyToListedCryptoPairs(string ticker, bool expected)
        {
            Assert.AreEqual(expected, CanSubscribe(Symbol.Create(ticker, SecurityType.Crypto, Market.InteractiveBrokers)));
        }

        // IB's tick applies whatever market the symbol is on
        [TestCase("BTCUSD", 80400.30, 80400.25)]
        [TestCase("ETHUSD", 2515.86, 2515.85)]
        public void RoundsCryptoPricesToTheBrokerageTick(string ticker, decimal price, decimal expected)
        {
            using var brokerage = CreateBrokerage();

            foreach (var market in new[] { Market.InteractiveBrokers, Market.Coinbase })
            {
                var symbol = Symbol.Create(ticker, SecurityType.Crypto, market);
                var contract = CreateContract(brokerage, symbol);

                Assert.AreEqual((double)expected, brokerage.NormalizePriceToBrokerage(price, contract, symbol), $"market {market}");
            }
        }

        // the same pair subscribed on the crypto market still routes here
        [TestCase("BTCUSD", true)]
        [TestCase("ZRXUSD", false)]
        public void SubscribesToListedPairsOnAnyMarket(string ticker, bool expected)
        {
            Assert.AreEqual(expected, CanSubscribe(Symbol.Create(ticker, SecurityType.Crypto, Market.Coinbase)));
        }

        // crypto is the only IB security type that trades in fractional units
        [Test]
        public void KeepsFractionalQuantitiesOnLimitOrders()
        {
            using var brokerage = CreateBrokerage();
            var order = new LimitOrder(_btcusd, 0.00001234m, 3370m, _orderTime);

            var ibOrder = ConvertOrder(brokerage, order);

            Assert.AreEqual(0.00001234m, ibOrder.TotalQuantity);
            Assert.AreEqual(IB.ActionSide.Buy, ibOrder.Action);
            // IB rejects anything but IOC or its five minute expiry with error 201
            Assert.AreEqual(IB.TimeInForce.Minutes, ibOrder.Tif);
            // CashQty is left at its unset default, only crypto buy market orders use it
            Assert.AreEqual(double.MaxValue, ibOrder.CashQty);
        }

        // IB only accepts crypto market orders as immediate-or-cancel
        [Test]
        public void SendsSellMarketOrdersAsImmediateOrCancelWithAQuantity()
        {
            using var brokerage = CreateBrokerage();
            var order = new MarketOrder(_btcusd, -0.5m, _orderTime);

            var ibOrder = ConvertOrder(brokerage, order);

            Assert.AreEqual(IB.TimeInForce.ImmediateOrCancel, ibOrder.Tif);
            Assert.AreEqual(IB.ActionSide.Sell, ibOrder.Action);
            Assert.AreEqual(0.5m, ibOrder.TotalQuantity);
            // sells are sized by quantity, only buys use a cash amount
            Assert.AreEqual(double.MaxValue, ibOrder.CashQty);
        }

        // IB sizes crypto buy market orders by the cash amount to spend instead of by quantity
        [Test]
        public void SendsBuyMarketOrdersWithACashQuantity()
        {
            var algorithm = new AlgorithmStub();
            algorithm.SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage);
            var security = algorithm.AddCrypto("BTCUSD");
            security.SetMarketPrice(new Tick(_orderTime, security.Symbol, 40000m, 40000m));

            using var brokerage = CreateBrokerage();
            SetPrivateField(brokerage, "_algorithm", algorithm);

            var ibOrder = ConvertOrder(brokerage, new MarketOrder(security.Symbol, 0.25m, _orderTime));

            Assert.AreEqual(IB.TimeInForce.ImmediateOrCancel, ibOrder.Tif);
            Assert.AreEqual(0.25 * 40000, ibOrder.CashQty);
            Assert.AreEqual(0m, ibOrder.TotalQuantity);
        }

        [Test]
        public void FailsBuyMarketOrdersWithoutAKnownPrice()
        {
            using var brokerage = CreateBrokerage();
            var order = new MarketOrder(_btcusd, 0.25m, _orderTime);

            var exception = Assert.Throws<TargetInvocationException>(() => ConvertOrder(brokerage, order));
            Assert.IsInstanceOf<InvalidOperationException>(exception.InnerException);
            StringAssert.Contains("crypto buy market order", exception.InnerException.Message);
        }

        [Test]
        public void ReadsBackFractionalOpenOrderQuantities()
        {
            using var brokerage = CreateBrokerage();
            var contract = CreateContract(brokerage, _btcusd);
            var ibOrder = new IBApi.Order
            {
                OrderId = 1,
                Action = IB.ActionSide.Buy,
                TotalQuantity = 0.00001234m,
                OrderType = IB.OrderType.Limit,
                LmtPrice = 3370,
                Tif = IB.TimeInForce.GoodTillCancel
            };

            var orders = ConvertOrders(brokerage, ibOrder, contract);

            Assert.AreEqual(1, orders.Count);
            Assert.AreEqual(0.00001234m, orders[0].Quantity);
            Assert.AreEqual(_btcusd, orders[0].Symbol);
        }

        /// <summary>
        /// Seeds the collaborators the conversions need, so no gateway is required
        /// </summary>
        private static InteractiveBrokersBrokerage CreateBrokerage()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateField(brokerage, "_symbolMapper", new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider));
            SetPrivateField(brokerage, "_account", "DU123456");
            // IB reports crypto tick sizes through the contract details, which we can't request offline
            SetPrivateField(brokerage, "_contractSpecificationService",
                new IB.ContractSpecificationService((contract, _, _) => new ContractDetails { Contract = contract, MinTick = 0.01 }));

            return brokerage;
        }

        private static Contract CreateContract(InteractiveBrokersBrokerage brokerage, Symbol symbol)
        {
            return (Contract)InvokePrivate(brokerage, "CreateContract", [symbol, false, null, null]);
        }

        private static Symbol MapSymbol(InteractiveBrokersBrokerage brokerage, Contract contract)
        {
            return (Symbol)InvokePrivate(brokerage, "MapSymbol", [contract]);
        }

        private static IBApi.Order ConvertOrder(InteractiveBrokersBrokerage brokerage, Order order)
        {
            var contract = CreateContract(brokerage, order.Symbol);
            return (IBApi.Order)InvokePrivate(brokerage, "ConvertOrder",
                [new List<Order> { order }, contract, 1], typeof(List<Order>), typeof(Contract), typeof(int));
        }

        private static List<Order> ConvertOrders(InteractiveBrokersBrokerage brokerage, IBApi.Order ibOrder, Contract contract)
        {
            return (List<Order>)InvokePrivate(brokerage, "ConvertOrders", [ibOrder, contract, new OrderState { Status = "Submitted" }]);
        }

        private static bool CanSubscribe(Symbol symbol)
        {
            return (bool)typeof(InteractiveBrokersBrokerage)
                .GetMethod("CanSubscribe", BindingFlags.NonPublic | BindingFlags.Static)
                .Invoke(null, [symbol]);
        }

        private static object InvokePrivate(InteractiveBrokersBrokerage brokerage, string name, object[] arguments, params Type[] parameterTypes)
        {
            var method = parameterTypes.Length > 0
                ? typeof(InteractiveBrokersBrokerage).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance, parameterTypes)
                : typeof(InteractiveBrokersBrokerage).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance);
            return method.Invoke(brokerage, arguments);
        }

        private static void SetPrivateField(InteractiveBrokersBrokerage brokerage, string name, object value)
        {
            typeof(InteractiveBrokersBrokerage)
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(brokerage, value);
        }
    }
}
