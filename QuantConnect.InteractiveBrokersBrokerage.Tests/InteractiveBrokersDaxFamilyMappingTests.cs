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
using System.Runtime.CompilerServices;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Interactive Brokers lists the three Eurex DAX futures under one symbol, "DAX", and tells them
    /// apart by trading class and multiplier: FDAX (25), FDXM (5) and FDXS (1). LEAN keeps one root per
    /// product (symbol-properties-database: eurex FDAX / FDXM / FDXS). These tests exercise the two
    /// mapping directions without an IB Gateway, on an uninitialized brokerage instance.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersDaxFamilyMappingTests
    {
        private static readonly DateTime Expiry = new(2026, 12, 18);

        private static InteractiveBrokersBrokerage CreateOfflineBrokerage()
        {
            TestSetup.ReloadConfiguration();
            var brokerage = (InteractiveBrokersBrokerage)RuntimeHelpers.GetUninitializedObject(typeof(InteractiveBrokersBrokerage));
            var mapperField = typeof(InteractiveBrokersBrokerage).GetField("_symbolMapper", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(mapperField, "_symbolMapper field not found");
            mapperField.SetValue(brokerage, new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider));
            return brokerage;
        }

        private static MethodInfo GetPrivateMethod(string name)
        {
            var method = typeof(InteractiveBrokersBrokerage).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(method, $"{name} not found");
            return method;
        }

        [TestCase("DAX", "FDAX", "FDAX")]
        [TestCase("DAX", "FDXM", "FDXM")]
        [TestCase("DAX", "FDXS", "FDXS")]
        [TestCase("DAX", "", "FDAX")]
        [TestCase("DAX", null, "FDAX")]
        [TestCase("ESTX50", "FESX", "FESX")]
        [TestCase("ES", "ES", "ES")]
        [TestCase("GBP", "GBP", "6B")]
        [TestCase("ES", "FDXS", "ES")]
        public void MapperResolvesTheLeanRootFromTheTradingClass(string ibSymbol, string tradingClass, string expectedLeanRoot)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            Assert.AreEqual(expectedLeanRoot, mapper.GetLeanRootSymbol(ibSymbol, SecurityType.Future, tradingClass));
        }

        [TestCase("FDXM", true, "DAX")]
        [TestCase("FDXS", true, "DAX")]
        [TestCase("FDAX", false, null)]
        [TestCase("ES", false, null)]
        public void MapperKnowsTheFuturesIdentifiedByTradingClass(string leanRoot, bool expected, string expectedIbSymbol)
        {
            Assert.AreEqual(expected, InteractiveBrokersSymbolMapper.TryGetFutureTradingClassContract(leanRoot, out var ibSymbol, out var tradingClass));
            Assert.AreEqual(expectedIbSymbol, ibSymbol);
            Assert.AreEqual(expected ? leanRoot : null, tradingClass);
            // the shared root mapper is unchanged: the FIX brokerage queries the IB futures list by this root
            Assert.AreEqual(leanRoot == "FDAX" ? "DAX" : leanRoot, new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider).GetBrokerageRootSymbol(leanRoot, SecurityType.Future));
        }

        [TestCase("FDAX", "25")]
        [TestCase("FDXM", "5")]
        [TestCase("FDXS", "1")]
        public void IbDaxContractMapsToTheLeanRootOfItsTradingClass(string tradingClass, string multiplier)
        {
            var brokerage = CreateOfflineBrokerage();
            var contract = new Contract
            {
                Symbol = "DAX",
                SecType = "FUT",
                Exchange = "EUREX",
                Currency = "EUR",
                TradingClass = tradingClass,
                Multiplier = multiplier,
                LastTradeDateOrContractMonth = Expiry.ToString("yyyyMMdd"),
                LocalSymbol = $"{tradingClass} DEC 26"
            };

            var symbol = (Symbol)GetPrivateMethod("MapSymbol").Invoke(brokerage, new object[] { contract });

            Assert.AreEqual(Market.EUREX, symbol.ID.Market);
            Assert.AreEqual(Expiry, symbol.ID.Date);
            Assert.AreEqual(tradingClass, symbol.ID.Symbol, $"IB DAX contract with trading class {tradingClass} mapped to LEAN root {symbol.ID.Symbol}");
        }

        [TestCase("FDAX", "25", null)]
        [TestCase("FDXM", "5", "FDXM")]
        [TestCase("FDXS", "1", "FDXS")]
        public void LeanDaxFamilyFutureCreatesTheIbDaxContractWithItsTradingClass(string leanRoot, string multiplier, string tradingClass)
        {
            var brokerage = CreateOfflineBrokerage();
            var symbol = Symbol.CreateFuture(leanRoot, Market.EUREX, Expiry);

            var contract = (Contract)GetPrivateMethod("CreateContract").Invoke(brokerage, new object[] { symbol, false, null, null });

            Assert.AreEqual("EUREX", contract.Exchange);
            Assert.AreEqual("EUR", contract.Currency);
            Assert.AreEqual(multiplier, contract.Multiplier);
            Assert.AreEqual("DAX", contract.Symbol, $"LEAN {leanRoot} was sent to IB as symbol {contract.Symbol}");
            // FDAX keeps resolving by symbol and multiplier as before; the products sharing its symbol carry their trading class
            Assert.AreEqual(tradingClass, contract.TradingClass, $"LEAN {leanRoot} was sent to IB with trading class '{contract.TradingClass}'");
        }
    }
}
