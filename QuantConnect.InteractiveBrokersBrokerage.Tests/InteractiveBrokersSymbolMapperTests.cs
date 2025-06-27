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
using IBApi;
using NUnit.Framework;
using QuantConnect.Securities;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersSymbolMapperTests
    {
        [TestCase("SPX")]
        [TestCase("SPXW")]
        public void IndexOptionBrokerageSymbol(string option)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var indexOption = Symbol.CreateOption(Symbols.SPX, option, Market.USA, OptionStyle.European, OptionRight.Call, 3800, new DateTime(2023, 1, 12));
            var brokerageSymbol = mapper.GetBrokerageSymbol(indexOption);

            Assert.AreEqual("SPX", brokerageSymbol);
        }

        [Test]
        public void ReturnsCorrectLeanSymbol()
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var symbol = mapper.GetLeanSymbol("EURUSD", SecurityType.Forex, Market.FXCM);
            Assert.AreEqual("EURUSD", symbol.Value);
            Assert.AreEqual(SecurityType.Forex, symbol.ID.SecurityType);
            Assert.AreEqual(Market.FXCM, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("AAPL", SecurityType.Equity, Market.USA);
            Assert.AreEqual("AAPL", symbol.Value);
            Assert.AreEqual(SecurityType.Equity, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("BRK B", SecurityType.Equity, Market.USA);
            Assert.AreEqual("BRK.B", symbol.Value);
            Assert.AreEqual(SecurityType.Equity, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            var expiry = new DateTime(2023, 10, 20);
            symbol = mapper.GetLeanSymbol("BRK B", SecurityType.Option, Market.USA, expiry, 362.5m, OptionRight.Put);
            Assert.AreEqual("BRK.B 231020P00362500", symbol.Value);
            Assert.AreEqual(SecurityType.Option, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);
            Assert.AreEqual(362.5m, symbol.ID.StrikePrice);
            Assert.AreEqual(expiry, symbol.ID.Date);
            Assert.AreEqual(OptionRight.Put, symbol.ID.OptionRight);
        }

        [Test]
        public void ReturnsCorrectBrokerageSymbol()
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var symbol = Symbol.Create("EURUSD", SecurityType.Forex, Market.FXCM);
            var brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("EURUSD", brokerageSymbol);

            symbol = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("AAPL", brokerageSymbol);

            symbol = Symbol.Create("BRK.B", SecurityType.Equity, Market.USA);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("BRK B", brokerageSymbol);

            symbol = Symbol.CreateCanonicalOption(symbol);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("BRK B", brokerageSymbol);
        }

        [TestCase("AAPL", "AAPL")]
        [TestCase("VXXB", "VXX")]
        [TestCase("NB", "BAC")]
        public void MapCorrectBrokerageSymbol(string ticker, string ibSymbol)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);
            var brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual(ibSymbol, brokerageSymbol);
        }

        [Test]
        public void ThrowsOnNullOrEmptyOrInvalidSymbol()
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            Assert.Throws<ArgumentException>(() => mapper.GetLeanSymbol(null, SecurityType.Forex, Market.FXCM));

            Assert.Throws<ArgumentException>(() => mapper.GetLeanSymbol("", SecurityType.Forex, Market.FXCM));

            var symbol = Symbol.Empty;
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));

            symbol = null;
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));

            symbol = Symbol.Create("", SecurityType.Forex, Market.FXCM);
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));

            symbol = Symbol.Create("ABC_XYZ", SecurityType.Forex, Market.FXCM);
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));
        }

        [TestCase("SPY JUN2021 345 P [SPY 210618P00345000 100]")]
        [TestCase("SPY    JUN2021 345 P [SPY   210618P00345000 100]")]
        [TestCase("SPY     JUN2021    345   P   [SPY         210618P00345000       100]")]
        public void MalformedContractSymbolCreatesOptionContract(string symbol)
        {
            var malformedContract = new Contract
            {
                IncludeExpired = false,
                Currency = "USD",
                Multiplier = "100",
                Symbol = symbol,
                SecType = IB.SecurityType.Option,
            };

            var expectedContract = new Contract
            {
                Symbol = "SPY",
                Multiplier = "100",
                LastTradeDateOrContractMonth = "20210618",
                Right = IB.RightType.Put,
                Strike = 345.0,
                Exchange = "Smart",
                SecType = IB.SecurityType.Option,
                IncludeExpired = false,
                Currency = "USD"
            };

            var actualContract = InteractiveBrokersSymbolMapper.ParseMalformedContractOptionSymbol(malformedContract);

            Assert.AreEqual(expectedContract.Symbol, actualContract.Symbol);
            Assert.AreEqual(expectedContract.Multiplier, actualContract.Multiplier);
            Assert.AreEqual(expectedContract.LastTradeDateOrContractMonth, actualContract.LastTradeDateOrContractMonth);
            Assert.AreEqual(expectedContract.Right, actualContract.Right);
            Assert.AreEqual(expectedContract.Strike, actualContract.Strike);
            Assert.AreEqual(expectedContract.Exchange, actualContract.Exchange);
            Assert.AreEqual(expectedContract.SecType, actualContract.SecType);
            Assert.AreEqual(expectedContract.IncludeExpired, actualContract.IncludeExpired);
            Assert.AreEqual(expectedContract.Currency, actualContract.Currency);
        }

        [TestCase("ES       MAR2021")]
        [TestCase("ES MAR2021")]
        public void MalformedContractSymbolCreatesFutureContract(string symbol)
        {
            var malformedContract = new Contract
            {
                IncludeExpired = false,
                Currency = "USD",
                Symbol = symbol,
                SecType = IB.SecurityType.Future
            };

            var expectedContract = new Contract
            {
                Symbol = "ES",
                LastTradeDateOrContractMonth = "20210319",
                SecType = IB.SecurityType.Future,
                IncludeExpired = false,
                Currency = "USD"
            };

            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var actualContract = mapper.ParseMalformedContractFutureSymbol(malformedContract, SymbolPropertiesDatabase.FromDataFolder());

            Assert.AreEqual(expectedContract.Symbol, actualContract.Symbol);
            Assert.AreEqual(expectedContract.Multiplier, actualContract.Multiplier);
            Assert.AreEqual(expectedContract.LastTradeDateOrContractMonth, actualContract.LastTradeDateOrContractMonth);
            Assert.AreEqual(expectedContract.Right, actualContract.Right);
            Assert.AreEqual(expectedContract.Strike, actualContract.Strike);
            Assert.AreEqual(expectedContract.Exchange, actualContract.Exchange);
            Assert.AreEqual(expectedContract.SecType, actualContract.SecType);
            Assert.AreEqual(expectedContract.IncludeExpired, actualContract.IncludeExpired);
            Assert.AreEqual(expectedContract.Currency, actualContract.Currency);
        }

        [TestCase(2021, 2, 23)]
        [TestCase(2021, 3, 25)]
        public void FuturesOptionsWithUnderlyingContractMonthMappedByRuleResolvesUnderlyingGetLeanSymbol(int year, int month, int day)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var expectedUnderlyingSymbol = Symbol.CreateFuture("GC", Market.COMEX, new DateTime(2021, 4, 28));
            var futureOption = mapper.GetLeanSymbol("OG", SecurityType.FutureOption, Market.COMEX, new DateTime(year, month, day));

            Assert.AreEqual(expectedUnderlyingSymbol, futureOption.Underlying);
        }

        private static TestCaseData[] LeanIbSymbolMappingTestCases = new TestCaseData[]
        {
            new("6B", "GBP", SecurityType.Future),
            new("AW", "AIGCI", SecurityType.Future),
            new("FESX", "ESTX50", SecurityType.Future),
            new("FDAX", "DAX", SecurityType.Future),
            new("SX5E", "ESTX50", SecurityType.Index),
        };

        [TestCaseSource(nameof(LeanIbSymbolMappingTestCases))]
        public void MapsLeanToIBSymbolDependingOnSecurityType(string leanTicker, string ibTicker, SecurityType securityType)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var leanSymbol = Symbol.Create(leanTicker, securityType, Market.InteractiveBrokers);
            var resultIbTicker = mapper.GetBrokerageSymbol(leanSymbol);

            Assert.AreEqual(ibTicker, resultIbTicker);
        }

        [TestCaseSource(nameof(LeanIbSymbolMappingTestCases))]
        public void MapsIBToLeanSymbolDependingOnSecurityType(string leanTicker, string ibTicker, SecurityType securityType)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            var expiry = new DateTime(2024, 09, 20);
            var expectedLeanSymbol = securityType == SecurityType.Future
                ? Symbol.CreateFuture(leanTicker, Market.InteractiveBrokers, expiry)
                : Symbol.Create(leanTicker, securityType, Market.InteractiveBrokers);
            var resultLeanSymbol = mapper.GetLeanSymbol(ibTicker, securityType, expectedLeanSymbol.ID.Market, expiry);

            Assert.AreEqual(expectedLeanSymbol, resultLeanSymbol);
        }

        [TestCase(SecurityType.Equity)]
        [TestCase(SecurityType.Option)]
        public void KeepsEquityDotsFromBrokerageSymbol(SecurityType securityType)
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var brokerageSymbol = "AGLE.CNT";

            var leanRootSymbol = mapper.GetLeanRootSymbol(brokerageSymbol, securityType);
            Assert.AreEqual(brokerageSymbol, leanRootSymbol);

            var leanSymbol = securityType == SecurityType.Equity
                ? mapper.GetLeanSymbol(brokerageSymbol, securityType, Market.USA)
                : mapper.GetLeanSymbol(brokerageSymbol, securityType, Market.USA, new DateTime(2024, 09, 20), 10.0m, OptionRight.Call);
            var expectedLeanSymbol = securityType == SecurityType.Equity
                ? Symbol.Create(brokerageSymbol, securityType, Market.USA)
                : Symbol.CreateOption(brokerageSymbol, Market.USA, OptionStyle.American, OptionRight.Call, 10.0m, new DateTime(2024, 09, 20));

            Assert.AreEqual(expectedLeanSymbol, leanSymbol);
            if (securityType == SecurityType.Equity)
            {
                Assert.AreEqual(brokerageSymbol, leanSymbol.Value);
            }
            else
            {
                Assert.AreEqual(brokerageSymbol, leanSymbol.Value.Split(' ')[0]);
                Assert.AreEqual(brokerageSymbol, leanSymbol.Underlying.Value);
            }

            var mappedBrokerageSymbol = mapper.GetBrokerageSymbol(leanSymbol);
            Assert.AreEqual(brokerageSymbol, mappedBrokerageSymbol);

            var mappedBrokerageRootSymbol = mapper.GetBrokerageRootSymbol(leanRootSymbol, securityType);
            Assert.AreEqual(brokerageSymbol, mappedBrokerageRootSymbol);
        }
    }
}
