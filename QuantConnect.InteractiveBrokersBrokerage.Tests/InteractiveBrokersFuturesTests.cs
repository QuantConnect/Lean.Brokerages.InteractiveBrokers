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
using System.Linq;
using IBApi;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Logging;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Ignore("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersFuturesTests
    {
        [Test]
        public void CreatesExpectedFuturesContracts()
        {
            var symbolMapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);

            using (var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider()))
            {
                ib.Connect();
                Assert.IsTrue(ib.IsConnected);

                var ibMarkets = new Dictionary<string, string>
                {
                    { Market.CME, "GLOBEX" },
                    { Market.NYMEX, "NYMEX" },
                    { Market.COMEX, "NYMEX" },
                    { Market.CBOT, "ECBOT" },
                    { Market.ICE, "NYBOT" },
                    { Market.CFE, "CFE" },
                    { Market.NYSELIFFE, "NYSELIFFE" }
                    { Market.INDIA, "NSE" }
                };

                var tickersByMarket = new Dictionary<string, string[]>
                {
                    {
                        Market.CFE,
                        new[]
                        {
                            "VX"
                        }
                    },
                    {
                        Market.CBOT,
                        new[]
                        {
                            "AW",
                            //"BCF",
                            //"BWF",
                            "EH",
                            "F1U",
                            "KE",
                            "TN",
                            "UB",
                            "YM",
                            "ZB",
                            "ZC",
                            "ZF",
                            "ZL",
                            "ZM",
                            "ZN",
                            "ZO",
                            "ZS",
                            "ZT",
                            "ZW",
                            "2YY",
                            "5YY",
                            "10Y",
                            "30Y"
                        }
                    },
                    {
                        Market.CME,
                        new[]
                        {
                            "6A",
                            "M6A",
                            "6B",
                            "M6B",
                            "6C",
                            "M6C",
                            "6E",
                            "M6E",
                            "6J",
                            "M6J",
                            "6L",
                            "6M",
                            "6N",
                            "6R",
                            "6S",
                            "M6S",
                            "6Z",
                            //"ACD",
                            //"AJY",
                            //"ANE",
                            "BIO",
                            "BTC",
                            "MicroBTC",
                            "CB",
                            //"CJY",
                            //"CNH",
                            "CSC",
                            //"DC",
                            "DY",
                            "E7",
                            //"EAD",
                            //"ECD",
                            //"EI",
                            "EMD",
                            "ES",
                            "MES",
                            //"ESK",
                            "GD",
                            "GDK",
                            "GE",
                            "GF",
                            //"GNF",
                            "HE",
                            //"IBV",
                            "J7",
                            //"LBS",
                            "LE",
                            "NKD",
                            "NQ",
                            "MicroNQ",
                            "RTY",
                            "M2K",
                            "IBVQ5"
                        }
                    },
                    {
                        Market.COMEX,
                        new[]
                        {
                            //"AUP",
                            //"EDP",
                            "GC",
                            "MGC",
                            "HG",
                            "SI",
                            "SIL"
                        }
                    },
                    {
                        Market.ICE,
                        new[]
                        {
                            "B",
                            "CC",
                            "CT",
                            "DX",
                            "G",
                            "KC",
                            "OJ",
                            "SB",
                        }
                    },
                    {
                        Market.NYMEX,
                        new[]
                        {
                            //"1S",
                            //"22",
                            //"A0D",
                            //"A0F",
                            //"A1L",
                            //"A1M",
                            //"A1R",
                            //"A32",
                            //"A3G",
                            //"A7E",
                            //"A7I",
                            //"A7Q",
                            //"A8J",
                            //"A8K",
                            //"A8O",
                            //"A91",
                            //"A9N",
                            //"AA6",
                            //"AA8",
                            //"ABS",
                            "ABT",
                            //"AC0",
                            //"AD0",
                            //"ADB",
                            //"AE5",
                            //"AGA",
                            //"AJL",
                            //"AJS",
                            //"AKL",
                            //"AKZ",
                            //"APS",
                            //"AR0",
                            "ARE",
                            //"AVZ",
                            //"AYV",
                            //"AYX",
                            //"AZ1",
                            //"B0",
                            //"B7H",
                            "BK",
                            //"BOO",
                            //"BR7",
                            "BZ",
                            "CL",
                            //"CRB",
                            //"CSW",
                            "CSX",
                            //"CU",
                            //"D1N",
                            //"DCB",
                            //"E6",
                            //"EN",
                            //"EPN",
                            //"EVC",
                            "EWG",
                            //"EWN",
                            "EXR",
                            //"FO",
                            "FRC",
                            //"FSS",
                            //"GCU",
                            //"HCL",
                            "HH",
                            "HO",
                            "HP",
                            "HRC",
                            //"HTT",
                            "NG",
                            "PA",
                            "PL",
                            "RB",
                            //"YO",
                            "M1B",
                            "M35",
                            "M5F",
                            "MAF",
                            "MCL",
                            "MEF",
                            "PAM",
                            "R5O",
                            "S5O"
                        }
                    },
                    {
                        Market.NYSELIFFE,
                        new[]
                        {
                            "MXEF",
                            "MXEA",
                            "M1EU",
                            "M1JP",
                            "M1MSA",
                            "MXUS",
                            "YG",
                            "YI",
                            "ZG",
                            "ZI"
                        }
                    },
                      {
                        Market.INDIA,
                        new[]
                        {
                            "NIFTY",
                        }
                    }
                };

                foreach (var kvp in tickersByMarket)
                {
                    var market = kvp.Key;
                    var tickers = kvp.Value;

                    foreach (var ticker in tickers)
                    {
                        var contract = new Contract
                        {
                            Symbol = symbolMapper.GetBrokerageRootSymbol(ticker, SecurityType.Future),
                            Currency = Currencies.USD,
                            Exchange = null,
                            SecType = "FUT"
                        };

                        Log.Trace($"Market: {market} - Future Ticker: {ticker}");
                        var results = ib.FindContracts(contract, contract.Symbol);
                        foreach (var contractDetails in results.Where(x => ibMarkets.Values.Contains(x.Contract.Exchange)))
                        {
                            var message = $"  - ContractDetails: {contractDetails.Contract} {contractDetails.ContractMonth}";
                            Log.Trace(message);

                            Assert.AreEqual(ibMarkets[market], contractDetails.Contract.Exchange, message);
                        }
                    }
                }
            }
        }

        [Test]
        public void CreateExpectedFutureContractsWithDifferentCurrencies()
        {
            using (var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider()))
            {
                ib.Connect();
                Assert.IsTrue(ib.IsConnected);

                var tickersByMarket = new Dictionary<string, string[]>
                {
                    {
                        Market.CME,
                        new[]
                        {
                            "AJY",
                            "ANE",
                            "IBV"
                        }

                    },
                    {
                        Market.CBOT,
                        new[]
                        {
                            "ZC"
                        }
                    },
                    {
                        Market.EUREX,
                        new[]
                        {
                            "FESX",
                            "FDAX",
                            "FDIV",
                            "FTDX"
                        }
                    },
                    {
                        Market.INDIA,
                        new[]
                        {
                            "NIFTY",
                            "BANKNIFTY",
                            "MIDCPNIFTY",
                            "FINNIFTY"
                        }
                    },
                };

                Assert.Multiple(() =>
                {
                    foreach (var kvp in tickersByMarket)
                    {
                        var market = kvp.Key;
                        var tickers = kvp.Value;

                        foreach (var ticker in tickers)
                        {
                            Log.Trace($"Market: {market} - Future Ticker: {ticker}");

                            var currentSymbol = Symbol.Create(ticker, SecurityType.Future, market);
                            var symbolsFound = ib.LookupSymbols(currentSymbol, false);
                            Assert.IsNotEmpty(symbolsFound, $"No contracts found for Market: {market} - Future Ticker: {ticker}");

                            foreach (var symbol in symbolsFound)
                            {
                                Log.Trace($"  - Symbol found in IB: {symbol} :: {symbol.Value} :: {symbol.ID.Date}");
                            }
                        }
                    }
                });
            }
        }
    }
}
