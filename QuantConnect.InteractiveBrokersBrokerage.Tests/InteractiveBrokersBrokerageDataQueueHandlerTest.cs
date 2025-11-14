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
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture, Explicit("These tests require the IBGateway to be installed.")]
    public class InteractiveBrokersBrokerageDataQueueHandlerTest
    {
        [Test]
        public void FutureSubscriptions()
        {
            using (var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider()))
            {
                ib.Connect();
                var gotEsData = false;
                var gotHsiData = false;

                var cancelationToken = new CancellationTokenSource();

                var es = Symbols.CreateFuturesCanonicalSymbol("ES");
                var firstEs = ib.LookupSymbols(es, includeExpired: false).First();
                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(firstEs, Resolution.Second), (s, e) => { gotEsData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                // non USD quote currency, HDK
                var hsi = Symbols.CreateFuturesCanonicalSymbol("HSI");
                var firstHsi = ib.LookupSymbols(hsi, includeExpired: false).First();
                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(firstHsi, Resolution.Second), (s, e) => { gotHsiData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                Thread.Sleep(2000);
                cancelationToken.Cancel();
                cancelationToken.Dispose();

                Assert.IsTrue(gotEsData);
                Assert.IsTrue(gotHsiData);
            }
        }

        [Test]
        public void GetsTickData()
        {
            using (var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider()))
            {
                ib.Connect();
                var gotUsdData = false;
                var gotEurData = false;

                var cancelationToken = new CancellationTokenSource();

                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(Symbols.AAPL, Resolution.Second), (s, e) => { gotUsdData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(Symbols.SPY, Resolution.Second), (s, e) => { gotEurData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                Thread.Sleep(2000);
                cancelationToken.Cancel();
                cancelationToken.Dispose();

                Assert.IsTrue(gotUsdData);
                Assert.IsTrue(gotEurData);
            }
        }

        [Test]
        public void GetsTickDataAfterDisconnectionConnectionCycle()
        {
            using (var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider()))
            {
                ib.Connect();
                var cancelationToken = new CancellationTokenSource();
                var gotUsdData = false;
                var gotEurData = false;

                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(Symbols.AAPL, Resolution.Second), (s, e) => { gotUsdData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                ProcessFeed(
                    ib.Subscribe(GetSubscriptionDataConfig<TradeBar>(Symbols.SPY, Resolution.Second), (s, e) => { gotEurData = true; }),
                    cancelationToken,
                    (tick) => Log(tick));

                Thread.Sleep(2000);

                Assert.IsTrue(gotUsdData);
                Assert.IsTrue(gotEurData);

                ib.Disconnect();
                gotUsdData = false;
                gotEurData = false;

                Thread.Sleep(2000);

                ib.Connect();
                Thread.Sleep(2000);

                cancelationToken.Cancel();
                cancelationToken.Dispose();

                Assert.IsTrue(gotUsdData);
                Assert.IsTrue(gotEurData);
            }
        }

        private static TestCaseData[] GetCFDSubscriptionTestCases()
        {
            var baseTestCases = new[]
            {
                new { TickType = TickType.Trade, Resolution = Resolution.Tick },
                new { TickType = TickType.Quote, Resolution = Resolution.Tick },
                new { TickType = TickType.Quote, Resolution = Resolution.Second }
            };

            var equityCfds = new[] { "AAPL", "SPY", "GOOG" };
            var indexCfds = new[] { "IBUS500", "IBAU200", "IBUS30", "IBUST100", "IBGB100", "IBEU50", "IBFR40", "IBHK50", "IBJP225" };
            var forexCfds = new[] { "AUDUSD", "NZDUSD", "USDCAD", "USDCHF" };
            var metalCfds = new[] { "XAUUSD", "XAGUSD" };

            return baseTestCases.SelectMany(testCase => new[]
            {
                new TestCaseData(equityCfds, testCase.TickType, testCase.Resolution),
                new TestCaseData(indexCfds, testCase.TickType, testCase.Resolution),
                new TestCaseData(forexCfds, testCase.TickType, testCase.Resolution),
                new TestCaseData(metalCfds, testCase.TickType, testCase.Resolution),
            }).ToArray();
        }

        [TestCaseSource(nameof(GetCFDSubscriptionTestCases))]
        public void CanSubscribeToCFD(IEnumerable<string> tickers, TickType tickType, Resolution resolution)
        {
            // Wait a bit to make sure previous tests already disconnected from IB
            Thread.Sleep(2000);

            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var cancelationToken = new CancellationTokenSource();

            var symbolsWithData = new HashSet<Symbol>();
            var locker = new object();

            foreach (var ticker in tickers)
            {
                var symbol = Symbol.Create(ticker, SecurityType.Cfd, Market.InteractiveBrokers);
                var config = resolution switch
                {
                    Resolution.Tick => GetSubscriptionDataConfig<Tick>(symbol, resolution),
                    _ => tickType == TickType.Trade
                        ? GetSubscriptionDataConfig<TradeBar>(symbol, resolution)
                        : GetSubscriptionDataConfig<QuoteBar>(symbol, resolution)
                };

                ProcessFeed(
                    ib.Subscribe(config, (s, e) =>
                    {
                        lock (locker)
                        {
                            symbolsWithData.Add(((NewDataAvailableEventArgs)e).DataPoint.Symbol);
                        }
                    }),
                    cancelationToken,
                    (tick) => Log(tick));
            }

            Thread.Sleep(10 * 1000);
            cancelationToken.Cancel();
            cancelationToken.Dispose();

            Assert.IsTrue(tickers.Any(x => symbolsWithData.Any(symbol => symbol.Value == x)));
        }

        private static TestCaseData[] GetCFDAndUnderlyingSubscriptionTestCases()
        {
            var baseTestCases = new[]
            {
                new { TickType = TickType.Trade, Resolution = Resolution.Tick },
                new { TickType = TickType.Quote, Resolution = Resolution.Tick },
                new { TickType = TickType.Quote, Resolution = Resolution.Second }
            };

            var equityCfd = "AAPL";
            var forexCfd = "AUDUSD";

            return baseTestCases.SelectMany(testCase => new[]
            {
                new TestCaseData(equityCfd, SecurityType.Equity, Market.USA, testCase.TickType, testCase.Resolution, true),
                new TestCaseData(equityCfd, SecurityType.Equity, Market.USA, testCase.TickType, testCase.Resolution, false),
                new TestCaseData(forexCfd, SecurityType.Forex, Market.Oanda, testCase.TickType, testCase.Resolution, true),
                new TestCaseData(forexCfd, SecurityType.Forex, Market.Oanda, testCase.TickType, testCase.Resolution, false),
            }).ToArray();
        }

        [TestCaseSource(nameof(GetCFDAndUnderlyingSubscriptionTestCases))]
        public void CanSubscribeToCFDAndUnderlying(string ticker, SecurityType underlyingSecurityType, string underlyingMarket,
            TickType tickType, Resolution resolution, bool underlyingFirst)
        {
            // Wait a bit to make sure previous tests already disconnected from IB
            Thread.Sleep(2000);

            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var cancelationToken = new CancellationTokenSource();

            var symbolsWithData = new HashSet<Symbol>();
            var locker = new object();

            var underlyingSymbol = Symbol.Create(ticker, underlyingSecurityType, underlyingMarket);
            var cfdSymbol = Symbol.Create(ticker, SecurityType.Cfd, Market.InteractiveBrokers);

            var underlyingConfig = resolution switch
            {
                Resolution.Tick => GetSubscriptionDataConfig<Tick>(underlyingSymbol, resolution),
                _ => tickType == TickType.Trade
                    ? GetSubscriptionDataConfig<TradeBar>(underlyingSymbol, resolution)
                    : GetSubscriptionDataConfig<QuoteBar>(underlyingSymbol, resolution)
            };
            var cfdConfig = resolution switch
            {
                Resolution.Tick => GetSubscriptionDataConfig<Tick>(cfdSymbol, resolution),
                _ => tickType == TickType.Trade
                    ? GetSubscriptionDataConfig<TradeBar>(cfdSymbol, resolution)
                    : GetSubscriptionDataConfig<QuoteBar>(cfdSymbol, resolution)
            };
            var configs = underlyingFirst
                ? new[] { underlyingConfig, cfdConfig }
                : new[] { cfdConfig, underlyingConfig };

            foreach (var config in configs)
            {
                ProcessFeed(
                ib.Subscribe(config, (s, e) =>
                {
                    lock (locker)
                    {
                        symbolsWithData.Add(((NewDataAvailableEventArgs)e).DataPoint.Symbol);
                    }
                }),
                cancelationToken,
                (tick) => Log(tick));
            }

            Thread.Sleep(10 * 1000);
            cancelationToken.Cancel();
            cancelationToken.Dispose();

            Assert.IsTrue(symbolsWithData.Contains(cfdSymbol));
            Assert.IsTrue(symbolsWithData.Contains(underlyingSymbol));
        }

        [Test]
        public void CannotSubscribeToCFDWithUnsupportedMarket()
        {
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var usSpx500Cfd = Symbol.Create("IBUS500", SecurityType.Cfd, Market.FXCM);
            var config = GetSubscriptionDataConfig<QuoteBar>(usSpx500Cfd, Resolution.Second);

            var enumerator = ib.Subscribe(config, (s, e) => { });

            Assert.IsNull(enumerator);
        }

        [TestCase("FESX")]
        [TestCase("FDAX")]
        [TestCase("FDIV")]
        [TestCase("FTDX")]
        public void CanSubscribeToEurexFutures(string ticker)
        {
            // Wait a bit to make sure previous tests already disconnected from IB
            Thread.Sleep(2000);

            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var canonicalFuture = Symbol.Create(ticker, SecurityType.Future, Market.EUREX);
            var contracts = TestUtils.GetFutureContracts(canonicalFuture, 3).ToList();
            Assert.AreEqual(3, contracts.Count);

            var resolutions = new[] { Resolution.Tick, Resolution.Second };
            var configs = contracts.SelectMany(symbol => resolutions.SelectMany(resolution =>
            {
                return resolution switch
                {
                    Resolution.Tick => new[] { GetSubscriptionDataConfig<Tick>(symbol, resolution) },
                    _ => new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, resolution),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, resolution)
                    }
                };
            }));

            var cancelationToken = new CancellationTokenSource();
            var data = new List<IBaseData>();

            foreach (var config in configs)
            {
                ProcessFeed(
                    ib.Subscribe(config, (s, e) =>
                    {
                        var dataPoint = ((NewDataAvailableEventArgs)e).DataPoint;
                        lock (data)
                        {
                            data.Add(dataPoint);
                        }
                    }),
                    cancelationToken,
                    (tick) => Log(tick));
            }

            Thread.Sleep(10 * 1000);
            cancelationToken.Cancel();
            cancelationToken.Dispose();

            var symbolsWithData = data.Select(tick => tick.Symbol).Distinct().ToList();
            CollectionAssert.IsNotEmpty(symbolsWithData);
            CollectionAssert.IsSubsetOf(symbolsWithData, contracts);

            var dataTypesWithData = data.Select(tick => tick.GetType()).Distinct().ToList();
            var expectedDataTypes = configs.Select(config => config.Type).Distinct().ToList();
            CollectionAssert.IsSubsetOf(dataTypesWithData, expectedDataTypes);
        }

        [Test]
        public void CanSubscribeToEurexIndex()
        {
            // Wait a bit to make sure previous tests already disconnected from IB
            Thread.Sleep(2000);

            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            var index = Symbol.Create("SX5E", SecurityType.Index, Market.EUREX);

            var resolutions = new[] { Resolution.Tick, Resolution.Second };
            var configs = resolutions.Select(resolution => resolution == Resolution.Tick
                ? GetSubscriptionDataConfig<Tick>(index, resolution)
                : GetSubscriptionDataConfig<TradeBar>(index, resolution));

            var cancelationToken = new CancellationTokenSource();
            var data = new List<IBaseData>();

            foreach (var config in configs)
            {
                ProcessFeed(
                    ib.Subscribe(config, (s, e) =>
                    {
                        var dataPoint = ((NewDataAvailableEventArgs)e).DataPoint;
                        lock (data)
                        {
                            data.Add(dataPoint);
                        }
                    }),
                    cancelationToken,
                    (tick) => Log(tick));
            }

            Thread.Sleep(20 * 1000);
            cancelationToken.Cancel();
            cancelationToken.Dispose();

            var symbolsWithData = data.Select(tick => tick.Symbol).Distinct().ToList();
            Assert.AreEqual(1, symbolsWithData.Count);
            Assert.AreEqual(index, symbolsWithData[0]);

            var dataTypesWithData = data.Select(tick => tick.GetType()).Distinct().ToList();
            var expectedDataTypes = configs.Select(config => config.Type).Distinct().ToList();
            Assert.AreEqual(expectedDataTypes.Count, dataTypesWithData.Count);
        }

        [Test]
        public void SubscribeOnDifferentFOPWithCorrectStrikePrices()
        {
            var cts = new CancellationTokenSource();
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());

            var corn = Symbol.CreateFuture(Futures.Grains.Corn, Market.CBOT, new(2025, 12, 12));
            var le = Symbol.CreateFuture(Futures.Meats.LiveCattle, Market.CME, new(2025, 12, 31));
            var jpy = Symbol.CreateFuture(Futures.Currencies.JPY, Market.CME, new(2025, 12, 15));
            var aud = Symbol.CreateFuture(Futures.Currencies.AUD, Market.CME, new(2025, 12, 15));

            // Not found
            var nzd = Symbol.CreateFuture(Futures.Currencies.NZD, Market.CME, new(2025, 12, 15));

            var style = SecurityType.FutureOption.DefaultOptionStyle();
            var fops = new List<Symbol>(6)
            {
                Symbol.CreateOption(corn, corn.ID.Market, style, OptionRight.Call, 4.1m, new(2025, 11, 21)), //  corn: price magnifier: 100
                Symbol.CreateOption(le, le.ID.Market, style, OptionRight.Call, 2.18m, new(2025, 12, 05)), // le: price magnifier: 100
                Symbol.CreateOption(jpy, jpy.ID.Market, style, OptionRight.Call, 0.00625m, new(2026, 01, 09)), // jpy: price magnifier: undefined
                Symbol.CreateOption(jpy, jpy.ID.Market, style, OptionRight.Put, 0.0068m, new(2026, 01, 09)), // jpy: price magnifier: undefined
                Symbol.CreateOption(aud, aud.ID.Market, style, OptionRight.Call, 0.635m, new(2025, 12, 05)), // aud: price magnifier: undefined

                // Not found
                Symbol.CreateOption(nzd, nzd.ID.Market, style, OptionRight.Call, 0.54m, new(2026, 12, 05)), // nzd: price magnifier: undefined
            };

            ib.Connect();

            var securityDefinitionNotFoundCounter = 0;
            ib.Message += (_, brokerageMessageEvent) =>
            {
                switch (brokerageMessageEvent.Code)
                {
                    case "200":
                        // No security definition has been found for the request. Origin: [Id=7] GetContractDetails: 6N15Z25  261205C00000540 (FOP NZD USD CME)}
                        securityDefinitionNotFoundCounter += 1;
                        break;
                }
            };

            var configs = new List<SubscriptionDataConfig>();
            foreach (var fop in fops)
            {
                var config = GetSubscriptionDataConfig<Tick>(fop, Resolution.Tick);
                configs.Add(config);

                ib.Subscribe(config, (_, EventArgs) => { });
            }

            cts.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));

            foreach (var config in configs)
            {
                ib.Unsubscribe(config);
            }

            cts.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));

            try
            {
                Assert.AreEqual(2, securityDefinitionNotFoundCounter, "Expected 2 missing security definitions: FUT 6N15Z25 and FOP 261205C00000540.");
            }
            finally
            {
                cts.Cancel();
                cts.DisposeSafely();
            }
        }

        protected SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
        {
            var entry = MarketHoursDatabase.FromDataFolder().GetEntry(symbol.ID.Market, symbol, symbol.SecurityType);
            return new SubscriptionDataConfig(
                typeof(T),
                symbol,
                resolution,
                entry.DataTimeZone,
                entry.ExchangeHours.TimeZone,
                true,
                true,
                false);
        }

        private void ProcessFeed(IEnumerator<BaseData> enumerator, CancellationTokenSource cancellationToken, Action<BaseData> callback = null)
        {
            Task.Run(() =>
            {
                try
                {
                    while (enumerator.MoveNext() && !cancellationToken.IsCancellationRequested)
                    {
                        BaseData tick = enumerator.Current;
                        if (callback != null)
                        {
                            callback.Invoke(tick);
                        }
                    }
                }
                catch (AssertionException)
                {
                    throw;
                }
                catch (Exception err)
                {
                    QuantConnect.Logging.Log.Error(err.Message);
                }
            });
        }

        private void Log(BaseData dataPoint)
        {
            if (dataPoint != null)
            {
                QuantConnect.Logging.Log.Trace("{dataPoint}");
            }
        }
    }
}
