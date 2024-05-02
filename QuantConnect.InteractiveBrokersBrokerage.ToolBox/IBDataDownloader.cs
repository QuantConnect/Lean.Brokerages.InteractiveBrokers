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
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Brokerages.InteractiveBrokers;

namespace QuantConnect.ToolBox.IBDownloader
{
    /// <summary>
    /// IB Downloader class
    /// </summary>
    public class IBDataDownloader : IDataDownloader, IDisposable
    {
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly InteractiveBrokersBrokerage _brokerage;

        /// <summary>
        /// Initializes a new instance of the <see cref="IBDataDownloader"/> class
        /// </summary>
        public IBDataDownloader()
        {
            _brokerage = new InteractiveBrokersBrokerage(null, null, null,
                Config.Get("ib-account"),
                Config.Get("ib-host", "LOCALHOST"),
                Config.GetInt("ib-port", 4001),
                Config.Get("ib-tws-dir"),
                Config.Get("ib-version", InteractiveBrokersBrokerage.DefaultVersion),
                Config.Get("ib-user-name"),
                Config.Get("ib-password"),
                Config.Get("ib-trading-mode"),
                Config.GetValue("ib-agent-description", Brokerages.InteractiveBrokers.Client.AgentDescription.Individual),
                loadExistingHoldings: false);

            _brokerage.Message += (object _, Brokerages.BrokerageMessageEvent e) =>
            {
                if (e.Type == Brokerages.BrokerageMessageType.Error)
                {
                    Logging.Log.Error(e.Message);
                }
                else
                {
                    Logging.Log.Trace(e.Message);
                }
            };

            _brokerage.Connect();
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">model class for passing in parameters for historical data</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {
            var symbol = dataDownloaderGetParameters.Symbol;
            var resolution = dataDownloaderGetParameters.Resolution;
            var startUtc = dataDownloaderGetParameters.StartUtc;
            var endUtc = dataDownloaderGetParameters.EndUtc;
            var tickType = dataDownloaderGetParameters.TickType;

            if (tickType != TickType.Quote || resolution == Resolution.Tick)
            {
                return null;
            }

            if (endUtc < startUtc)
            {
                return Enumerable.Empty<BaseData>();
            }

            var symbols = new List<Symbol>{ symbol };
            if (symbol.IsCanonical())
            {
                symbols = GetChainSymbols(symbol, true).ToList();
            }

            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            return symbols
                .Select(symbol =>
                {
                    var request = new HistoryRequest(startUtc,
                        endUtc,
                        typeof(QuoteBar),
                        symbol,
                        resolution,
                        exchangeHours: exchangeHours,
                        dataTimeZone: dataTimeZone,
                        resolution,
                        includeExtendedMarketHours: true,
                        false,
                        DataNormalizationMode.Adjusted,
                        TickType.Quote);

                    var history = _brokerage.GetHistory(request);

                    if (history == null)
                    {
                        Logging.Log.Trace($"IBDataDownloader.Get(): Ignoring history request for unsupported symbol {symbol}");
                    }

                    return history;
                })
                .Where(history => history != null)
                .SelectMany(history => history);
        }

        /// <summary>
        /// Returns an IEnumerable of Future/Option contract symbols for the given root ticker
        /// </summary>
        /// <param name="symbol">The Symbol to get futures/options chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        public IEnumerable<Symbol> GetChainSymbols(Symbol symbol, bool includeExpired)
        {
            return _brokerage.LookupSymbols(symbol, includeExpired);
        }

        /// <summary>
        /// Downloads historical data from the brokerage and saves it in LEAN format.
        /// </summary>
        /// <param name="symbols">The list of symbols</param>
        /// <param name="tickType">The tick type</param>
        /// <param name="resolution">The resolution</param>
        /// <param name="securityType">The security type</param>
        /// <param name="startTimeUtc">The starting date/time (UTC)</param>
        /// <param name="endTimeUtc">The ending date/time (UTC)</param>
        public void DownloadAndSave(List<Symbol> symbols, Resolution resolution, SecurityType securityType, TickType tickType, DateTime startTimeUtc, DateTime endTimeUtc)
        {
            var writer = new LeanDataWriter(Globals.DataFolder, resolution, securityType, tickType);
            writer.DownloadAndSave(_brokerage, symbols, startTimeUtc, endTimeUtc);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_brokerage != null)
            {
                _brokerage.Disconnect();
                _brokerage.Dispose();
            }
        }
    }
}
