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
using QuantConnect.Logging;
using QuantConnect.Data.Market;
using System.Collections.Generic;

namespace QuantConnect.ToolBox.IBDownloader
{
    public static class IBDownloaderProgram
    {
        /// <summary>
        /// Primary entry point to the program. This program only supports FOREX for now.
        /// </summary>
        public static void IBDownloader(IList<string> tickers, string resolution, DateTime fromDate, DateTime toDate)
        {
            if (resolution.IsNullOrEmpty() || tickers.IsNullOrEmpty())
            {
                Console.WriteLine("IBDownloader ERROR: '--tickers=' or '--resolution=' parameter is missing");
                Console.WriteLine("--tickers=eg EURUSD,USDJPY");
                Console.WriteLine("--resolution=Second/Minute/Hour/Daily/All");
                Environment.Exit(1);
            }
            try
            {
                var allResolutions = resolution.ToLowerInvariant() == "all";
                var castResolution = allResolutions ? Resolution.Second : (Resolution)Enum.Parse(typeof(Resolution), resolution);
                var startDate = fromDate.ConvertToUtc(TimeZones.NewYork);
                var endDate = toDate.ConvertToUtc(TimeZones.NewYork);

                // fix end date
                endDate = new DateTime(Math.Min(endDate.Ticks, DateTime.Now.AddDays(-1).Ticks));

                // Max number of histoy days
                int maxDays = 1;
                if (!allResolutions)
                {
                    switch (castResolution)
                    {
                        case Resolution.Daily:
                            maxDays = 365;
                            break;
                        case Resolution.Hour:
                            maxDays = 30;
                            break;
                        case Resolution.Minute:
                            maxDays = 10;
                            break;
                    }
                }

                // Load settings from config.json
                var dataDirectory = Globals.DataFolder;

                // Only FOREX for now
                SecurityType securityType = SecurityType.Forex;
                string market = Market.FXCM;


                using (var downloader = new IBDataDownloader())
                {
                    foreach (var ticker in tickers)
                    {
                        // Download the data
                        var symbol = Symbol.Create(ticker, securityType, market);

                        var auxEndDate = startDate.AddDays(maxDays);
                        auxEndDate = new DateTime(Math.Min(auxEndDate.Ticks, endDate.Ticks));

                        while (startDate < auxEndDate)
                        {
                            var data = downloader.Get(new DataDownloaderGetParameters(symbol, castResolution, startDate, auxEndDate, TickType.Quote));
                            if (data == null)
                            {
                                break;
                            }
                            var bars = data.Cast<QuoteBar>().ToList();

                            if (allResolutions)
                            {
                                // Save the data (second resolution)
                                var writer = new LeanDataWriter(castResolution, symbol, dataDirectory);
                                writer.Write(bars);

                                // Save the data (other resolutions)
                                foreach (var res in new[] { Resolution.Minute, Resolution.Hour, Resolution.Daily })
                                {
                                    var resData = LeanData.AggregateQuoteBars(bars, symbol, res.ToTimeSpan());

                                    writer = new LeanDataWriter(res, symbol, dataDirectory);
                                    writer.Write(resData);
                                }
                            }
                            else
                            {
                                // Save the data (single resolution)
                                var writer = new LeanDataWriter(castResolution, symbol, dataDirectory);
                                writer.Write(data);
                            }

                            startDate  = auxEndDate;
                            auxEndDate = auxEndDate.AddDays(maxDays);
                            auxEndDate = new DateTime(Math.Min(auxEndDate.Ticks, endDate.Ticks));
                        }
                    }

                }

            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }
    }
}
