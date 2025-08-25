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

using Newtonsoft.Json;
using QuantConnect.Interfaces;
using QuantConnect.Securities.Future;
using QuantConnect.Securities.FutureOption;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using IBApi;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Securities.IndexOption;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Provides the mapping between Lean symbols and InteractiveBrokers symbols.
    /// </summary>
    public class InteractiveBrokersSymbolMapper : ISymbolMapper
    {
        private readonly IMapFileProvider _mapFileProvider;

        // we have a special treatment of futures and other security types, because IB renamed several exchange tickers (like GBP instead of 6B).
        // We fix this: We map those tickers back to their original names using the map below
        private readonly Dictionary<SecurityType, Dictionary<string, string>> _ibNameMap = new Dictionary<SecurityType, Dictionary<string, string>>();

        // lean uses dots for ticker suffixes, which are mapped to spaces because IB uses spaces for suffixes.
        // There are also a few cases where IB uses dots in equity tickers for other purposes (e.g. AGLE.CNT).
        // In those cases we make sure we don't replace dots with spaces when converting from Lean to IB symbols.
        private static HashSet<string> _ibEquityTickersWithDot = new();

        /// <summary>
        /// Constructs InteractiveBrokersSymbolMapper. Default parameters are used.
        /// </summary>
        public InteractiveBrokersSymbolMapper(IMapFileProvider mapFileProvider) :
            this(Path.Combine("InteractiveBrokers", "IB-symbol-map.json"))
        {
            _mapFileProvider = mapFileProvider;
        }

        /// <summary>
        /// Constructs InteractiveBrokersSymbolMapper
        /// </summary>
        /// <param name="ibNameMap">New names map (IB -> LEAN)</param>
        public InteractiveBrokersSymbolMapper(Dictionary<SecurityType, Dictionary<string, string>> ibNameMap)
        {
            _ibNameMap = ibNameMap;
        }

        /// <summary>
        /// Constructs InteractiveBrokersSymbolMapper
        /// </summary>
        /// <param name="ibNameMapFullName">Full file name of the map file</param>
        public InteractiveBrokersSymbolMapper(string ibNameMapFullName)
        {
            if (File.Exists(ibNameMapFullName))
            {
                _ibNameMap = JsonConvert.DeserializeObject<Dictionary<SecurityType, Dictionary<string, string>>>(File.ReadAllText(ibNameMapFullName));
            }
        }
        /// <summary>
        /// Converts a Lean symbol instance to an InteractiveBrokers symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The InteractiveBrokers symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol?.Value))
            {
                throw new ArgumentException("Invalid symbol: " + (symbol == null ? "null" : symbol.ToString()));
            }

            if (symbol.ID.SecurityType != SecurityType.Forex &&
                symbol.ID.SecurityType != SecurityType.Equity &&
                symbol.ID.SecurityType != SecurityType.Index &&
                symbol.ID.SecurityType != SecurityType.Option &&
                symbol.ID.SecurityType != SecurityType.IndexOption &&
                symbol.ID.SecurityType != SecurityType.FutureOption &&
                symbol.ID.SecurityType != SecurityType.Future &&
                symbol.ID.SecurityType != SecurityType.Cfd)
            {
                throw new ArgumentException("Invalid security type: " + symbol.ID.SecurityType);
            }

            var ticker = GetMappedTicker(symbol);

            if (string.IsNullOrWhiteSpace(ticker))
            {
                throw new ArgumentException("Invalid symbol: " + symbol.ToString());
            }

            if (symbol.ID.SecurityType == SecurityType.Forex && ticker.Length != 6)
            {
                throw new ArgumentException("Forex symbol length must be equal to 6: " + symbol.Value);
            }

            return GetBrokerageRootSymbol(ticker, symbol.SecurityType);
        }

        /// <summary>
        /// Converts an InteractiveBrokers symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The InteractiveBrokers symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">Expiration date of the security(if applicable)</param>
        /// <param name="strike">The strike of the security (if applicable)</param>
        /// <param name="optionRight">The option right of the security (if applicable)</param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market, DateTime expirationDate = default(DateTime), decimal strike = 0, OptionRight optionRight = 0)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException("Invalid symbol: " + brokerageSymbol);

            if (securityType != SecurityType.Forex &&
                securityType != SecurityType.Equity &&
                securityType != SecurityType.Index &&
                securityType != SecurityType.Option &&
                securityType != SecurityType.IndexOption &&
                securityType != SecurityType.Future &&
                securityType != SecurityType.FutureOption &&
                securityType != SecurityType.Cfd)
                throw new ArgumentException("Invalid security type: " + securityType);

            try
            {
                var ticker = GetLeanRootSymbol(brokerageSymbol, securityType);
                switch (securityType)
                {
                    case SecurityType.Future:
                        return Symbol.CreateFuture(ticker, market, expirationDate);

                    case SecurityType.Option:
                        return Symbol.CreateOption(ticker, market, OptionStyle.American, optionRight, strike, expirationDate);

                    case SecurityType.IndexOption:
                        // Index Options have their expiry offset from their last trading date by one day. We add one day
                        // to get the expected expiration date.
                        return Symbol.CreateOption(
                            Symbol.Create(IndexOptionSymbol.MapToUnderlying(ticker), SecurityType.Index, market),
                            ticker,
                            market,
                            securityType.DefaultOptionStyle(),
                            optionRight,
                            strike,
                            IndexOptionSymbol.GetExpiryDate(ticker, expirationDate));

                    case SecurityType.FutureOption:
                        var future = FuturesOptionsUnderlyingMapper.GetUnderlyingFutureFromFutureOption(
                            ticker,
                            market,
                            expirationDate,
                            DateTime.Now);

                        if (future == null)
                        {
                            // This is the worst case scenario, because we didn't find a matching futures contract for the FOP.
                            // Note that this only applies to CBOT symbols for now.
                            throw new ArgumentException($"The Future Option with expected underlying of {future} with expiry: {expirationDate:yyyy-MM-dd} has no matching underlying future contract.");
                        }

                        return Symbol.CreateOption(
                            future,
                            market,
                            OptionStyle.American,
                            optionRight,
                            strike,
                            expirationDate);
                }

                return Symbol.Create(ticker, securityType, market);
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error in GetLeanSymbol");
                throw new ArgumentException($"Invalid symbol: {brokerageSymbol}, security type: {securityType}, market: {market}.");
            }
        }

        /// <summary>
        /// IB specific versions of the symbol mapping (GetBrokerageRootSymbol) for future root symbols
        /// </summary>
        /// <param name="rootSymbol">LEAN root symbol</param>
        /// <returns></returns>
        public string GetBrokerageRootSymbol(string rootSymbol, SecurityType securityType)
        {
            var brokerageSymbol = rootSymbol;
            if (TryGetSymbolMap(securityType, out var symbolMap))
            {
                brokerageSymbol = symbolMap.FirstOrDefault(kv => kv.Value == rootSymbol).Key;
            }

            return brokerageSymbol ?? rootSymbol;
        }

        /// <summary>
        /// IB specific versions of the symbol mapping (GetLeanRootSymbol) for future root symbols
        /// </summary>
        /// <param name="brokerageRootSymbol">IB Brokerage root symbol</param>
        /// <returns></returns>
        public string GetLeanRootSymbol(string brokerageRootSymbol, SecurityType securityType)
        {
            var isEquityOrOption = securityType == SecurityType.Equity || securityType == SecurityType.Option;
            if (isEquityOrOption && brokerageRootSymbol.Contains('.'))
            {
                _ibEquityTickersWithDot.Add(brokerageRootSymbol);
            }

            var ticker = TryGetSymbolMap(securityType, out var symbolMap) && symbolMap.TryGetValue(brokerageRootSymbol, out var rootSymbol)
                ? rootSymbol
                : brokerageRootSymbol;

            if (isEquityOrOption || securityType == SecurityType.Cfd)
            {
                ticker = ticker.Replace(" ", ".");
            }

            return ticker;
        }

        /// <summary>
        /// Parses a contract for future with malformed data.
        /// Malformed data usually manifests itself by having "0" assigned to some values
        /// we expect, like the contract's expiry date. The contract is returned by IB
        /// like this, usually due to a high amount of data subscriptions that are active
        /// in an account, surpassing IB's imposed limit. Read more about this here: https://interactivebrokers.github.io/tws-api/rtd_fqa_errors.html#rtd_common_errors_maxmktdata
        ///
        /// We are provided a string in the Symbol in malformed contracts that can be
        /// parsed to construct the clean contract, which is done by this method.
        /// </summary>
        /// <param name="malformedContract">Malformed contract (for futures), i.e. a contract with invalid values ("0") in some of its fields</param>
        /// <param name="symbolPropertiesDatabase">The symbol properties database to use</param>
        /// <returns>Clean Contract for the future</returns>
        /// <remarks>
        /// The malformed contract returns data similar to the following when calling <see cref="InteractiveBrokersBrokerage.GetContractDetails"/>: ES       MAR2021
        /// </remarks>
        public Contract ParseMalformedContractFutureSymbol(Contract malformedContract, SymbolPropertiesDatabase symbolPropertiesDatabase)
        {
            Log.Trace($"InteractiveBrokersSymbolMapper.ParseMalformedContractFutureSymbol(): Parsing malformed contract: {InteractiveBrokersBrokerage.GetContractDescription(malformedContract)} with trading class: \"{malformedContract.TradingClass}\"");

            // capture any character except spaces, match spaces, capture any char except digits, capture digits
            var matches = Regex.Matches(malformedContract.Symbol, @"^(\S*)\s*(\D*)(\d*)");

            var match = matches[0].Groups;
            var contractSymbol = match[1].Value;
            var contractMonthExpiration = DateTime.ParseExact(match[2].Value, "MMM", CultureInfo.CurrentCulture).Month;
            var contractYearExpiration = match[3].Value;

            var leanSymbol = GetLeanRootSymbol(contractSymbol, SecurityType.Future);
            string market;
            if (!symbolPropertiesDatabase.TryGetMarket(leanSymbol, SecurityType.Future, out market))
            {
                market = InteractiveBrokersBrokerageModel.DefaultMarketMap[SecurityType.Future];
            }
            var canonicalSymbol = Symbol.Create(leanSymbol, SecurityType.Future, market);
            var contractMonthYear = new DateTime(int.Parse(contractYearExpiration, CultureInfo.InvariantCulture), contractMonthExpiration, 1);
            // we get the expiration date using the FuturesExpiryFunctions
            var contractExpirationDate = FuturesExpiryFunctions.FuturesExpiryFunction(canonicalSymbol)(contractMonthYear);

            return new Contract
            {
                Symbol = contractSymbol,
                Multiplier = malformedContract.Multiplier,
                LastTradeDateOrContractMonth = $"{contractExpirationDate:yyyyMMdd}",
                Exchange = malformedContract.Exchange,
                SecType = malformedContract.SecType,
                IncludeExpired = false,
                Currency = malformedContract.Currency
            };
        }

        private string GetMappedTicker(Symbol symbol)
        {
            if (symbol.ID.SecurityType.IsOption())
            {
                symbol = symbol.Underlying;
            }

            var ticker = symbol.ID.Symbol;
            if (symbol.ID.SecurityType == SecurityType.Equity)
            {
                var mapFile = _mapFileProvider.Get(AuxiliaryDataKey.Create(symbol)).ResolveMapFile(symbol);
                ticker = mapFile.GetMappedSymbol(DateTime.UtcNow, symbol.Value);

                if (!_ibEquityTickersWithDot.Contains(ticker))
                {
                    ticker = ticker.Replace(".", " ");
                }
            }
            else if (symbol.SecurityType == SecurityType.Cfd)
            {
                ticker = ticker.Replace(".", " ");
            }
            return ticker;
        }

        /// <summary>
        /// Parses a contract for options with malformed data.
        /// Malformed data usually manifests itself by having "0" assigned to some values
        /// we expect, like the contract's expiry date. The contract is returned by IB
        /// like this, usually due to a high amount of data subscriptions that are active
        /// in an account, surpassing IB's imposed limit. Read more about this here: https://interactivebrokers.github.io/tws-api/rtd_fqa_errors.html#rtd_common_errors_maxmktdata
        ///
        /// We are provided a string in the Symbol in malformed contracts that can be
        /// parsed to construct the clean contract, which is done by this method.
        /// </summary>
        /// <param name="malformedContract">Malformed contract (for options), i.e. a contract with invalid values ("0") in some of its fields</param>
        /// <param name="exchange">Exchange that the contract's asset lives on/where orders will be routed through</param>
        /// <returns>Clean Contract for the option</returns>
        /// <remarks>
        /// The malformed contract returns data similar to the following when calling <see cref="InteractiveBrokersBrokerage.GetContractDetails"/>:
        /// OPT SPY JUN2021 350 P [SPY 210618P00350000 100] USD 0 0 0
        ///
        /// ... which the contents inside [] follow the pattern:
        ///
        /// [SYMBOL YY_MM_DD_OPTIONRIGHT_STRIKE(divide by 1000) MULTIPLIER]
        /// </remarks>
        public static Contract ParseMalformedContractOptionSymbol(Contract malformedContract, string exchange = "Smart")
        {
            Log.Trace($"InteractiveBrokersSymbolMapper.ParseMalformedContractOptionSymbol(): Parsing malformed contract: {InteractiveBrokersBrokerage.GetContractDescription(malformedContract)} with trading class: \"{malformedContract.TradingClass}\"");

            // we search for the '[ ]' pattern, inside of it we: (capture any character except spaces, match spaces) -> 3 times
            var matches = Regex.Matches(malformedContract.Symbol, @"^.*[\[](\S*)\s*(\S*)\s*(\S*)[\]]");

            var match = matches[0].Groups;
            var contractSymbol = match[1].Value;
            var contractSpecification = match[2].Value;
            var multiplier = match[3].Value;
            var expiryDate = "20" + contractSpecification.Substring(0, 6);
            var contractRight = contractSpecification[6] == 'C' ? IB.RightType.Call : IB.RightType.Put;
            var contractStrike = long.Parse(contractSpecification.Substring(7), CultureInfo.InvariantCulture) / 1000.0;

            return new Contract
            {
                Symbol = contractSymbol,
                Multiplier = multiplier,
                LastTradeDateOrContractMonth = expiryDate,
                Right = contractRight,
                Strike = contractStrike,
                Exchange = exchange,
                SecType = malformedContract.SecType,
                IncludeExpired = false,
                Currency = malformedContract.Currency
            };
        }

        /// <summary>
        /// Gets the symbol mapper for the given security type.
        /// If security type is an option type and no map is found for it, it tries to get the map for its underlying security type.
        /// </summary>
        private bool TryGetSymbolMap(SecurityType securityType, out Dictionary<string, string> symbolMap)
        {
            return _ibNameMap.TryGetValue(securityType, out symbolMap) ||
                (securityType.IsOption() && _ibNameMap.TryGetValue(Symbol.GetUnderlyingFromOptionType(securityType), out symbolMap));
        }
    }
}
