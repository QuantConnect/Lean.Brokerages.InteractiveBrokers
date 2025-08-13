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

using IBApi;
using System;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Data.Auxiliary;

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Provides functionality to determine the primary exchange for equity symbols.
    /// Uses map file data as the first source and falls back to Interactive Brokers contract details if needed.
    /// </summary>
    public class EquityPrimaryExchangeService
    {
        /// <summary>
        /// Provides primary exchange data based on LEAN map files.
        /// </summary>
        private readonly MapFilePrimaryExchangeProvider _exchangeProvider;

        /// <summary>
        /// Caches previously retrieved primary exchange values for symbols to avoid repeated lookups.
        /// </summary>
        private readonly Dictionary<Symbol, string> _primaryExchangeBySymbol = [];

        /// <summary>
        /// Delegate used to request Interactive Brokers contract details for a given contract.
        /// </summary>
        /// <param name="contract">The Interactive Brokers contract to retrieve details for.</param>
        /// <param name="ticker">The associated ticker symbol, used for logging or lookup.</param>
        /// <param name="failIfNotFound">True if an exception should be thrown when the contract is not found; false to allow null results.
        /// </param>
        /// <returns>The retrieved contract details, or null if not found and <paramref name="failIfNotFound"/> is false.</returns>
        private Func<Contract, string, bool, ContractDetails> _getContractDetails;

        /// <summary>
        /// Initializes a new instance of the <see cref="EquityPrimaryExchangeService"/> class.
        /// </summary>
        /// <param name="mapFileProvider">The provider for historical map file data.</param>
        /// <param name="getContractDetails">
        /// A delegate method used to request Interactive Brokers contract details.
        /// </param>
        public EquityPrimaryExchangeService(IMapFileProvider mapFileProvider, Func<Contract, string, bool, ContractDetails> getContractDetails)
        {
            _exchangeProvider = new MapFilePrimaryExchangeProvider(mapFileProvider);
            _getContractDetails = getContractDetails;
        }

        /// <summary>
        /// Gets the primary exchange for the specified contract and symbol.
        /// Checks LEAN map files first, and if unavailable, retrieves from Interactive Brokers.
        /// </summary>
        /// <param name="contract">The Interactive Brokers contract object.</param>
        /// <param name="symbol">The LEAN <see cref="Symbol"/> associated with the contract.</param>
        /// <param name="failIfNotFound">
        /// True if an exception should be thrown when the primary exchange cannot be determined;
        /// false to return null.
        /// </param>
        /// <returns>
        /// The primary exchange name as a string, or null if not found and
        /// <paramref name="failIfNotFound"/> is false.
        /// </returns>
        public string GetPrimaryExchange(Contract contract, Symbol symbol, bool failIfNotFound = true)
        {
            var primaryExchange = _exchangeProvider.GetPrimaryExchange(symbol.ID)?.Name;

            if (primaryExchange is null)
            {
                primaryExchange = _getContractDetails(contract, symbol.Value, failIfNotFound)?.Contract.PrimaryExch;
                if (primaryExchange is null)
                {
                    return null;
                }

                _primaryExchangeBySymbol[symbol] = primaryExchange;

                return primaryExchange;
            }

            return primaryExchange;
        }
    }
}
