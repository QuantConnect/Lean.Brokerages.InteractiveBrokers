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
using System.Collections.Concurrent;

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Provides a cached lookup for an instrument's trading class and minimum tick size using Interactive Brokers <c>ContractDetails</c> data.
    /// </summary>
    /// <remarks>
    /// This service reduces the number of IB <c>reqContractDetails</c> requests by caching
    /// the <see cref="ContractSpecification"/> for all instruments in the same option chain.
    /// </remarks>
    public class ContractSpecificationService
    {
        /// <summary>
        /// Delegate used to request Interactive Brokers contract details.
        /// </summary>
        private Func<Contract, string, bool, ContractDetails> _getContractDetails;

        /// <summary>
        /// Thread-safe cache that maps canonical <see cref="Symbol"/> instances to their associated trading class.
        /// </summary>
        /// <remarks>
        /// This collection is used to reduce the number of <see cref="_client.ClientSocket.reqContractDetails(requestId, contract);"/> requests
        /// for option chains, since all options in the same chain share the same trading class.
        /// </remarks>
        internal readonly ConcurrentDictionary<Symbol, ContractSpecification> _tradingClassByCanonicalSymbol = [];

        /// <summary>
        /// Initializes a new instance of the <see cref="ContractSpecificationService"/> class.
        /// </summary>
        /// <param name="getContractDetails">
        /// Delegate function used to retrieve IB <see cref="ContractDetails"/> for a given contract.
        /// </param>
        public ContractSpecificationService(Func<Contract, string, bool, ContractDetails> getContractDetails)
        {
            _getContractDetails = getContractDetails;
        }

        /// <summary>
        /// Gets the trading class for the specified contract and symbol, using cached data if available.
        /// </summary>
        /// <param name="contract">The IB contract to retrieve data for.</param>
        /// <param name="symbol">The associated Lean symbol.</param>
        /// <param name="failIfNotFound">
        /// If <c>true</c>, the request is treated as a required lookup and will be logged as a <see cref="InteractiveBrokersBrokerage.RequestType.ContractDetails"/> request.
        /// If <c>false</c>, the request is treated as a soft lookup (<see cref="InteractiveBrokersBrokerage.RequestType.SoftContractDetails"/>) that does not cause a failure if no details are found.
        /// </param>
        /// <returns>The trading class string, or <c>null</c> if the data could not be retrieved.</returns>
        public string GetTradingClass(Contract contract, Symbol symbol, bool failIfNotFound = true) => GetOrAddSpecification(contract, symbol, failIfNotFound)?.TradingClass;

        /// <summary>
        /// Gets the minimum price increment (tick size) for the specified contract and symbol,
        /// using cached data if available.
        /// </summary>
        /// <param name="contract">The IB contract to retrieve data for.</param>
        /// <param name="symbol">The associated Lean symbol.</param>
        /// <param name="failIfNotFound">
        /// If <c>true</c>, the request is treated as a required lookup and will be logged as a <see cref="InteractiveBrokersBrokerage.RequestType.ContractDetails"/> request.
        /// If <c>false</c>, the request is treated as a soft lookup (<see cref="InteractiveBrokersBrokerage.RequestType.SoftContractDetails"/>) that does not cause a failure if no details are found.
        /// </param>
        /// <returns>
        /// The minimum tick size, or <c>0</c> if the data could not be retrieved.
        /// </returns>
        public decimal GetMinTick(Contract contract, Symbol symbol, bool failIfNotFound = true) => GetOrAddSpecification(contract, symbol, failIfNotFound)?.MinTick ?? 0m;

        /// <summary>
        /// Gets the <see cref="ContractSpecification"/> for the given contract and symbol,
        /// retrieving it from the cache if present or requesting it from IB if not.
        /// </summary>
        /// <param name="contract">The IB contract to retrieve data for.</param>
        /// <param name="symbol">The associated Lean symbol.</param>
        /// <param name="failIfNotFound">
        /// If <c>true</c>, the request is treated as a required lookup and will be logged as a <see cref="InteractiveBrokersBrokerage.RequestType.ContractDetails"/> request.
        /// If <c>false</c>, the request is treated as a soft lookup (<see cref="InteractiveBrokersBrokerage.RequestType.SoftContractDetails"/>) that does not cause a failure if no details are found.
        /// </param>
        /// <returns>
        /// The <see cref="ContractSpecification"/> containing the trading class and minimum tick size,
        /// or <c>null</c> if the data could not be retrieved.
        /// </returns>
        private ContractSpecification GetOrAddSpecification(Contract contract, Symbol symbol, bool failIfNotFound)
        {
            var canonicalSymbol = symbol.HasCanonical() ? symbol.Canonical : null;
            if (canonicalSymbol is not null && _tradingClassByCanonicalSymbol.TryGetValue(canonicalSymbol, out var cached))
            {
                return cached;
            }

            if (symbol.SecurityType is QuantConnect.SecurityType.FutureOption or QuantConnect.SecurityType.IndexOption)
            {
                // Futures options and Index Options trading class is the same as the FOP ticker.
                // This is required in order to resolve the contract details successfully.
                // We let this method complete even though we assign twice so that the
                // contract details are added to the cache and won't require another lookup.
                contract.TradingClass = symbol.ID.Symbol;
            }

            var details = _getContractDetails(contract, symbol.Value, failIfNotFound);
            if (details == null)
            {
                return null; // Unable to find contract details
            }

            var specification = new ContractSpecification(details.Contract.TradingClass, Convert.ToDecimal(details.MinTick));
            if (canonicalSymbol is not null)
            {
                _tradingClassByCanonicalSymbol[canonicalSymbol] = specification;
            }

            return specification;
        }
    }
}
