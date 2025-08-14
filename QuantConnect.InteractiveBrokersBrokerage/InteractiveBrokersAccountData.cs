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

using System.Collections.Generic;
using System.Collections.Concurrent;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// This class contains account specific data such as properties, cash balances and holdings
    /// </summary>
    public class InteractiveBrokersAccountData
    {
        /// <summary>
        /// The raw IB account properties
        /// </summary>
        public ConcurrentDictionary<string, string> AccountProperties { get; } = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// The account cash balances indexed by currency
        /// </summary>
        public ConcurrentDictionary<string, decimal> CashBalances { get; } = new ConcurrentDictionary<string, decimal>();

        /// <summary>
        /// The account holdings indexed by symbol
        /// </summary>
        public ConcurrentDictionary<Symbol, MergedHoldings> AccountHoldings { get; } = new ();

        /// <summary>
        /// Clears this instance of <see cref="InteractiveBrokersAccountData"/>
        /// </summary>
        public void Clear()
        {
            AccountProperties.Clear();
            CashBalances.Clear();
            AccountHoldings.Clear();
        }

        public class MergedHoldings
        {
            private readonly HashSet<string> _accounts = new();
            public Holding Holding { get; set; } = new();
            public void Merge(Holding incoming, string account)
            {
                // only merge in holdings of multiple accounts, IB can send the same holdings for the same account multiple times
                if (_accounts.Add(account))
                {
                    if (_accounts.Count == 1)
                    {
                        // shortcut
                        Holding = incoming;
                        return;
                    }

                    // Merge with existing holding: add quantities and recalculate average price
                    // This happens when holdings are reported separately (e.g., across FA group accounts)
                    var totalCost = (Holding.AveragePrice * Holding.Quantity) + (incoming.AveragePrice * incoming.Quantity);

                    Holding.Quantity += incoming.Quantity;

                    // Avoid division by zero when position is fully closed
                    if (Holding.Quantity != 0)
                    {
                        Holding.AveragePrice = totalCost / Holding.Quantity;
                    }
                }
            }
        }
    }
}
