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

using QuantConnect.Securities.Future;
using System.Collections.Generic;
using System;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    public class TestUtils
    {
        public static IEnumerable<Symbol> GetFutureContracts(Symbol canonical, int count)
        {
            var expiryFunction = FuturesExpiryFunctions.FuturesExpiryFunction(canonical);
            var lastExpiryMonth = DateTime.UtcNow;

            // Get the next N contracts
            for (var i = 0; i < count; i++)
            {
                var expiry = expiryFunction(lastExpiryMonth);
                var contract = Symbol.CreateFuture(canonical.ID.Symbol, canonical.ID.Market, expiry);

                yield return contract;

                lastExpiryMonth = expiry.AddMonths(1);
            }
        }
    }
}
