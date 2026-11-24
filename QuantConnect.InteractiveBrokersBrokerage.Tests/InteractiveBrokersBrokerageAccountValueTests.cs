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

using System.Reflection;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersBrokerageAccountValueTests
    {
        // newer IB Gateway installations send the per-currency account values as "$LEDGER-<key>" by default
        [TestCase("")]
        [TestCase("$LEDGER-")]
        public void HandleUpdateAccountValueReadsCashBalanceAndBaseCurrency(string keyPrefix)
        {
            using var brokerage = new InteractiveBrokersBrokerage();

            brokerage.HandleUpdateAccountValue(this, new IB.UpdateAccountValueEventArgs($"{keyPrefix}CashBalance", "1137895.07", "BASE", "DU123"));
            brokerage.HandleUpdateAccountValue(this, new IB.UpdateAccountValueEventArgs($"{keyPrefix}CashBalance", "1137895.07", "USD", "DU123"));
            brokerage.HandleUpdateAccountValue(this, new IB.UpdateAccountValueEventArgs($"{keyPrefix}CashBalance", "-250.5", "EUR", "DU123"));
            brokerage.HandleUpdateAccountValue(this, new IB.UpdateAccountValueEventArgs($"{keyPrefix}ExchangeRate", "1.00", "USD", "DU123"));

            var accountData = (InteractiveBrokersAccountData)typeof(InteractiveBrokersBrokerage)
                .GetField("_accountData", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(brokerage);

            Assert.AreEqual(2, accountData.CashBalances.Count);
            Assert.AreEqual(1137895.07m, accountData.CashBalances["USD"]);
            Assert.AreEqual(-250.5m, accountData.CashBalances["EUR"]);
            Assert.AreEqual("USD", brokerage.AccountBaseCurrency);
        }
    }
}
