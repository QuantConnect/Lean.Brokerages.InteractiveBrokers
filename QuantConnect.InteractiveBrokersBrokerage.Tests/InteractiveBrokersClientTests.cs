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
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersClientTests
    {
        [TestCase(0.4, false)]
        [TestCase(10.6, false)]
        [TestCase(-2.5, false)]
        [TestCase(100, false)]
        [TestCase(0.4, true)]
        [TestCase(10.6, true)]
        [TestCase(-2.5, true)]
        [TestCase(100, true)]
        public void PortfolioUpdateKeepsExactPosition(decimal position, bool isPositionMulti)
        {
            var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            UpdatePortfolioEventArgs update = null;
            client.UpdatePortfolio += (_, e) => update = e;

            var contract = new Contract { Symbol = "AAPL", SecType = "STK", Currency = "USD" };
            if (isPositionMulti)
            {
                client.positionMulti(requestId: 1, account: "DU123456", modelCode: string.Empty, contract, position, averageCost: 150);
            }
            else
            {
                client.updatePortfolio(contract, position, marketPrice: 150, marketValue: 0, averageCost: 150, unrealisedPnl: 0, realisedPnl: 0, accountName: "DU123456");
            }

            Assert.IsNotNull(update);
            Assert.AreEqual(position, update.Position);
        }
    }
}
