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
using NUnit.Framework;
using QuantConnect.Orders;
using System.Collections.Generic;
using QuantConnect.Brokerages.InteractiveBrokers;
using LeanOrder = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Hermetic tests of the contingent orders (OCO, OTO, OUO, brackets) support: attached orders and OCA groups
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersContingentOrdersTests
    {
        private static readonly DateTime Time = new DateTime(2024, 1, 2, 15, 0, 0);

        [Test]
        public void RebuildsBracketFromOpenOrders()
        {
            var entry = Open(1, parentId: 0, ocaGroup: null, ocaType: 0, new LimitOrder(Symbols.SPY, 100, 100, Time));
            var takeProfit = Open(2, parentId: 1, ocaGroup: "LEAN-1", ocaType: 1, new LimitOrder(Symbols.SPY, -100, 110, Time));
            var stopLoss = Open(3, parentId: 1, ocaGroup: "LEAN-1", ocaType: 1, new StopMarketOrder(Symbols.SPY, -100, 90, Time));
            var plain = Open(4, parentId: 0, ocaGroup: null, ocaType: 0, new LimitOrder(Symbols.AAPL, 10, 100, Time));
            var openOrders = new List<(IBApi.Order, List<LeanOrder>)> { entry, takeProfit, stopLoss, plain };

            InteractiveBrokersBrokerage.SetContingencies(openOrders);

            var bracket = openOrders.Take(3).Select(x => x.Item2[0]).ToList();
            // the set is shared
            Assert.AreEqual(1, bracket.Select(x => x.Contingency.OrderIds).Distinct().Count());
            Assert.AreEqual(3, bracket[0].Contingency.Count);
            Assert.AreEqual(ContingencyRole.Parent, bracket[0].Contingency.Links.Single().Role);
            foreach (var exit in bracket.Skip(1))
            {
                Assert.IsTrue(exit.IsWaitingForTrigger());
                Assert.AreEqual(bracket[0].Contingency.Links[0].Id, exit.GetContingencyLink(ContingencyRole.Child).Id);
                Assert.AreEqual(ContingencyType.OneCancelsOther, exit.GetSiblingLink().Type);
            }
            Assert.IsNull(plain.Item2[0].Contingency);
        }

        [Test]
        public void FilledParentLeavesWorkingSiblings()
        {
            // the parent is gone, the exits are working and reduce each other
            var takeProfit = Open(2, parentId: 1, ocaGroup: "LEAN-1", ocaType: 2, new LimitOrder(Symbols.SPY, -100, 110, Time));
            var stopLoss = Open(3, parentId: 1, ocaGroup: "LEAN-1", ocaType: 2, new StopMarketOrder(Symbols.SPY, -100, 90, Time));
            var openOrders = new List<(IBApi.Order, List<LeanOrder>)> { takeProfit, stopLoss };

            InteractiveBrokersBrokerage.SetContingencies(openOrders);

            foreach (var exit in openOrders.Select(x => x.Item2[0]))
            {
                Assert.IsFalse(exit.IsWaitingForTrigger());
                Assert.AreEqual(2, exit.Contingency.Count);
                Assert.AreEqual(ContingencyType.OneUpdatesOther, exit.Contingency.Links.Single().Type);
            }
        }

        [Test]
        public void SingleOrderOfAnOcaGroupIsPlain()
        {
            var order = Open(2, parentId: 0, ocaGroup: "LEAN-1", ocaType: 1, new LimitOrder(Symbols.SPY, -100, 110, Time));

            InteractiveBrokersBrokerage.SetContingencies(new List<(IBApi.Order, List<LeanOrder>)> { order });
            Assert.IsNull(order.Item2[0].Contingency);
        }

        private static (IBApi.Order, List<LeanOrder>) Open(int orderId, int parentId, string ocaGroup, int ocaType, LeanOrder leanOrder)
        {
            var ibOrder = new IBApi.Order { OrderId = orderId, ParentId = parentId, OcaGroup = ocaGroup, OcaType = ocaType };
            leanOrder.BrokerId.Add(orderId.ToStringInvariant());
            return (ibOrder, new List<LeanOrder> { leanOrder });
        }
    }
}
