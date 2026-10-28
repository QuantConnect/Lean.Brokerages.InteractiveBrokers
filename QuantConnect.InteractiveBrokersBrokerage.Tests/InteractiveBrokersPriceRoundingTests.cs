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
using System.Collections.Generic;
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Hermetic regression tests for the price rounding in <c>NormalizePriceToBrokerage</c>:
    /// a contract whose details could not be fetched has an unknown (zero) minimum tick, and the
    /// price must then be sent as the algorithm set it, never as 0 (issue #278).
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersPriceRoundingTests
    {
        private static readonly MethodInfo RoundPriceMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod("RoundPrice", BindingFlags.Static | BindingFlags.NonPublic);

        private static readonly Symbol PlatinumFuture = Symbol.CreateFuture("PL", Market.NYMEX, new DateTime(2027, 1, 27));

        private static readonly Contract PlatinumContract = new Contract
        {
            Symbol = "PL", SecType = IB.SecurityType.Future, Exchange = "NYMEX", Currency = "USD", LastTradeDateOrContractMonth = "20270127"
        };

        [TestCase(1615.0, 0.1, 1615.0)]
        [TestCase(1615.37, 0.1, 1615.4)]
        [TestCase(1615.0, 0, 1615.0)]
        [TestCase(1615.37, 0, 1615.37)]
        [TestCase(1615.37, -1, 1615.37)]
        public void RoundPriceKeepsTheInputWhenTheMinTickIsUnknown(decimal input, decimal minTick, decimal expected)
        {
            var rounded = (decimal)RoundPriceMethod.Invoke(null, new object[] { input, minTick });

            Assert.AreEqual(expected, rounded);
        }

        [Test]
        public void PriceIsSentUnroundedWhenTheContractDetailsAreUnavailable()
        {
            // the contract details request timed out: no specification, min tick 0
            var brokerage = CreateOfflineBrokerage(details: null);
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, e) => messages.Add(e);

            var price = brokerage.NormalizePriceToBrokerage(1615.0m, PlatinumContract, PlatinumFuture);

            Assert.AreEqual(1615.0, price);
            Assert.IsEmpty(messages, "no PriceRounding warning: the price was not changed");
        }

        [Test]
        public void MagnifiedPriceIsSentUnroundedWithoutWarningWhenTheContractDetailsAreUnavailable()
        {
            // ZC has a price magnifier of 100: the price sent is magnified but not rounded
            var cornFuture = Symbol.CreateFuture("ZC", Market.CBOT, new DateTime(2027, 3, 12));
            var cornContract = new Contract
            {
                Symbol = "ZC", SecType = IB.SecurityType.Future, Exchange = "CBOT", Currency = "USD", LastTradeDateOrContractMonth = "20270312"
            };
            var brokerage = CreateOfflineBrokerage(details: null);
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, e) => messages.Add(e);

            var price = brokerage.NormalizePriceToBrokerage(4.5025m, cornContract, cornFuture);

            Assert.AreEqual(450.25, price);
            Assert.IsEmpty(messages, "no PriceRounding warning: the tick size is unknown");
        }

        [Test]
        public void PriceIsRoundedToTheMinTickWhenTheContractDetailsAreAvailable()
        {
            var brokerage = CreateOfflineBrokerage(details: new ContractDetails { Contract = PlatinumContract, MinTick = 0.1 });
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, e) => messages.Add(e);

            var price = brokerage.NormalizePriceToBrokerage(1615.37m, PlatinumContract, PlatinumFuture);

            Assert.AreEqual(1615.4, price);
            Assert.AreEqual(1, messages.Count);
            Assert.AreEqual("PriceRounding", messages[0].Code);
        }

        private static InteractiveBrokersBrokerage CreateOfflineBrokerage(ContractDetails details)
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage._contractSpecificationService = new IB.ContractSpecificationService((contract, ticker, failIfNotFound) => details);
            return brokerage;
        }
    }
}
