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
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using LeanOrder = QuantConnect.Orders.Order;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Hermetic regression test for IB error 201 ("Invalid account number") when an FA group
    /// filter is active and an order also carries a per-order <see cref="InteractiveBrokersOrderProperties.Account"/>
    /// override. The fixture invokes <c>InteractiveBrokersBrokerage.ConvertOrder</c> via reflection
    /// (the assembly already exposes its internals to this test project) so the test can run in CI
    /// without a live IB Gateway / TWS connection.
    /// </summary>
    /// <remarks>
    /// Validated end-to-end against a live IB Financial Advisor master account: the same
    /// (filter set + per-order Account override) configuration that produced IB error 201 before
    /// the fix is accepted with <c>Status: Submitted</c> after the fix. A companion two-order
    /// scenario (one with an Account override, one without) confirmed both branches route as
    /// intended in TWS — the per-order override appears under the "IndBrokerage" allocation, the
    /// default order appears under the configured FA group's allocation method.
    /// </remarks>
    [TestFixture]
    public class InteractiveBrokersFaGroupOrderConversionTests
    {
        private const string FaMasterAccount = "F1234567";    // any account containing 'F' is a master account
        private const string FaGroupName     = "TestGroup1";
        private const string AgentDescription = "I";          // IB.AgentDescription.Individual

        private static readonly FieldInfo AccountField =
            typeof(InteractiveBrokersBrokerage).GetField("_account", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly FieldInfo AgentDescriptionField =
            typeof(InteractiveBrokersBrokerage).GetField("_agentDescription", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly FieldInfo FaFilterField =
            typeof(InteractiveBrokersBrokerage).GetField("_financialAdvisorsGroupFilter", BindingFlags.Static | BindingFlags.NonPublic);
        private static readonly MethodInfo ConvertOrderMethod =
            typeof(InteractiveBrokersBrokerage).GetMethod(
                "ConvertOrder",
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                types: new[] { typeof(List<LeanOrder>), typeof(Contract), typeof(int) },
                modifiers: null);

        [TearDown]
        public void TearDown()
        {
            // Reset the static FA filter so other fixtures aren't polluted.
            FaFilterField.SetValue(null, null);
        }

        /// <summary>
        /// When the FA group filter is configured and the order carries a per-order
        /// <c>Account</c> override, the resulting <see cref="IBApi.Order"/> must have
        /// <c>Account</c> set and <c>FaGroup</c>/<c>FaMethod</c> cleared. The two are mutually
        /// exclusive on the wire; sending them together produces IB error 201
        /// ("Invalid account number").
        /// </summary>
        [Test]
        public void LimitOrder_WithAccountOverrideAndGroupFilter_AccountWinsAndFaGroupCleared()
        {
            // Arrange
            var brokerage = CreateOfflineBrokerage();
            FaFilterField.SetValue(null, FaGroupName);

            const string overrideAccount = "TestSubAccount";
            var props = new InteractiveBrokersOrderProperties
            {
                Account = overrideAccount,
                FaGroup = string.Empty,
                FaProfile = string.Empty,
                FaMethod = string.Empty,
            };

            var leanOrder = new LimitOrder(Symbols.SPY, 10m, 100.00m,
                new DateTime(2026, 1, 1, 15, 0, 0, DateTimeKind.Utc),
                tag: "",
                properties: props);

            var ibContract = new Contract
            {
                Symbol = "SPY", SecType = IB.SecurityType.Stock, Exchange = "SMART", Currency = "USD"
            };

            // Act
            var ibOrder = (IBApi.Order)ConvertOrderMethod.Invoke(
                brokerage,
                new object[] { new List<LeanOrder> { leanOrder }, ibContract, 1 });

            // Assert
            Assert.AreEqual(overrideAccount, ibOrder.Account,
                "When orderProperties.Account is supplied, ibOrder.Account must reflect that override.");
            Assert.IsTrue(string.IsNullOrEmpty(ibOrder.FaGroup),
                "FaGroup must be cleared when an Account override is supplied (IB rejects Account+FaGroup together with error 201).");
            Assert.IsTrue(string.IsNullOrEmpty(ibOrder.FaMethod),
                "FaMethod must be cleared alongside FaGroup when an Account override is supplied.");
        }

        /// <summary>
        /// Builds an <see cref="InteractiveBrokersBrokerage"/> with just enough state for
        /// <c>ConvertOrder</c> to run without opening a TCP connection to TWS / IB Gateway.
        /// </summary>
        private static InteractiveBrokersBrokerage CreateOfflineBrokerage()
        {
            var brokerage = new InteractiveBrokersBrokerage();

            AccountField.SetValue(brokerage, FaMasterAccount);
            AgentDescriptionField.SetValue(brokerage, AgentDescription);

            // Stub contract-details provider — returns a deterministic min tick of 0.01 so the
            // limit price round-trips cleanly through NormalizePriceToBrokerage.
            brokerage._contractSpecificationService = new IB.ContractSpecificationService(
                (contract, ticker, failIfNotFound) => new ContractDetails
                {
                    Contract = contract,
                    MinTick = 0.01
                });

            return brokerage;
        }
    }
}
