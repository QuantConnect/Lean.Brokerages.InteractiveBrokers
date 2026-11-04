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
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersFinancialAdvisorErrorRoutingTests
    {
        [Test]
        public void HandleErrorSuppressesOnlyOwnedFinancialAdvisorServiceRequestErrors()
        {
            using var client = new IB.InteractiveBrokersClient(new IBApi.EReaderMonitorSignal());
            using var accountState = new InteractiveBrokersFinancialAdvisorAccountState(
                client,
                () => { },
                () => true,
                _ => Symbol.Empty,
                "F-MASTER");
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", accountState);

            try
            {
                var serviceRequestId = (int)typeof(InteractiveBrokersFinancialAdvisorAccountState)
                    .GetMethod("NextRequestId", BindingFlags.NonPublic | BindingFlags.Instance)
                    .Invoke(accountState, null);
                var messages = new List<BrokerageMessageEvent>();
                brokerage.Message += (_, message) => messages.Add(message);
                client.Error += brokerage.HandleError;

                client.error(serviceRequestId, 0, 321, "FA service failure", string.Empty);
                client.error(-2, 0, 321, "unallocated negative request failure", string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.IsTrue(accountState.IsServiceOwnedRequestId(serviceRequestId));
                    Assert.AreEqual(1, messages.Count);
                    Assert.AreEqual(BrokerageMessageType.Error, messages[0].Type);
                    Assert.AreEqual("321", messages[0].Code);
                    StringAssert.Contains("unallocated negative request failure", messages[0].Message);
                });
            }
            finally
            {
                SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", null);
            }
        }

        [TestCase(-1)]
        [TestCase(0)]
        [TestCase(int.MaxValue)]
        public void ExpectedGroupsReadbackRetrySuppressesOnlyBrokerageMessage(int requestId)
        {
            using var client = new IB.InteractiveBrokersClient(new IBApi.EReaderMonitorSignal());
            using var accountState = new InteractiveBrokersFinancialAdvisorAccountState(
                client,
                () => { },
                () => true,
                _ => Symbol.Empty,
                "F-MASTER");
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", accountState);
            var pending = InstallGroupsReadbackPending(accountState);

            try
            {
                var publicErrorCount = 0;
                var messages = new List<BrokerageMessageEvent>();
                brokerage.Message += (_, message) => messages.Add(message);
                client.Error += brokerage.HandleError;
                client.Error += (_, _) => publicErrorCount++;

                client.error(
                    requestId,
                    0,
                    10230,
                    "The FA configuration has unsaved changes",
                    string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(1, publicErrorCount,
                        "The client Error event remains public even when the brokerage owns the retry.");
                    Assert.IsEmpty(messages);
                    Assert.IsTrue(GetPendingBoolean(pending, "Finished"),
                        "The internal error callback must still complete the pending readback for retry.");
                    Assert.IsFalse(accountState.IsExpectedGroupsReadbackRetry(
                        new IB.ErrorEventArgs(requestId, 0, 10230, "repeated")),
                        "A completed readback no longer owns a later unkeyed 10230 error.");
                });

                client.error(
                    requestId,
                    0,
                    10230,
                    "Unrelated unsaved changes",
                    string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(2, publicErrorCount);
                    Assert.AreEqual(1, messages.Count);
                    Assert.AreEqual("10230", messages[0].Code);
                    StringAssert.Contains("Unrelated unsaved changes", messages[0].Message);
                });
            }
            finally
            {
                SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", null);
            }
        }

        [TestCase(-1)]
        [TestCase(int.MaxValue)]
        public void UnsavedChangesWithoutPendingGroupsReadbackIsNotSuppressed(int requestId)
        {
            using var client = new IB.InteractiveBrokersClient(new IBApi.EReaderMonitorSignal());
            using var accountState = new InteractiveBrokersFinancialAdvisorAccountState(
                client,
                () => { },
                () => true,
                _ => Symbol.Empty,
                "F-MASTER");
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", accountState);

            try
            {
                var messages = new List<BrokerageMessageEvent>();
                brokerage.Message += (_, message) => messages.Add(message);
                client.Error += brokerage.HandleError;

                client.error(
                    requestId, 0, 10230, "Unowned unsaved changes", string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(1, messages.Count);
                    Assert.AreEqual("10230", messages[0].Code);
                    StringAssert.Contains("Unowned unsaved changes", messages[0].Message);
                });
            }
            finally
            {
                SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", null);
            }
        }

        [Test]
        public void UnsavedChangesBeforeGroupsReadbackWireAuthorizationIsNotSuppressed()
        {
            using var client = new IB.InteractiveBrokersClient(new IBApi.EReaderMonitorSignal());
            using var accountState = new InteractiveBrokersFinancialAdvisorAccountState(
                client,
                () => { },
                () => true,
                _ => Symbol.Empty,
                "F-MASTER");
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", accountState);
            var pending = InstallGroupsReadbackPending(accountState, wireSent: false);

            try
            {
                var messages = new List<BrokerageMessageEvent>();
                brokerage.Message += (_, message) => messages.Add(message);
                client.Error += brokerage.HandleError;

                client.error(
                    int.MaxValue,
                    0,
                    10230,
                    "Pre-wire unsaved changes",
                    string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(1, messages.Count);
                    Assert.AreEqual("10230", messages[0].Code);
                    Assert.IsFalse(GetPendingBoolean(pending, "Finished"));
                });
            }
            finally
            {
                SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", null);
            }
        }

        [Test]
        public void OtherErrorsWithMaximumRequestIdAreNotSuppressed()
        {
            using var client = new IB.InteractiveBrokersClient(new IBApi.EReaderMonitorSignal());
            using var accountState = new InteractiveBrokersFinancialAdvisorAccountState(
                client,
                () => { },
                () => true,
                _ => Symbol.Empty,
                "F-MASTER");
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", accountState);
            var pending = InstallGroupsReadbackPending(accountState);

            try
            {
                var messages = new List<BrokerageMessageEvent>();
                brokerage.Message += (_, message) => messages.Add(message);
                client.Error += brokerage.HandleError;

                client.error(
                    int.MaxValue,
                    0,
                    10231,
                    "Unrelated maximum-id error",
                    string.Empty);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(1, messages.Count);
                    Assert.AreEqual("10231", messages[0].Code);
                    Assert.IsFalse(GetPendingBoolean(pending, "Finished"));
                });
            }
            finally
            {
                SetPrivateFieldValue(brokerage, "_financialAdvisorAccountState", null);
            }
        }

        private static object InstallGroupsReadbackPending(
            InteractiveBrokersFinancialAdvisorAccountState accountState,
            bool wireSent = true)
        {
            var stateType = typeof(InteractiveBrokersFinancialAdvisorAccountState);
            var pendingType = stateType.GetNestedType(
                "PendingRequest", BindingFlags.NonPublic);
            var pendingKindType = stateType.GetNestedType(
                "PendingKind", BindingFlags.NonPublic);
            var constructor = pendingType.GetConstructors(
                    BindingFlags.Instance | BindingFlags.NonPublic)
                .Single();
            var pending = constructor.Invoke(new[]
            {
                null,
                System.Enum.Parse(pendingKindType, "FinancialAdvisorReadback"),
                (object)0,
                1,
                0L
            });
            pendingType.GetProperty(
                    "WireSent", BindingFlags.Instance | BindingFlags.NonPublic)
                .SetValue(pending, wireSent);
            SetPrivateFieldValue(accountState, "_pendingRequest", pending);
            return pending;
        }

        private static bool GetPendingBoolean(object pending, string propertyName)
        {
            return (bool)pending.GetType()
                .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(pending);
        }

        private static void SetPrivateFieldValue(object instance, string name, object value)
        {
            instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(instance, value);
        }
    }
}
