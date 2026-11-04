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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Brokerages.InteractiveBrokers.Client;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersFinancialAdvisorAccountStateRuntimeTests
    {
        [Test]
        public async Task BrokerageOperationsAreSerializedTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, snapshot.Generation);
                Assert.IsTrue(snapshot.IsComplete);
                Assert.AreEqual(2, scenario.GroupsRequestCount);
                Assert.AreEqual(
                    InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(
                        Scenario.GroupsXml),
                    snapshot.GroupConfigurationVersion);
                CollectionAssert.AreEqual(
                    new[]
                    {
                        "managed",
                        "fa:1",
                        "fa:3",
                        "family",
                        "positions:Alpha",
                        "positions:Beta",
                        "positions:ACC3",
                        "account:ACC1",
                        "account:ACC2",
                        "account:ACC3",
                        "managed",
                        "fa:1",
                        "fa:3",
                        "family"
                    },
                    scenario.Requests);
                CollectionAssert.AreEqual(
                    Enumerable.Range(0, 6).Select(offset => int.MinValue + offset),
                    scenario.KeyedRequestIds);
                Assert.IsTrue(
                    scenario.KeyedRequestIds.All(state.IsServiceOwnedRequestId));
                Assert.AreEqual(2, snapshot.Groups.Count);
                Assert.AreEqual(2, snapshot.AllGroups.Count);
                Assert.AreEqual(3, snapshot.Accounts.Count);
                Assert.AreEqual(4, snapshot.AccountDirectory.Count);
                CollectionAssert.AreEqual(new[] { "ACC3" }, snapshot.UnassignedAccountIds);
                Assert.AreEqual(1, scenario.MaximumConcurrentExternalCalls);
            });
        }

        [Test]
        public async Task SnapshotGenerationMonotonicTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();

            var first = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var firstAccountIds = first.Accounts.Keys.ToArray();
            var second = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Throws<NotSupportedException>(() =>
                ((IDictionary<string, BrokerageAccountState>)first.Accounts).Remove("ACC1"));
            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, first.Status);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, second.Status);
                Assert.AreEqual(1, first.Generation);
                Assert.AreEqual(2, second.Generation);
                Assert.Greater(second.Generation, first.Generation);
                CollectionAssert.AreEqual(firstAccountIds, first.Accounts.Keys);
                Assert.GreaterOrEqual(second.LastSuccessfulUpdateUtc,
                    first.LastSuccessfulUpdateUtc);
            });
        }

        [Test]
        public async Task DisposeAfterReadyPublishesStaleSnapshotTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            state.Dispose();
            var stale = state.Snapshot;

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, stale.Status);
                Assert.AreEqual(ready.Generation, stale.Generation);
                Assert.AreEqual(
                    ready.LastSuccessfulUpdateUtc,
                    stale.LastSuccessfulUpdateUtc);
                CollectionAssert.AreEquivalent(
                    ready.Groups.Keys,
                    stale.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    ready.Accounts.Keys,
                    stale.Accounts.Keys);
                Assert.AreEqual(
                    ready.Accounts["ACC1"].NetLiquidation,
                    stale.Accounts["ACC1"].NetLiquidation);
                StringAssert.Contains("disposed", stale.ErrorMessage);
            });
        }

        [Test]
        public async Task DisposeDuringFirstRefreshPublishesFailedSnapshotTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var requestEntered = new ManualResetEventSlim();
            using var releaseRequest = new ManualResetEventSlim();
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            scenario.Actions.RequestManagedAccounts = authorize =>
                requestManagedAccounts(() =>
                {
                    var authorized = authorize();
                    if (authorized)
                    {
                        requestEntered.Set();
                        if (!releaseRequest.Wait(TimeSpan.FromSeconds(5)))
                        {
                            throw new TimeoutException(
                                "Test managed-account request was not released.");
                        }
                    }
                    return authorized;
                });
            using var state = scenario.CreateState();

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(requestEntered.Wait(TimeSpan.FromSeconds(5)));
            var worker = GetPrivateField<Task>(state, "_worker");
            try
            {
                state.Dispose();
                var failed = state.Snapshot;
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Failed,
                        failed.Status);
                    Assert.AreEqual(0, failed.Generation);
                    Assert.AreEqual(default(DateTime), failed.LastSuccessfulUpdateUtc);
                    StringAssert.Contains("disposed", failed.ErrorMessage);
                });
            }
            finally
            {
                releaseRequest.Set();
            }
            await worker.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.AreEqual(
                BrokerageAccountSnapshotStatus.Failed,
                state.Snapshot.Status);
        }

        [Test]
        public async Task OrdinaryRefreshFailureDoesNotBlockGroupTradingTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            scenario.Actions.RequestPositions =
                (requestId, accountOrGroup, authorize) =>
                    scenario.RunAuthorized(authorize, () =>
                        throw new InvalidOperationException(
                            "simulated ordinary position refresh failure"));

            var stale = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, ready.Status);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, stale.Status);
                StringAssert.Contains(
                    "simulated ordinary position refresh failure", stale.ErrorMessage);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [TestCase("managed")]
        [TestCase("fa:1")]
        [TestCase("fa:3")]
        [TestCase("family")]
        public async Task UnkeyedCallbackBeforeWireAuthorizationIsDiscardedTest(
            string request)
        {
            using var scenario = new Scenario();
            var managedAccounts = scenario.Actions.RequestManagedAccounts;
            var financialAdvisor = scenario.Actions.RequestFinancialAdvisor;
            var familyCodes = scenario.Actions.RequestFamilyCodes;
            var foreignCallbackInjected = false;
            void InjectForeignCallback(string currentRequest)
            {
                if (foreignCallbackInjected || currentRequest != request)
                {
                    return;
                }
                foreignCallbackInjected = true;
                switch (currentRequest)
                {
                    case "managed":
                        scenario.Client.managedAccounts("MASTER,FOREIGN");
                        break;
                    case "fa:1":
                        scenario.Client.receiveFA(1, Scenario.EmptyGroupsXml);
                        break;
                    case "fa:3":
                        scenario.Client.receiveFA(3, "<ListOfAccountAliases />");
                        break;
                    case "family":
                        scenario.Client.familyCodes(Array.Empty<FamilyCode>());
                        break;
                }
            }
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                InjectForeignCallback("managed");
                return managedAccounts(authorize);
            };
            scenario.Actions.RequestFinancialAdvisor = (faDataType, authorize) =>
            {
                InjectForeignCallback($"fa:{faDataType}");
                return financialAdvisor(faDataType, authorize);
            };
            scenario.Actions.RequestFamilyCodes = authorize =>
            {
                InjectForeignCallback("family");
                return familyCodes(authorize);
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.IsTrue(foreignCallbackInjected);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(
                    2,
                    scenario.Requests.Count(actualRequest => actualRequest == request));
            });
        }

        [TestCase("managed", false)]
        [TestCase("managed", true)]
        [TestCase("fa:1", false)]
        [TestCase("fa:1", true)]
        [TestCase("fa:3", false)]
        [TestCase("fa:3", true)]
        [TestCase("family", false)]
        [TestCase("family", true)]
        public async Task UnkeyedSocketExceptionUsesAuthorizationBoundaryTest(
            string request,
            bool afterAuthorization)
        {
            using var scenario = new Scenario();
            var managedAccounts = scenario.Actions.RequestManagedAccounts;
            var financialAdvisor = scenario.Actions.RequestFinancialAdvisor;
            var familyCodes = scenario.Actions.RequestFamilyCodes;
            bool Fail(Func<bool> authorize)
            {
                if (afterAuthorization && !authorize())
                {
                    return false;
                }
                throw new System.Net.Sockets.SocketException(
                    (int)System.Net.Sockets.SocketError.ConnectionReset);
            }
            scenario.Actions.RequestManagedAccounts = authorize =>
                request == "managed" ? Fail(authorize) : managedAccounts(authorize);
            scenario.Actions.RequestFinancialAdvisor = (faDataType, authorize) =>
                request == $"fa:{faDataType}"
                    ? Fail(authorize)
                    : financialAdvisor(faDataType, authorize);
            scenario.Actions.RequestFamilyCodes = authorize =>
                request == "family" ? Fail(authorize) : familyCodes(authorize);
            using var state = scenario.CreateState();

            var failed = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            scenario.Actions.RequestManagedAccounts = managedAccounts;
            scenario.Actions.RequestFinancialAdvisor = financialAdvisor;
            scenario.Actions.RequestFamilyCodes = familyCodes;
            if (afterAuthorization)
            {
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, failed.Status);
                    Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
                });
                return;
            }

            Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, failed.Status);
            var recovered = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task AuthorizedKeyedWriteFailureStillCancelsSubscriptionTest(
            bool positions)
        {
            using var scenario = Scenario.SingleAccount();
            var attemptedRequestId = 0;
            if (positions)
            {
                scenario.Actions.RequestPositions =
                    (requestId, accountOrGroup, authorize) =>
                        scenario.RunAuthorized(authorize, () =>
                        {
                            attemptedRequestId = requestId;
                            throw new System.Net.Sockets.SocketException(
                                (int)System.Net.Sockets.SocketError.ConnectionReset);
                        });
            }
            else
            {
                scenario.Actions.RequestAccountUpdates =
                    (requestId, accountId, authorize) =>
                        scenario.RunAuthorized(authorize, () =>
                        {
                            attemptedRequestId = requestId;
                            throw new System.Net.Sockets.SocketException(
                                (int)System.Net.Sockets.SocketError.ConnectionReset);
                        });
            }
            using var state = scenario.CreateState();

            var failed = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var canceledRequestIds = positions
                ? scenario.CanceledPositionIds
                : scenario.CanceledAccountIds;

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, failed.Status);
                Assert.AreNotEqual(0, attemptedRequestId);
                Assert.AreEqual(
                    1,
                    canceledRequestIds.Count(
                        requestId => requestId == attemptedRequestId));
            });
        }

        [Test]
        public async Task UnkeyedTimeoutRequiresFreshConnectionTest()
        {
            using var scenario = new Scenario
            {
                ManagedAccounts = "MASTER,ACC1",
                GroupsDocument = Scenario.EmptyGroupsXml,
                EndingGroupsDocument = Scenario.EmptyGroupsXml,
                AliasesDocument = "<ListOfAccountAliases />",
                FamilyCodes = Array.Empty<FamilyCode>()
            };
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                if (!authorize())
                {
                    return false;
                }
                scenario.Requests.Add("managed-timeout");
                return true;
            };
            using var state = scenario.CreateState(requestTimeout: TimeSpan.FromMilliseconds(100));

            var timeout = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, timeout.Status);
                StringAssert.Contains("Timed out waiting for IB managed accounts", timeout.ErrorMessage);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            });

            // A late unkeyed callback cannot be reused after the timeout.
            scenario.Client.managedAccounts(scenario.ManagedAccounts);
            Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            scenario.Client.error(-1, 0, 1101, "Connectivity restored; data lost.", string.Empty);
            Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            scenario.Client.error(-1, 0, 1102, "Connectivity restored; data maintained.", string.Empty);
            Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));

            scenario.Actions.RequestManagedAccounts = authorize =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add("managed-reconnected");
                    scenario.Client.managedAccounts(scenario.ManagedAccounts);
                });
            scenario.Client.nextValidId(123);
            Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            scenario.Client.connectionClosed();
            scenario.Client.nextValidId(124);

            var recovered = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
            Assert.AreEqual(1, recovered.Generation);
        }

        [Test]
        public async Task BlockingPublicSubscriberTimeoutRecoversAfterPhysicalReconnectTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var callbacks = new BlockingCollection<Action>();
            using var subscriberEntered = new ManualResetEventSlim();
            using var releaseSubscriber = new ManualResetEventSlim();
            using var lateManagedAccountsDelivered = new ManualResetEventSlim();
            using var reconnectDelivered = new ManualResetEventSlim();
            Exception callbackPumpException = null;
            var callbackPump = Task.Run(() =>
            {
                try
                {
                    foreach (var callback in callbacks.GetConsumingEnumerable())
                    {
                        callback();
                    }
                }
                catch (Exception exception)
                {
                    callbackPumpException = exception;
                }
            });
            EventHandler<ReceiveFaEventArgs> blockingSubscriber = (_, _) =>
            {
                subscriberEntered.Set();
                if (!releaseSubscriber.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("The blocking public subscriber was not released.");
                }
            };
            scenario.Client.ReceiveFa += blockingSubscriber;
            using var state = scenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(100));
            try
            {
                callbacks.Add(() =>
                    scenario.Client.receiveFA(1, Scenario.EmptyGroupsXml));
                Assert.IsTrue(subscriberEntered.Wait(TimeSpan.FromSeconds(5)));

                scenario.Actions.RequestManagedAccounts = authorize =>
                    scenario.RunAuthorized(authorize, () =>
                    {
                        scenario.Requests.Add("managed-behind-blocked-subscriber");
                        callbacks.Add(() =>
                        {
                            scenario.Client.managedAccounts(scenario.ManagedAccounts);
                            lateManagedAccountsDelivered.Set();
                        });
                    });

                var timeout = await RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(Array.Empty<string>()));

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, timeout.Status);
                    StringAssert.Contains(
                        "Timed out waiting for IB managed accounts", timeout.ErrorMessage);
                    Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
                    Assert.IsFalse(lateManagedAccountsDelivered.IsSet);
                });

                releaseSubscriber.Set();
                Assert.IsTrue(lateManagedAccountsDelivered.Wait(TimeSpan.FromSeconds(5)));
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));

                scenario.Actions.RequestManagedAccounts = authorize =>
                    scenario.RunAuthorized(authorize, () =>
                    {
                        scenario.Requests.Add("managed-after-physical-reconnect");
                        scenario.Client.managedAccounts(scenario.ManagedAccounts);
                    });
                callbacks.Add(() =>
                {
                    scenario.Client.connectionClosed();
                    scenario.Client.nextValidId(124);
                    state.NotifyBrokerageConnected();
                    reconnectDelivered.Set();
                });
                Assert.IsTrue(reconnectDelivered.Wait(TimeSpan.FromSeconds(5)));

                var recovered = await WaitForReadyGenerationAsync(state, timeout.Generation);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                    Assert.AreEqual(timeout.Generation + 1, recovered.Generation);
                    CollectionAssert.Contains(
                        scenario.Requests, "managed-after-physical-reconnect");
                });
            }
            finally
            {
                scenario.Client.ReceiveFa -= blockingSubscriber;
                releaseSubscriber.Set();
                callbacks.CompleteAdding();
                await Task.WhenAny(
                    callbackPump, Task.Delay(TimeSpan.FromSeconds(5)));
            }
            Assert.Multiple(() =>
            {
                Assert.IsTrue(callbackPump.IsCompleted, "The callback pump did not stop.");
                Assert.IsNull(callbackPumpException);
            });
        }

        [TestCase(1101)]
        [TestCase(1102)]
        public async Task LogicalReconnectRestoresAvailabilityWithoutOutstandingUnkeyedRequestTest(
            int recoveryCode)
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            scenario.Client.error(
                -1, 0, 1100, "Connectivity between IB and TWS was lost.", string.Empty);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            });
            var disconnectedRequestVersion = GetRequestVersion(state);
            var disconnectedRequestCount = scenario.Requests.Count;

            scenario.Client.error(
                -1, 0, recoveryCode, "Connectivity between IB and TWS was restored.",
                string.Empty);
            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                Assert.AreEqual(disconnectedRequestVersion, GetRequestVersion(state));
                Assert.AreEqual(disconnectedRequestCount, scenario.Requests.Count);
            });
            var recovered = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.AreEqual(ready.Generation + 1, recovered.Generation);
            });
        }

        [Test]
        public async Task LogicalReconnectDoesNotClearOutstandingUnkeyedRequestTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var wireSent = new ManualResetEventSlim();
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                if (!authorize())
                {
                    return false;
                }
                wireSent.Set();
                return true;
            };
            using var state = scenario.CreateState();
            var interruptedRefresh = RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(wireSent.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsTrue(SpinWait.SpinUntil(
                () => IsPendingRequestWireSent(state), TimeSpan.FromSeconds(5)));

            scenario.Client.error(
                -1, 0, 1100, "Connectivity between IB and TWS was lost.", string.Empty);
            var stale = await interruptedRefresh;
            scenario.Client.error(
                -1, 0, 1102, "Connectivity between IB and TWS was restored.", string.Empty);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, stale.Status);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            });

            scenario.Actions.RequestManagedAccounts = authorize =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add("managed-reconnected");
                    scenario.Client.managedAccounts(scenario.ManagedAccounts);
                });
            scenario.Client.nextValidId(123);
            Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            scenario.Client.connectionClosed();
            scenario.Client.nextValidId(124);

            var recovered = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task AutomaticManagedAccountsDoesNotCompleteExplicitRequestTest(
            bool reconnect)
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var previousGeneration = 0L;
            if (reconnect)
            {
                var ready = await RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(Array.Empty<string>()));
                previousGeneration = ready.Generation;
                scenario.Client.connectionClosed();
                scenario.Requests.Clear();
            }

            using var explicitRequestSent = new ManualResetEventSlim();
            var explicitRequestCount = 0;
            scenario.Actions.RequestManagedAccounts = authorize =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add("managed-explicit");
                    if (Interlocked.Increment(ref explicitRequestCount) == 1)
                    {
                        explicitRequestSent.Set();
                    }
                    else
                    {
                        scenario.Client.managedAccounts(scenario.ManagedAccounts);
                    }
                });

            scenario.Client.connectAck();
            Task<BrokerageAccountSnapshot> refresh;
            if (reconnect)
            {
                scenario.Client.nextValidId(321);
                state.NotifyBrokerageConnected();
                refresh = WaitForReadyGenerationAsync(state, previousGeneration);
            }
            else
            {
                refresh = RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(Array.Empty<string>()));
            }

            Assert.IsTrue(explicitRequestSent.Wait(TimeSpan.FromSeconds(5)));
            scenario.Client.managedAccounts("MASTER,AUTO");
            scenario.Client.managedAccounts(scenario.ManagedAccounts);
            var snapshot = await refresh;

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEquivalent(
                    new[] { "MASTER", "ACC1" }, snapshot.ManagedAccountIds);
                CollectionAssert.DoesNotContain(
                    snapshot.ManagedAccountIds, "AUTO");
                Assert.AreEqual(
                    2,
                    scenario.Requests.Count(request => request == "managed-explicit"));
            });
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task HandshakeCacheRequiresPriorRefreshDemandTest(
            bool reconnect)
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var previousGeneration = 0L;
            if (reconnect)
            {
                var ready = await RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(Array.Empty<string>()));
                previousGeneration = ready.Generation;
                scenario.Client.connectionClosed();
            }

            scenario.Requests.Clear();
            scenario.Actions.RequestManagedAccounts = authorize =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add("ending-managed-request");
                    scenario.Client.managedAccounts(scenario.ManagedAccounts);
                });

            scenario.Client.connectAck();
            scenario.Client.managedAccounts(scenario.ManagedAccounts);
            scenario.Client.managedAccounts("MASTER,DUPLICATE");

            BrokerageAccountSnapshot snapshot;
            if (reconnect)
            {
                scenario.Client.nextValidId(654);
                state.NotifyBrokerageConnected();
                snapshot = await WaitForReadyGenerationAsync(
                    state, previousGeneration);
            }
            else
            {
                snapshot = await RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(Array.Empty<string>()));
            }

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEquivalent(
                    new[] { "MASTER", "ACC1" }, snapshot.ManagedAccountIds);
                CollectionAssert.DoesNotContain(
                    snapshot.ManagedAccountIds, "DUPLICATE");
                CollectionAssert.DoesNotContain(
                    snapshot.ManagedAccountIds, "EXPLICIT");
                Assert.AreEqual(
                    reconnect ? 1 : 2,
                    scenario.Requests.Count(request =>
                        request == "ending-managed-request"));
            });
        }

        [Test]
        public async Task HandshakeCacheIsBoundToPhysicalConnectionGenerationTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            scenario.Client.connectionClosed();
            scenario.Requests.Clear();
            scenario.Client.connectAck();
            scenario.Client.managedAccounts("MASTER,STALE");
            scenario.Client.connectAck();
            var explicitRequests = 0;
            scenario.Actions.RequestManagedAccounts = authorize =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add("managed-current-generation");
                    scenario.Client.managedAccounts(scenario.ManagedAccounts);
                    if (Interlocked.Increment(ref explicitRequests) == 1)
                    {
                        scenario.Client.managedAccounts(scenario.ManagedAccounts);
                    }
                });

            scenario.Client.nextValidId(655);
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(
                state, ready.Generation);

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEquivalent(
                    new[] { "MASTER", "ACC1" }, recovered.ManagedAccountIds);
                CollectionAssert.DoesNotContain(recovered.ManagedAccountIds, "STALE");
                Assert.AreEqual(
                    2,
                    scenario.Requests.Count(request =>
                        request == "managed-current-generation"));
            });
        }

        [Test]
        public async Task PhysicalReconnectQueuesRefreshOnlyAfterPriorAcceptedRequestTest()
        {
            using (var unusedScenario = Scenario.SingleAccount())
            using (var unusedState = unusedScenario.CreateState())
            {
                unusedScenario.Client.connectionClosed();
                unusedScenario.Client.nextValidId(123);
                unusedState.NotifyBrokerageConnected();
                unusedScenario.Client.nextValidId(124);
                unusedState.NotifyBrokerageConnected();

                Assert.Multiple(() =>
                {
                    CollectionAssert.IsEmpty(unusedScenario.Requests);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        unusedState.Snapshot.Status);
                });
            }

            using var scenario = new Scenario();
            var brokerageConnected = true;
            using var state = scenario.CreateState(
                isConnected: () => brokerageConnected);
            var alpha = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Alpha" }));

            scenario.Client.connectionClosed();
            brokerageConnected = false;
            Assert.IsFalse(state.RequestRefresh(
                new[] { "Beta" }, new[] { "ACC3" }));
            scenario.Requests.Clear();
            scenario.Client.nextValidId(125);

            CollectionAssert.IsEmpty(
                scenario.Requests,
                "NextValidId occurs while Connect() still reports IsConnecting.");

            brokerageConnected = true;
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(
                state, alpha.Generation);

            CollectionAssert.AreEqual(
                new[]
                {
                    "managed",
                    "fa:1",
                    "fa:3",
                    "family",
                    "positions:Alpha",
                    "account:ACC1",
                    "managed",
                    "fa:1",
                    "fa:3",
                    "family"
                },
                scenario.Requests);
            var requestCount = scenario.Requests.Count;
            var requestVersion = GetRequestVersion(state);

            scenario.Client.nextValidId(126);
            state.NotifyBrokerageConnected();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(alpha.Generation + 1, recovered.Generation);
                Assert.IsFalse(recovered.IsComplete);
                CollectionAssert.AreEquivalent(
                    new[] { "Alpha" }, recovered.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    new[] { "ACC1" }, recovered.Accounts.Keys);
                Assert.AreEqual(requestCount, scenario.Requests.Count);
                Assert.AreEqual(requestVersion, GetRequestVersion(state));
            });
        }

        [Test]
        public async Task FullObsoleteChannelRetainsReconnectRefreshDemandTest()
        {
            using var scenario = new Scenario();
            using var paceEntered = new ManualResetEventSlim();
            using var releasePacing = new ManualResetEventSlim();
            var blockPacing = false;
            var blockedPaceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                if (blockPacing &&
                    Interlocked.Increment(ref blockedPaceCalls) == 1)
                {
                    paceEntered.Set();
                    if (!releasePacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException(
                            "Test pacing callback was not released.");
                    }
                }
            });
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Alpha" }));

            blockPacing = true;
            var interrupted = RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Beta" }));
            Assert.IsTrue(paceEntered.Wait(TimeSpan.FromSeconds(5)));
            try
            {
                FillRefreshQueue(state);
                scenario.Client.connectionClosed();
                FillRefreshQueue(state);
                scenario.Client.nextValidId(656);
                state.NotifyBrokerageConnected();
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
            }
            finally
            {
                releasePacing.Set();
            }
            await interrupted;
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                state.NotifyBrokerageConnected();
                return state.Snapshot.Status ==
                    BrokerageAccountSnapshotStatus.Refreshing;
            }, TimeSpan.FromSeconds(5)));
            var recovered = await WaitForReadyGenerationAsync(
                state, ready.Generation);

            Assert.Multiple(() =>
            {
                CollectionAssert.AreEqual(new[] { "Beta" }, recovered.Groups.Keys);
                Assert.AreEqual(ready.Generation + 1, recovered.Generation);
            });
        }

        [Test]
        public async Task ServiceRequestOwnershipPersistsUntilPhysicalReconnectTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var retiredRequestId = scenario.KeyedRequestIds.First();
            var firstConnectionRequestCount = scenario.KeyedRequestIds.Count;

            Assert.IsTrue(state.IsServiceOwnedRequestId(retiredRequestId));

            scenario.Client.error(
                -1, 0, 1100,
                "Connectivity between IB and TWS was lost.", string.Empty);
            scenario.Client.error(
                -1, 0, 1102,
                "Connectivity between IB and TWS was restored.", string.Empty);

            Assert.IsTrue(
                state.IsServiceOwnedRequestId(retiredRequestId),
                "A logical reconnect must retain service request ownership.");

            scenario.Client.connectionClosed();

            Assert.IsTrue(
                state.IsServiceOwnedRequestId(retiredRequestId),
                "Late callbacks remain service-owned until reconnect is confirmed.");

            scenario.Client.nextValidId(127);
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(
                state, ready.Generation);
            var reconnectRequestIds = scenario.KeyedRequestIds
                .Skip(firstConnectionRequestCount)
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(ready.Generation + 1, recovered.Generation);
                Assert.IsFalse(state.IsServiceOwnedRequestId(retiredRequestId));
                Assert.IsNotEmpty(reconnectRequestIds);
                Assert.IsTrue(
                    reconnectRequestIds.All(state.IsServiceOwnedRequestId));
            });
        }

        [Test]
        public void ServiceRequestOwnershipUsesBoundedConnectionEpochIntervalTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var state = scenario.CreateState();
            var nextRequestId = typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetMethod(
                    "NextRequestId",
                    BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(nextRequestId);
            var allocated = Enumerable.Range(0, 10000)
                .Select(_ => (int)nextRequestId.Invoke(state, null))
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(int.MinValue, allocated[0]);
                Assert.AreEqual(int.MinValue + allocated.Length - 1, allocated[^1]);
                Assert.IsTrue(allocated.All(state.IsServiceOwnedRequestId));
                Assert.IsFalse(state.IsServiceOwnedRequestId(-2));
                Assert.IsNull(typeof(InteractiveBrokersFinancialAdvisorAccountState)
                    .GetField(
                        "_serviceOwnedRequestIds",
                        BindingFlags.Instance | BindingFlags.NonPublic));
                Assert.AreEqual(
                    (long)int.MinValue,
                    GetPrivateField<long>(
                        state,
                        "_serviceOwnedRequestIdEpochStart"));
                Assert.AreEqual(
                    (long)allocated[^1],
                    GetPrivateField<long>(
                        state,
                        "_serviceOwnedRequestIdCurrentMax"));
            });
        }

        [Test]
        public async Task QueuedScopeDuringUnkeyedTimeoutRemainsStaleTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var wireSent = new ManualResetEventSlim();
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                if (!authorize())
                {
                    return false;
                }
                scenario.Requests.Add("managed-timeout");
                wireSent.Set();
                return true;
            };
            using var state = scenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(100));

            var timeoutTask = RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(wireSent.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsTrue(state.RequestRefresh(
                Array.Empty<string>(), new[] { "ACC1" }));

            var timeout = await timeoutTask;
            Assert.IsTrue(SpinWait.SpinUntil(
                () => !HasQueuedRefresh(state) &&
                    GetPrivateField<object>(state, "_activeRefresh") == null,
                TimeSpan.FromSeconds(5)),
                "The queued refresh was not deterministically rejected after the timeout.");

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, timeout.Status);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                StringAssert.Contains(
                    "Timed out waiting for IB managed accounts",
                    state.Snapshot.ErrorMessage);
                CollectionAssert.AreEqual(
                    new[] { "managed-timeout" }, scenario.Requests);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            });
        }

        [Test]
        public async Task ReconnectBetweenCallbackAndNextInstallRejectsObsoleteScopeTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var reconnected = new ManualResetEventSlim();
            using var releaseOldAction = new ManualResetEventSlim();
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                if (!authorize())
                {
                    return false;
                }
                scenario.Requests.Add("managed-obsolete");
                scenario.Client.managedAccounts(scenario.ManagedAccounts);
                scenario.Client.connectionClosed();
                scenario.Client.nextValidId(456);
                reconnected.Set();
                if (!releaseOldAction.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException(
                        "The obsolete wire action was not released.");
                }
                return true;
            };
            using var state = scenario.CreateState();

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            try
            {
                Assert.IsTrue(reconnected.Wait(TimeSpan.FromSeconds(5)));
                scenario.Actions.RequestManagedAccounts = requestManagedAccounts;
                releaseOldAction.Set();
                state.NotifyBrokerageConnected();
                var recovered = await WaitForReadyGenerationAsync(state, 0);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                    CollectionAssert.AreEqual(
                        new[] { "managed-obsolete", "managed" },
                        scenario.Requests.Take(2));
                    Assert.AreEqual(1, recovered.Generation);
                });
            }
            finally
            {
                releaseOldAction.Set();
            }
        }

        [Test]
        public async Task InvalidationAtWireBoundaryPreventsSocketActionTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var boundaryReturned = new ManualResetEventSlim();
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                try
                {
                    scenario.Client.connectionClosed();
                    scenario.Client.nextValidId(789);
                    if (!authorize())
                    {
                        return false;
                    }
                    scenario.Requests.Add("unauthorized-managed-write");
                    scenario.Client.managedAccounts(scenario.ManagedAccounts);
                    return true;
                }
                finally
                {
                    scenario.Actions.RequestManagedAccounts = requestManagedAccounts;
                    boundaryReturned.Set();
                }
            };
            using var state = scenario.CreateState();

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(boundaryReturned.Wait(TimeSpan.FromSeconds(5)));
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(state, 0);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                CollectionAssert.DoesNotContain(
                    scenario.Requests, "unauthorized-managed-write");
            });
        }

        [Test]
        public async Task DisconnectDuringPacingDoesNotWriteOnFreshConnectionTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var paceEntered = new ManualResetEventSlim();
            using var releasePacing = new ManualResetEventSlim();
            var paceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                if (Interlocked.Increment(ref paceCalls) == 1)
                {
                    paceEntered.Set();
                    if (!releasePacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException("Test pacing was not released.");
                    }
                }
            });

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            try
            {
                Assert.IsTrue(paceEntered.Wait(TimeSpan.FromSeconds(5)));
                scenario.Client.connectionClosed();
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                scenario.Client.nextValidId(456);
            }
            finally
            {
                releasePacing.Set();
            }

            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(state, 0);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.AreEqual(2, scenario.Requests.Count(request => request == "managed"),
                    "The pre-disconnect pending request must not write on the fresh connection.");
            });
        }

        [Test]
        public async Task DisconnectDuringCancelDoesNotCancelOnFreshConnectionTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var cancelPaceEntered = new ManualResetEventSlim();
            using var releaseCancelPacing = new ManualResetEventSlim();
            var paceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                if (Interlocked.Increment(ref paceCalls) == 6)
                {
                    cancelPaceEntered.Set();
                    if (!releaseCancelPacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException("Test cancellation pacing was not released.");
                    }
                }
            });

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            var staleRequestId = 0;
            try
            {
                Assert.IsTrue(cancelPaceEntered.Wait(TimeSpan.FromSeconds(5)));
                staleRequestId = scenario.KeyedRequestIds.Single();
                scenario.Client.connectionClosed();
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                scenario.Client.nextValidId(789);
            }
            finally
            {
                releaseCancelPacing.Set();
            }

            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(state, 0);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.IsFalse(scenario.CanceledPositionIds.Contains(staleRequestId));
                Assert.AreEqual(1, scenario.CanceledPositionIds.Count,
                    "Only the recovered pass may send a position cancellation.");
            });
        }

        [Test]
        public async Task SupersededRefreshCancelsItsLivePositionsSubscriptionTest()
        {
            using var scenario = new Scenario();
            using var cancelEntered = new ManualResetEventSlim();
            using var releaseCancel = new ManualResetEventSlim();
            var canceledRequestId = 0;
            scenario.Actions.CancelPositions = (requestId, authorize) =>
            {
                canceledRequestId = requestId;
                cancelEntered.Set();
                if (!releaseCancel.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("Test cancellation was not released.");
                }
                return scenario.RunAuthorized(
                    authorize,
                    () => scenario.CanceledPositionIds.Add(requestId));
            };
            using var state = scenario.CreateState();

            Assert.IsTrue(state.RequestRefresh(new[] { "Alpha" }));
            try
            {
                Assert.IsTrue(cancelEntered.Wait(TimeSpan.FromSeconds(5)));
                Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            }
            finally
            {
                releaseCancel.Set();
            }

            var snapshot = await WaitForReadyGenerationAsync(state, 0);
            Assert.Multiple(() =>
            {
                Assert.IsTrue(snapshot.IsComplete);
                Assert.AreNotEqual(0, canceledRequestId);
                CollectionAssert.Contains(
                    scenario.CanceledPositionIds, canceledRequestId);
            });
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task FailedKeyedCancellationRetriesOnceOnNextSuccessfulWireTest(
            bool positions)
        {
            using var scenario = new Scenario();
            var originalCancel = positions
                ? scenario.Actions.CancelPositions
                : scenario.Actions.CancelAccountUpdates;
            var failedRequestId = 0;
            var attempts = 0;

            bool Cancel(int requestId, Func<bool> authorize)
            {
                if (failedRequestId == 0)
                {
                    failedRequestId = requestId;
                }
                if (requestId != failedRequestId)
                {
                    return originalCancel(requestId, authorize);
                }
                if (Interlocked.Increment(ref attempts) == 1)
                {
                    return scenario.RunAuthorized(
                        authorize,
                        () => throw new InvalidOperationException(
                            "simulated first cancellation failure"));
                }
                return originalCancel(requestId, authorize);
            }

            if (positions)
            {
                scenario.Actions.CancelPositions = Cancel;
            }
            else
            {
                scenario.Actions.CancelAccountUpdates = Cancel;
            }
            using var state = scenario.CreateState();
            var callbackStateLock =
                typeof(InteractiveBrokersFinancialAdvisorAccountState)
                    .GetField(
                        "_callbackStateLock",
                        BindingFlags.Instance | BindingFlags.NonPublic)
                    ?.GetValue(state);
            Assert.IsNotNull(callbackStateLock);
            var retryCalledUnderLock = false;
            scenario.ExternalCallProbe = () =>
                retryCalledUnderLock |= Monitor.IsEntered(callbackStateLock);

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var canceledRequestIds = positions
                ? scenario.CanceledPositionIds
                : scenario.CanceledAccountIds;

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, attempts);
                Assert.AreEqual(
                    1,
                    canceledRequestIds.Count(requestId => requestId == failedRequestId));
                Assert.AreEqual(1, scenario.MaximumConcurrentExternalCalls);
                Assert.IsFalse(
                    retryCalledUnderLock,
                    "A cancellation retry reached the socket delegate under the callback-state lock.");
            });
        }

        [Test]
        public async Task FailedKeyedCancellationRetryIsAttemptedOnlyOnceTest()
        {
            using var scenario = new Scenario();
            var originalCancel = scenario.Actions.CancelPositions;
            var failedRequestId = 0;
            var attempts = 0;
            scenario.Actions.CancelPositions = (requestId, authorize) =>
            {
                if (failedRequestId == 0)
                {
                    failedRequestId = requestId;
                }
                if (requestId != failedRequestId)
                {
                    return originalCancel(requestId, authorize);
                }
                Interlocked.Increment(ref attempts);
                return scenario.RunAuthorized(
                    authorize,
                    () => throw new InvalidOperationException(
                        "simulated persistent cancellation failure"));
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, attempts);
                CollectionAssert.DoesNotContain(
                    scenario.CanceledPositionIds, failedRequestId);
                Assert.Greater(
                    scenario.KeyedRequestIds.Count,
                    2,
                    "Later successful wire writes must not trigger a third retry.");
                Assert.AreEqual(1, scenario.MaximumConcurrentExternalCalls);
            });
        }

        [Test]
        public async Task PhysicalReconnectDiscardsFailedCancellationRequestIdTest()
        {
            using var scenario = Scenario.SingleAccount();
            using var reconnected = new ManualResetEventSlim();
            var originalCancel = scenario.Actions.CancelPositions;
            var originalAccountRequest = scenario.Actions.RequestAccountUpdates;
            var failedRequestId = 0;
            var attempts = 0;
            scenario.Actions.CancelPositions = (requestId, authorize) =>
            {
                if (failedRequestId == 0)
                {
                    failedRequestId = requestId;
                    Interlocked.Increment(ref attempts);
                    return scenario.RunAuthorized(
                        authorize,
                        () => throw new InvalidOperationException(
                            "simulated cancellation failure before reconnect"));
                }
                if (requestId == failedRequestId)
                {
                    Interlocked.Increment(ref attempts);
                }
                return originalCancel(requestId, authorize);
            };
            scenario.Actions.RequestAccountUpdates = (requestId, accountId, authorize) =>
            {
                scenario.Actions.RequestAccountUpdates = originalAccountRequest;
                scenario.Client.connectionClosed();
                scenario.Client.nextValidId(456);
                reconnected.Set();
                return originalAccountRequest(requestId, accountId, authorize);
            };
            using var state = scenario.CreateState();

            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(reconnected.Wait(TimeSpan.FromSeconds(5)));
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(state, 0);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.AreEqual(1, attempts);
                CollectionAssert.DoesNotContain(
                    scenario.CanceledPositionIds, failedRequestId);
                Assert.IsTrue(
                    scenario.CanceledPositionIds.All(requestId =>
                        requestId != failedRequestId));
            });
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task LogicalReconnectRetriesFailedCancellationOnceBeforeRefreshTest(
            bool recoveryBeforeRetention)
        {
            using var scenario = Scenario.SingleAccount();
            using var paceEntered = new ManualResetEventSlim();
            using var releasePacing = new ManualResetEventSlim();
            var sequence = new ConcurrentQueue<string>();
            var originalCancel = scenario.Actions.CancelPositions;
            var cancelInvocations = 0;
            var staleRequestId = 0;
            scenario.Actions.CancelPositions = (requestId, authorize) =>
            {
                if (staleRequestId == 0)
                {
                    staleRequestId = requestId;
                }
                if (requestId != staleRequestId)
                {
                    return originalCancel(requestId, authorize);
                }
                if (Interlocked.Increment(ref cancelInvocations) == 1)
                {
                    return scenario.RunAuthorized(
                        authorize,
                        () => throw new InvalidOperationException(
                            "simulated first cancellation failure"));
                }
                return originalCancel(requestId, () =>
                {
                    var authorized = authorize();
                    if (authorized)
                    {
                        sequence.Enqueue("cancellation-retry");
                    }
                    return authorized;
                });
            };
            var paceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                var call = Interlocked.Increment(ref paceCalls);
                if (call == (recoveryBeforeRetention ? 6 : 7))
                {
                    paceEntered.Set();
                    if (!releasePacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException(
                            "Test request pacing was not released.");
                    }
                }
            });

            var interrupted = RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            try
            {
                Assert.IsTrue(paceEntered.Wait(TimeSpan.FromSeconds(5)));
                scenario.Client.error(
                    -1,
                    0,
                    1100,
                    "Connectivity between IB and TWS was lost.",
                    string.Empty);
                scenario.Client.error(
                    -1,
                    0,
                    1102,
                    "Connectivity between IB and TWS was restored.",
                    string.Empty);
                scenario.Client.error(
                    -1,
                    0,
                    1102,
                    "Duplicate connectivity-restored notification.",
                    string.Empty);
            }
            finally
            {
                releasePacing.Set();
            }

            await interrupted;
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            scenario.Actions.RequestManagedAccounts = authorize =>
                requestManagedAccounts(() =>
                {
                    var authorized = authorize();
                    if (authorized)
                    {
                        sequence.Enqueue("recovered-refresh");
                    }
                    return authorized;
                });
            var recovered = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.AreEqual(2, cancelInvocations);
                Assert.AreEqual(
                    1,
                    scenario.CanceledPositionIds.Count(
                        requestId => requestId == staleRequestId));
                CollectionAssert.AreEqual(
                    new[] { "cancellation-retry", "recovered-refresh" },
                    sequence.Take(2));
                Assert.IsTrue(state.IsServiceOwnedRequestId(staleRequestId));
            });
        }

        [Test]
        public async Task QueuedScopesAreMergedAndCompleteDiscoveryDominatesTest()
        {
            using var scenario = new Scenario();
            using var paceEntered = new ManualResetEventSlim();
            using var releasePacing = new ManualResetEventSlim();
            var paceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                if (Interlocked.Increment(ref paceCalls) == 1)
                {
                    paceEntered.Set();
                    if (!releasePacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException("Test pacing was not released.");
                    }
                }
            });

            var refresh = RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Alpha" }));
            try
            {
                Assert.IsTrue(paceEntered.Wait(TimeSpan.FromSeconds(5)));
                var version = GetRequestVersion(state);
                Assert.IsTrue(state.RequestRefresh(new[] { "Alpha" }));
                Assert.AreEqual(version, GetRequestVersion(state));
                Assert.IsFalse(HasQueuedRefresh(state));

                Assert.IsTrue(state.RequestRefresh(
                    new[] { "Beta" }, new[] { "ACC3" }));
                Assert.IsTrue(state.RequestRefresh(new[] { "Alpha" }));

                var partial = GetQueuedScope(state);
                CollectionAssert.AreEqual(new[] { "Beta", "Alpha" }, partial.Groups);
                CollectionAssert.AreEqual(new[] { "ACC3" }, partial.AdditionalAccounts);
                Assert.IsFalse(partial.CompleteDiscovery);

                Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
                var complete = GetQueuedScope(state);
                CollectionAssert.AreEqual(new[] { "Beta", "Alpha" }, complete.Groups);
                CollectionAssert.AreEqual(new[] { "ACC3" }, complete.AdditionalAccounts);
                Assert.IsTrue(complete.CompleteDiscovery);
            }
            finally
            {
                releasePacing.Set();
            }

            var snapshot = await refresh;
            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.IsTrue(snapshot.IsComplete);
                CollectionAssert.AreEquivalent(
                    new[] { "Alpha", "Beta" }, snapshot.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    new[] { "ACC1", "ACC2", "ACC3" }, snapshot.Accounts.Keys);
            });
        }

        [Test]
        public async Task FullChannelDoesNotAdvanceVersionOrOrphanActiveRefreshTest()
        {
            using var scenario = new Scenario();
            using var paceEntered = new ManualResetEventSlim();
            using var releasePacing = new ManualResetEventSlim();
            var paceCalls = 0;
            using var state = scenario.CreateState(paceRequest: () =>
            {
                if (Interlocked.Increment(ref paceCalls) == 1)
                {
                    paceEntered.Set();
                    if (!releasePacing.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException("Test pacing was not released.");
                    }
                }
            });

            var activeRefresh = RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Alpha" }));
            try
            {
                Assert.IsTrue(paceEntered.Wait(TimeSpan.FromSeconds(5)));
                FillRefreshQueue(state);
                var requestVersion = GetRequestVersion(state);

                Assert.IsFalse(state.RequestRefresh(new[] { "Beta" }));
                Assert.AreEqual(requestVersion, GetRequestVersion(state));
            }
            finally
            {
                releasePacing.Set();
            }

            var snapshot = await activeRefresh;
            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEqual(new[] { "Alpha" }, snapshot.Groups.Keys);
            });
        }

        [Test]
        public async Task UnkeyedGlobalErrorsAreIgnoredTest()
        {
            using var scenario = Scenario.SingleAccount();
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                scenario.Client.error(
                    -1, 0, 399, "Unrelated global warning.", string.Empty);
                scenario.Client.error(
                    0, 0, 321, "Unrelated request-zero warning.", string.Empty);
                return requestManagedAccounts(authorize);
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
        }

        [Test]
        public async Task TimedOutPositionsRequestRetriesOnceWithFreshIdTest()
        {
            using var scenario = Scenario.SingleAccount();
            var clock = Stopwatch.StartNew();
            var firstRequestId = 0;
            var requestCount = 0;
            var firstCancellationElapsed = TimeSpan.Zero;
            var retryRequestElapsed = TimeSpan.Zero;
            scenario.Actions.RequestPositions =
                (requestId, accountOrGroup, authorize) =>
                    scenario.RunAuthorized(authorize, () =>
                    {
                        scenario.Requests.Add($"positions:{accountOrGroup}");
                        scenario.KeyedRequestIds.Add(requestId);
                        if (Interlocked.Increment(ref requestCount) == 1)
                        {
                            firstRequestId = requestId;
                            return;
                        }

                        retryRequestElapsed = clock.Elapsed;
                        scenario.Client.positionMulti(
                            firstRequestId,
                            accountOrGroup,
                            string.Empty,
                            Scenario.MappedContract(),
                            99m,
                            42);
                        scenario.Client.positionMultiEnd(firstRequestId);
                        scenario.Client.positionMulti(
                            requestId,
                            accountOrGroup,
                            string.Empty,
                            Scenario.MappedContract(),
                            1.25m,
                            42);
                        scenario.Client.positionMultiEnd(requestId);
                    });
            scenario.Actions.CancelPositions = (requestId, authorize) =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.CanceledPositionIds.Add(requestId);
                    if (requestId == firstRequestId)
                    {
                        firstCancellationElapsed = clock.Elapsed;
                    }
                });
            using var state = scenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(50));

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var positionRequestIds = scenario.KeyedRequestIds
                .Except(scenario.AccountRequestIds)
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, positionRequestIds.Length);
                Assert.AreEqual(2, positionRequestIds.Distinct().Count());
                CollectionAssert.AreEqual(
                    positionRequestIds,
                    scenario.CanceledPositionIds);
                Assert.GreaterOrEqual(
                    retryRequestElapsed - firstCancellationElapsed,
                    TimeSpan.FromMilliseconds(200));
                Assert.AreEqual(
                    1.25m,
                    snapshot.Accounts["ACC1"].Positions.Single().Quantity,
                    "Late rows from the canceled request must not enter the retry result.");
            });
        }

        [Test]
        public async Task PositionsWireTimeoutIsNotRetriedTest()
        {
            using var scenario = Scenario.SingleAccount();
            var attempts = 0;
            var attemptedRequestId = 0;
            scenario.Actions.RequestPositions =
                (requestId, accountOrGroup, authorize) =>
                    scenario.RunAuthorized(authorize, () =>
                    {
                        attemptedRequestId = requestId;
                        Interlocked.Increment(ref attempts);
                        throw new TimeoutException("simulated positions wire timeout");
                    });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains(
                    "simulated positions wire timeout",
                    snapshot.ErrorMessage);
                Assert.AreEqual(1, attempts);
                CollectionAssert.AreEqual(
                    new[] { attemptedRequestId },
                    scenario.CanceledPositionIds);
            });
        }

        [Test]
        public async Task FaRowsRequireOwnedRequestIdTest()
        {
            using var positionScenario = Scenario.SingleAccount();
            positionScenario.Actions.RequestPositions =
                (requestId, accountOrGroup, authorize) =>
            {
                if (!authorize())
                {
                    return false;
                }
                positionScenario.Requests.Add($"positions:{accountOrGroup}");
                positionScenario.KeyedRequestIds.Add(requestId);
                positionScenario.Client.positionMulti(
                    requestId + 1,
                    accountOrGroup,
                    string.Empty,
                    Scenario.MappedContract(),
                    9.5m,
                    42);
                positionScenario.Client.positionMultiEnd(requestId + 1);
                return true;
            };
            using var positionState = positionScenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(100));

            var positionSnapshot = await RunRefreshAsync(
                positionState,
                () => positionState.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, positionSnapshot.Status);
                StringAssert.Contains("Timed out waiting for IB positions for 'ACC1'",
                    positionSnapshot.ErrorMessage);
                Assert.AreEqual(2, positionScenario.KeyedRequestIds.Count);
                Assert.AreEqual(2, positionScenario.KeyedRequestIds.Distinct().Count());
                CollectionAssert.AreEqual(
                    positionScenario.KeyedRequestIds,
                    positionScenario.CanceledPositionIds);
                Assert.IsTrue(positionScenario.CanceledPositionIds.All(
                    positionState.IsServiceOwnedRequestId));
                Assert.IsEmpty(positionScenario.AccountRequestIds);
            });

            using var accountScenario = Scenario.SingleAccount();
            accountScenario.Actions.RequestAccountUpdates =
                (requestId, accountId, authorize) =>
            {
                if (!authorize())
                {
                    return false;
                }
                accountScenario.Requests.Add($"account:{accountId}");
                accountScenario.KeyedRequestIds.Add(requestId);
                accountScenario.AccountRequestIds.Add(requestId);
                accountScenario.Client.accountUpdateMulti(
                    requestId + 1,
                    accountId,
                    string.Empty,
                    "AccountReady",
                    "true",
                    string.Empty);
                accountScenario.Client.accountUpdateMultiEnd(requestId + 1);
                return true;
            };
            using var accountState = accountScenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(100));

            var accountSnapshot = await RunRefreshAsync(
                accountState,
                () => accountState.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, accountSnapshot.Status);
                StringAssert.Contains("Timed out waiting for IB account updates for 'ACC1'",
                    accountSnapshot.ErrorMessage);
                Assert.AreEqual(1, accountScenario.CanceledAccountIds.Count);
                Assert.AreEqual(accountScenario.AccountRequestIds.Single(),
                    accountScenario.CanceledAccountIds.Single());
                Assert.IsTrue(accountState.IsServiceOwnedRequestId(
                    accountScenario.CanceledAccountIds.Single()));
            });

            using var summaryScenario = Scenario.SingleAccount();
            summaryScenario.Actions.RequestAccountUpdates =
                (requestId, accountId, authorize) =>
                    summaryScenario.RunAuthorized(authorize, () =>
                    {
                        summaryScenario.Client.accountUpdateMulti(
                            requestId, "All", string.Empty,
                            "AccountReady", "false", string.Empty);
                        summaryScenario.Client.accountUpdateMulti(
                            requestId, "MASTER", string.Empty,
                            "NetLiquidation", "999999", "USD");
                        summaryScenario.Client.accountUpdateMulti(
                            requestId, "MASTERA", string.Empty,
                            "TotalCashValue", "999999", "USD");
                        summaryScenario.EmitAccountValues(requestId, accountId);
                    });
            using var summaryState = summaryScenario.CreateState();

            var summarySnapshot = await RunRefreshAsync(
                summaryState,
                () => summaryState.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready, summarySnapshot.Status);
                Assert.AreEqual(
                    1000.25m, summarySnapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(
                    250.50m, summarySnapshot.Accounts["ACC1"].TotalCashValue);
            });
        }

        [Test]
        public async Task ScopedRefreshRetainsCompleteTopologyTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState(configuredGroup: "Alpha");

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.IsFalse(snapshot.IsComplete);
                CollectionAssert.AreEqual(new[] { "Alpha" }, snapshot.Groups.Keys);
                CollectionAssert.AreEquivalent(new[] { "Alpha", "Beta" },
                    snapshot.AllGroups.Keys);
                CollectionAssert.AreEqual(new[] { "ACC1" }, snapshot.Accounts.Keys);
                CollectionAssert.AreEqual(new[] { "ACC3" }, snapshot.UnassignedAccountIds);
                CollectionAssert.AreEquivalent(
                    new[] { "MASTER", "ACC1", "ACC2", "ACC3" },
                    snapshot.AccountDirectory.Keys);
                CollectionAssert.AreEqual(new[] { "positions:Alpha" },
                    scenario.Requests.Where(request =>
                        request.StartsWith("positions:", StringComparison.Ordinal)));
                CollectionAssert.AreEqual(new[] { "account:ACC1" },
                    scenario.Requests.Where(request =>
                        request.StartsWith("account:", StringComparison.Ordinal)));
            });
        }

        [Test]
        public async Task AdditionalScopeAcceptsSelectedAndMovedManagedAccountsTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(
                    new[] { "Alpha" },
                    new[] { "ACC1", "ACC2" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEqual(new[] { "Alpha" }, snapshot.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    new[] { "ACC1", "ACC2" }, snapshot.Accounts.Keys);
                CollectionAssert.AreEqual(
                    new[] { "positions:Alpha", "positions:ACC2" },
                    scenario.Requests.Where(request =>
                        request.StartsWith("positions:", StringComparison.Ordinal)));
                Assert.AreEqual(
                    1,
                    scenario.Requests.Count(request => request == "account:ACC1"));
                Assert.AreEqual(
                    1,
                    scenario.Requests.Count(request => request == "account:ACC2"));
            });
        }

        [TestCase("MASTER")]
        [TestCase("MASTERA")]
        [TestCase("UNKNOWN")]
        public async Task AdditionalScopeRejectsAccountsOutsideManagedChildrenTest(
            string accountId)
        {
            using var scenario = Scenario.SingleAccount();
            if (accountId == "MASTERA")
            {
                scenario.ManagedAccounts = "MASTER,MASTERA,ACC1";
            }
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(
                    Array.Empty<string>(),
                    new[] { accountId }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains(
                    $"Account '{accountId}' is not a managed Financial Advisor subaccount.",
                    snapshot.ErrorMessage);
            });
        }

        [Test]
        public void ConfiguredGroupFilterStillRejectsAdditionalAccountScopeTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState(configuredGroup: "Alpha");

            var exception = Assert.Throws<InvalidOperationException>(() =>
                state.RequestRefresh(new[] { "Alpha" }, new[] { "ACC2" }));

            StringAssert.Contains(
                "Additional account collection is unavailable",
                exception.Message);
        }

        [Test]
        public async Task ScopedRefreshRejectsUnsupportedSavedMethodOnlyWhenTargetedTest()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>ACC1</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>Monetary</name>
                    <defaultMethod>MonetaryAmount</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>SavedPctChange</name>
                    <defaultMethod>PctChange</defaultMethod>
                    <ListOfAccts><String>ACC3</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = groups,
                EndingGroupsDocument = groups
            };
            using var state = scenario.CreateState(configuredGroup: "Alpha");

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var supportedOrder = new IBApi.Order { FaGroup = "Alpha" };
            var unsupportedOrder = new IBApi.Order { FaGroup = "Monetary" };
            var savedPctChangeOrder = new IBApi.Order { FaGroup = "SavedPctChange" };

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEqual(new[] { "Alpha" }, snapshot.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    new[] { "Alpha", "Monetary", "SavedPctChange" },
                    snapshot.AllGroups.Keys);
                CollectionAssert.IsSubsetOf(
                    new[] { "ACC1", "ACC2", "ACC3" }, snapshot.AccountDirectory.Keys);
                CollectionAssert.Contains(
                    snapshot.AccountDirectory["ACC3"].GroupNames,
                    "SavedPctChange");
                Assert.DoesNotThrow(() =>
                    InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                        supportedOrder,
                        snapshot));
                StringAssert.Contains(
                    "unsupported saved allocation method 'MonetaryAmount'",
                    Assert.Throws<NotSupportedException>(() =>
                        InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                            unsupportedOrder,
                            snapshot)).Message);
                StringAssert.Contains(
                    "not supported",
                    Assert.Throws<NotSupportedException>(() =>
                        InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                            savedPctChangeOrder,
                            snapshot)).Message);
                savedPctChangeOrder.FaMethod = "PctChange";
                savedPctChangeOrder.FaPercentage = "25";
                Assert.Throws<NotSupportedException>(() =>
                    InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                        savedPctChangeOrder,
                        snapshot));
            });
        }

        [Test]
        public async Task CompleteDiscoveryPublishesUnsupportedSavedMethodsTest()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>ACC1</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>Monetary</name>
                    <defaultMethod>MonetaryAmount</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>SavedPctChange</name>
                    <defaultMethod>PctChange</defaultMethod>
                    <ListOfAccts><String>ACC3</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = groups,
                EndingGroupsDocument = groups
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.IsTrue(snapshot.IsComplete);
                CollectionAssert.AreEquivalent(
                    new[] { "Alpha", "Monetary", "SavedPctChange" },
                    snapshot.AllGroups.Keys);
                CollectionAssert.AreEquivalent(
                    snapshot.AllGroups.Keys,
                    snapshot.Groups.Keys);
                Assert.IsNull(state.UnsupportedConfigurationError);
                Assert.DoesNotThrow(() =>
                    InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                        new IBApi.Order { FaGroup = "Alpha" },
                        snapshot));
                Assert.Throws<NotSupportedException>(() =>
                    InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                        new IBApi.Order { FaGroup = "Monetary" },
                        snapshot));
                StringAssert.Contains(
                    "not supported",
                    Assert.Throws<NotSupportedException>(() =>
                        InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                            new IBApi.Order { FaGroup = "SavedPctChange" },
                            snapshot)).Message);
            });
        }

        [Test]
        public async Task BlankConfiguredFilterScopesRelationshipValidationTest()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>ACC1</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>Monetary</name>
                    <defaultMethod>MonetaryAmount</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>External</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts><String>NOT_MANAGED</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = groups,
                EndingGroupsDocument = groups
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.IsFalse(snapshot.IsComplete);
                CollectionAssert.AreEqual(new[] { "Alpha" }, snapshot.Groups.Keys);
                CollectionAssert.AreEquivalent(
                    new[] { "Alpha", "Monetary", "External" },
                    snapshot.AllGroups.Keys);
                Assert.AreEqual(
                    BrokerageAccountRelationship.Unknown,
                    snapshot.AccountDirectory["NOT_MANAGED"].Relationship);
                Assert.DoesNotThrow(() =>
                    InteractiveBrokersBrokerage.ValidateFinancialAdvisorAllocationMethod(
                        new IBApi.Order { FaGroup = "Alpha" },
                        snapshot));
            });
        }

        [Test]
        public async Task NoExternalCallUnderSynchronizationTest()
        {
            using var scenario = new Scenario();
            object callbackStateLock = null;
            var callObservedUnderLock = false;
            scenario.ExternalCallProbe = () =>
            {
                if (callbackStateLock != null)
                {
                    callObservedUnderLock |= Monitor.IsEntered(callbackStateLock);
                }
            };
            using var state = scenario.CreateState(
                paceRequest: scenario.ExternalCallProbe,
                mapSymbol: contract =>
                {
                    scenario.ExternalCallProbe();
                    return contract.Symbol == "UNMAPPED"
                        ? throw new InvalidOperationException("No LEAN symbol mapping.")
                        : Symbol.Create(contract.Symbol, SecurityType.Equity, Market.USA);
                });
            callbackStateLock = typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_callbackStateLock",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);
            Assert.IsNotNull(callbackStateLock);

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
            Assert.IsFalse(callObservedUnderLock,
                "An injected request, pacing, mapping, or publication callback ran under the callback-state lock.");
        }

        [TestCase("MASTER")]
        [TestCase("MASTERA")]
        [TestCase("NOT_MANAGED")]
        public async Task SemanticTopologyFailureIsBrokerageExceptionTest(
            string invalidAccountId)
        {
            var invalidGroup = $"""
                <ListOfGroups>
                  <Group>
                    <name>Invalid</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>{invalidAccountId}</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = Scenario.SingleAccount();
            scenario.GroupsDocument = invalidGroup;
            scenario.EndingGroupsDocument = invalidGroup;
            string reported = null;
            object callbackStateLock = null;
            var reporterCalledUnderLock = false;
            using var state = scenario.CreateState(
                reportUnsupported: message =>
                {
                    reporterCalledUnderLock = callbackStateLock != null &&
                        Monitor.IsEntered(callbackStateLock);
                    reported = message;
                });
            callbackStateLock = typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_callbackStateLock",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);
            Assert.IsNotNull(callbackStateLock);

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                Assert.AreEqual(snapshot.ErrorMessage, reported);
                Assert.AreEqual(snapshot.ErrorMessage, state.UnsupportedConfigurationError);
                Assert.IsFalse(reporterCalledUnderLock);
                StringAssert.Contains("not a managed subaccount", snapshot.ErrorMessage);
                StringAssert.Contains(invalidAccountId, snapshot.ErrorMessage);
                StringAssert.Contains("Correct the group membership in TWS",
                    snapshot.ErrorMessage);
                Assert.IsTrue(typeof(InteractiveBrokersFinancialAdvisorAccountState
                    .UnsupportedFinancialAdvisorConfigurationException)
                    .IsSubclassOf(typeof(InvalidOperationException)));
                Assert.IsFalse(scenario.Requests.Any(request =>
                    request.StartsWith("positions:", StringComparison.Ordinal) ||
                    request.StartsWith("account:", StringComparison.Ordinal)));
            });
        }

        [Test]
        public async Task MalformedTopologyLatchPersistsUntilReadyTest()
        {
            const string malformedGroups = """
                <ListOfGroups>
                  <Group>
                    <name>Malformed</name>
                    <ListOfAccts><String>ACC1</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
            """;
            using var scenario = Scenario.SingleAccount();
            scenario.GroupsDocument = malformedGroups;
            scenario.EndingGroupsDocument = malformedGroups;
            var reports = new List<string>();
            using var state = scenario.CreateState(reportUnsupported: reports.Add);

            var unsupported = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var unsupportedReason = unsupported.ErrorMessage;

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, unsupported.Status);
                Assert.AreEqual(unsupportedReason, state.UnsupportedConfigurationError);
                StringAssert.Contains("contained no allocation method", unsupportedReason);
                CollectionAssert.AreEqual(new[] { unsupportedReason }, reports);
            });

            scenario.Client.connectionClosed();
            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                Assert.AreEqual(unsupportedReason, state.UnsupportedConfigurationError);
            });
            scenario.Client.nextValidId(123);
            scenario.GroupsDocument = Scenario.EmptyGroupsXml;
            scenario.EndingGroupsDocument = Scenario.EmptyGroupsXml;

            using var refreshStarted = new ManualResetEventSlim();
            using var releaseRefresh = new ManualResetEventSlim();
            var requestManagedAccounts = scenario.Actions.RequestManagedAccounts;
            var requestPositions = scenario.Actions.RequestPositions;
            scenario.Actions.RequestManagedAccounts = authorize =>
            {
                refreshStarted.Set();
                if (!releaseRefresh.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("The ordinary refresh was not released.");
                }
                return requestManagedAccounts(authorize);
            };
            scenario.Actions.RequestPositions = (requestId, accountOrGroup, authorize) =>
                scenario.RunAuthorized(authorize, () =>
                    throw new InvalidOperationException(
                        "simulated ordinary refresh failure"));

            var ordinaryTask = RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            Assert.IsTrue(refreshStarted.Wait(TimeSpan.FromSeconds(5)));
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Refreshing,
                    state.Snapshot.Status);
                Assert.AreEqual(unsupportedReason, state.UnsupportedConfigurationError);
            });
            releaseRefresh.Set();
            var ordinaryFailure = await ordinaryTask;

            Assert.Multiple(() =>
            {
                StringAssert.Contains(
                    "simulated ordinary refresh failure",
                    ordinaryFailure.ErrorMessage);
                Assert.AreEqual(unsupportedReason, state.UnsupportedConfigurationError);
            });

            scenario.Actions.RequestManagedAccounts = requestManagedAccounts;
            scenario.Actions.RequestPositions = requestPositions;
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, ready.Status);
                Assert.IsNull(state.UnsupportedConfigurationError);
                CollectionAssert.AreEqual(new[] { unsupportedReason }, reports);
            });
        }

        [TestCase("ManagedAccounts")]
        [TestCase("Aliases")]
        [TestCase("FamilyCodes")]
        [TestCase("GroupMembership")]
        [TestCase("AllocationConfiguration")]
        public async Task ReadySnapshotRequiresStableTopologyTest(string changedSource)
        {
            const string ratioGroups = """
                <ListOfGroups>
                  <Group><name>Ratio</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account><acct>ACC1</acct><amount>1</amount></Account>
                    <Account><acct>ACC2</acct><amount>2</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;
            const string changedRatioGroups = """
                <ListOfGroups>
                  <Group><name>Ratio</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account><acct>ACC1</acct><amount>1</amount></Account>
                    <Account><acct>ACC2</acct><amount>3</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario();
            switch (changedSource)
            {
                case "ManagedAccounts":
                    scenario.EndingManagedAccounts = "MASTER,ACC1,ACC2,ACC3,ACC4";
                    break;
                case "Aliases":
                    scenario.EndingAliasesDocument = """
                        <ListOfAccountAliases>
                          <AccountAlias><account>ACC1</account><alias>Changed Client</alias></AccountAlias>
                          <AccountAlias><account>ACC2</account><alias>Beta Client</alias></AccountAlias>
                          <AccountAlias><account>ACC3</account><alias>Unassigned Client</alias></AccountAlias>
                        </ListOfAccountAliases>
                        """;
                    break;
                case "FamilyCodes":
                    scenario.EndingFamilyCodes =
                    [
                        new() { AccountID = "ACC1", FamilyCodeStr = "Changed-Family" },
                        new() { AccountID = "ACC2", FamilyCodeStr = "Family-B" },
                        new() { AccountID = "ACC3", FamilyCodeStr = "Family-C" }
                    ];
                    break;
                case "GroupMembership":
                    scenario.EndingGroupsDocument = Scenario.GroupsXml.Replace(
                        "<String>ACC1</String>",
                        "<String>ACC3</String>",
                        StringComparison.Ordinal);
                    break;
                case "AllocationConfiguration":
                    scenario.GroupsDocument = ratioGroups;
                    scenario.EndingGroupsDocument = changedRatioGroups;
                    break;
                default:
                    Assert.Fail($"Unexpected topology source '{changedSource}'.");
                    break;
            }
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                Assert.IsFalse(snapshot.IsReady);
                Assert.AreEqual(0, snapshot.Generation);
                Assert.AreEqual(2, scenario.GroupsRequestCount);
                StringAssert.Contains("topology changed while the account snapshot was collected",
                    snapshot.ErrorMessage);
            });
        }

        [Test]
        public async Task EndingGroupsRereadDiscardsPreWireCallbackTest()
        {
            using var scenario = new Scenario
            {
                EndingGroupsDocument = Scenario.GroupsXml.Replace(
                    "<String>ACC1</String>",
                    "<String>ACC3</String>",
                    StringComparison.Ordinal)
            };
            var financialAdvisor = scenario.Actions.RequestFinancialAdvisor;
            var groupsActionCount = 0;
            scenario.Actions.RequestFinancialAdvisor = (faDataType, authorize) =>
            {
                if (faDataType == 1 && ++groupsActionCount == 2)
                {
                    scenario.Client.receiveFA(1, Scenario.GroupsXml);
                }
                return financialAdvisor(faDataType, authorize);
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                Assert.IsFalse(snapshot.IsReady);
                Assert.AreEqual(0, snapshot.Generation);
                Assert.AreEqual(2, groupsActionCount);
                Assert.AreEqual(2, scenario.GroupsRequestCount);
                StringAssert.Contains(
                    "topology changed while the account snapshot was collected",
                    snapshot.ErrorMessage);
            });
        }

        [Test]
        public async Task EquivalentTopologyFormattingDoesNotProduceFalseDriftTest()
        {
            using var scenario = new Scenario
            {
                EndingManagedAccounts = " acc3, ACC2,master,acc1 ",
                EndingGroupsDocument = """
                    <ListOfGroups>
                      <Group>
                        <name>Beta</name><defaultMethod>Equal</defaultMethod>
                        <ListOfAccts><String>ACC2</String></ListOfAccts>
                      </Group>
                      <Group>
                        <name>Alpha</name><defaultMethod>NetLiq</defaultMethod>
                        <ListOfAccts><String>ACC1</String></ListOfAccts>
                      </Group>
                    </ListOfGroups>
                    """,
                EndingAliasesDocument = """
                    <ListOfAccountAliases>
                      <AccountAlias><account>acc3</account><alias> Unassigned Client </alias></AccountAlias>
                      <AccountAlias><account>acc2</account><alias> Beta Client </alias></AccountAlias>
                      <AccountAlias><account>acc1</account><alias> Alpha Client </alias></AccountAlias>
                    </ListOfAccountAliases>
                    """,
                EndingFamilyCodes =
                [
                    new() { AccountID = "acc3", FamilyCodeStr = " Family-C " },
                    new() { AccountID = "acc2", FamilyCodeStr = " Family-B " },
                    new() { AccountID = "acc1", FamilyCodeStr = " Family-A " }
                ]
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, snapshot.Generation);
                Assert.IsTrue(snapshot.IsComplete);
            });
        }

        [Test]
        public async Task TopologyDriftAfterReadyPreservesLastGoodSnapshotTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState();
            var ready = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            scenario.EndingAliasesDocument = """
                <ListOfAccountAliases>
                  <AccountAlias><account>ACC1</account><alias>Changed Client</alias></AccountAlias>
                  <AccountAlias><account>ACC2</account><alias>Beta Client</alias></AccountAlias>
                  <AccountAlias><account>ACC3</account><alias>Unassigned Client</alias></AccountAlias>
                </ListOfAccountAliases>
                """;

            var stale = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, ready.Status);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, stale.Status);
                Assert.AreEqual(ready.Generation, stale.Generation);
                Assert.AreEqual(
                    ready.LastSuccessfulUpdateUtc,
                    stale.LastSuccessfulUpdateUtc);
                Assert.AreEqual(ready.MembershipHash, stale.MembershipHash);
                Assert.AreEqual(
                    ready.GroupConfigurationVersion,
                    stale.GroupConfigurationVersion);
                CollectionAssert.AreEquivalent(ready.Groups.Keys, stale.Groups.Keys);
                CollectionAssert.AreEquivalent(ready.Accounts.Keys, stale.Accounts.Keys);
                Assert.AreEqual(
                    ready.Accounts["ACC1"].NetLiquidation,
                    stale.Accounts["ACC1"].NetLiquidation);
            });
        }

        [Test]
        public async Task FractionalAndUnmappedPositionsArePreservedTest()
        {
            using var scenario = new Scenario();
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var mapped = snapshot.Accounts["ACC1"].Positions.Single();
            var unmapped = snapshot.Accounts["ACC2"].UnmappedPositions.Single();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1.25m, mapped.Quantity);
                Assert.AreEqual(100.5m, mapped.AveragePrice);
                Assert.AreEqual(2.75m, unmapped.Quantity);
                Assert.AreEqual(12.345m, unmapped.AveragePrice);
                Assert.AreEqual("UNMAPPED", unmapped.BrokerageSymbol);
                Assert.IsTrue(snapshot.HasUnmappedPositions);
            });
        }

        [TestCase("EUR", "SBF")]
        [TestCase("USD", "SBF")]
        [TestCase("EUR", "NYSE")]
        [TestCase("USD", "")]
        public async Task ProductionMapperPreservesMismatchedEquityAsUnmappedTest(
            string foreignCurrency,
            string foreignPrimaryExchange)
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var mapFileProvider = new AirMapFileProvider();
            var brokerageType = typeof(InteractiveBrokersBrokerage);
            brokerageType.GetField("_symbolMapper",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.SetValue(brokerage, new InteractiveBrokersSymbolMapper(mapFileProvider));
            brokerageType.GetField("_exchangeProvider",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.SetValue(brokerage, new MapFilePrimaryExchangeProvider(mapFileProvider));
            var mapMethod = brokerageType.GetMethod(
                "MapFinancialAdvisorPositionSymbol",
                BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(mapMethod);
            var mapSymbol = (Func<Contract, Symbol>)Delegate.CreateDelegate(
                typeof(Func<Contract, Symbol>), brokerage, mapMethod);

            using var scenario = new Scenario();
            scenario.Actions.RequestPositions = (requestId, accountOrGroup, authorize) =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add($"positions:{accountOrGroup}");
                    scenario.KeyedRequestIds.Add(requestId);
                    if (accountOrGroup == "Alpha")
                    {
                        scenario.Client.positionMulti(
                            requestId,
                            "ACC1",
                            "Foreign",
                            CreateAirContract(901, foreignCurrency, foreignPrimaryExchange),
                            5m,
                            42.25);
                    }
                    else if (accountOrGroup == "Beta")
                    {
                        scenario.Client.positionMulti(
                            requestId,
                            "ACC2",
                            "US",
                            CreateAirContract(902, Currencies.USD, "NYSE"),
                            7m,
                            81.5);
                    }
                    scenario.Client.positionMultiEnd(requestId);
                });
            using var state = scenario.CreateState(mapSymbol: mapSymbol);

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var foreignAccount = snapshot.Accounts["ACC1"];
            var usAccount = snapshot.Accounts["ACC2"];
            var unmapped = foreignAccount.UnmappedPositions.Single();
            var mapped = usAccount.Positions.Single();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.IsTrue(snapshot.IsComplete);
                Assert.IsTrue(snapshot.HasUnmappedPositions);
                Assert.IsEmpty(foreignAccount.Positions);
                Assert.IsEmpty(usAccount.UnmappedPositions);
                Assert.AreEqual("AIR", mapped.Symbol.Value);
                Assert.AreEqual(Market.USA, mapped.Symbol.ID.Market);
                Assert.AreEqual(7m, mapped.Quantity);
                Assert.AreEqual(81.5m, mapped.AveragePrice);
                Assert.AreEqual("US", mapped.ModelCode);
                Assert.AreEqual("901", unmapped.BrokerageContractId);
                Assert.AreEqual("AIR", unmapped.BrokerageSymbol);
                Assert.AreEqual("AIR", unmapped.LocalSymbol);
                Assert.AreEqual("STK", unmapped.BrokerageSecurityType);
                Assert.AreEqual(foreignCurrency, unmapped.Currency);
                Assert.AreEqual("SMART", unmapped.Exchange);
                Assert.AreEqual(foreignPrimaryExchange, unmapped.PrimaryExchange);
                Assert.AreEqual("AIR", unmapped.TradingClass);
                Assert.AreEqual("1", unmapped.Multiplier);
                Assert.AreEqual(5m, unmapped.Quantity);
                Assert.AreEqual(42.25m, unmapped.AveragePrice);
                Assert.AreEqual("Foreign", unmapped.ModelCode);
                StringAssert.Contains("does not match mapped LEAN symbol", unmapped.ErrorMessage);
            });
        }

        [Test]
        [NonParallelizable]
        public async Task GroupAccountNameCollisionFallsBackToExactPositionsTest()
        {
            const string collisionGroups = """
                <ListOfGroups>
                  <Group>
                    <name>acc1</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts>
                      <String>ACC2</String>
                      <String>ACC3</String>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts><String>ACC3</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario
                {
                    GroupsDocument = collisionGroups,
                    EndingGroupsDocument = collisionGroups
                };
                scenario.Actions.RequestPositions =
                    (requestId, accountOrGroup, authorize) =>
                        scenario.RunAuthorized(authorize, () =>
                        {
                            scenario.Requests.Add($"positions:{accountOrGroup}");
                            scenario.KeyedRequestIds.Add(requestId);
                            if (accountOrGroup == "Beta")
                            {
                                scenario.Client.positionMulti(
                                    requestId,
                                    "ACC3",
                                    "Model-B",
                                    Scenario.MappedContract(),
                                    3m,
                                    103d);
                            }
                            else if (accountOrGroup == "ACC2")
                            {
                                scenario.Client.positionMulti(
                                    requestId,
                                    "ACC2",
                                    "Model-A",
                                    Scenario.MappedContract(),
                                    2m,
                                    102d);
                            }
                            scenario.Client.positionMultiEnd(requestId);
                        });
                using var state = scenario.CreateState();

                var snapshot = await RunRefreshAsync(
                    state,
                    () => state.RequestRefresh(new[] { "ACC1", "Beta" }));
                var positionRequests = scenario.Requests.Where(request =>
                    request.StartsWith("positions:", StringComparison.Ordinal)).ToArray();
                var fallbackMessage = logHandler.Logs.Single(entry =>
                    entry.MessageType == LogType.Error && entry.Message.Contains(
                        "FA group 'acc1' conflicts with managed account 'ACC1'",
                        StringComparison.Ordinal)).Message;

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                    CollectionAssert.AreEqual(
                        new[] { "positions:Beta", "positions:ACC2" },
                        positionRequests);
                    Assert.AreEqual(
                        2m, snapshot.Accounts["ACC2"].Positions.Single().Quantity);
                    Assert.AreEqual(
                        3m, snapshot.Accounts["ACC3"].Positions.Single().Quantity);
                    Assert.IsNull(state.UnsupportedConfigurationError);
                    StringAssert.Contains(
                        "members not covered by another selected group will use " +
                        "per-account collection", fallbackMessage);
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        public async Task GroupAccountNameCollisionExactPositionsRejectForeignRowsTest()
        {
            const string collisionGroup = """
                <ListOfGroups>
                  <Group>
                    <name>aCc1</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = collisionGroup,
                EndingGroupsDocument = collisionGroup
            };
            scenario.Actions.RequestPositions =
                (requestId, accountOrGroup, authorize) =>
                    scenario.RunAuthorized(authorize, () =>
                    {
                        scenario.Requests.Add($"positions:{accountOrGroup}");
                        scenario.KeyedRequestIds.Add(requestId);
                        scenario.Client.positionMulti(
                            requestId,
                            "ACC3",
                            "Model-A",
                            Scenario.MappedContract(),
                            1m,
                            100d);
                        scenario.Client.positionMultiEnd(requestId);
                    });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "aCC1" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains(
                    "Position response for 'ACC2' contained account 'ACC3'",
                    snapshot.ErrorMessage);
                CollectionAssert.AreEqual(
                    new[] { "positions:ACC2" },
                    scenario.Requests.Where(request =>
                        request.StartsWith("positions:", StringComparison.Ordinal)));
                Assert.IsNull(state.UnsupportedConfigurationError);
            });
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task PositionRowsMustMatchTheirRequestedGroupOrAccountTest(
            bool groupRequest)
        {
            using var scenario = groupRequest ? new Scenario() : Scenario.SingleAccount();
            scenario.Actions.RequestPositions = (requestId, accountOrGroup, authorize) =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add($"positions:{accountOrGroup}");
                    scenario.KeyedRequestIds.Add(requestId);
                    scenario.Client.positionMulti(
                        requestId,
                        "ACC2",
                        "Model-A",
                        Scenario.MappedContract(),
                        1m,
                        100d);
                    scenario.Client.positionMultiEnd(requestId);
                });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(
                    groupRequest ? new[] { "Alpha" } : Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains(
                    groupRequest
                        ? "FA group 'Alpha' contained non-member account 'ACC2'"
                        : "Position response for 'ACC1' contained account 'ACC2'",
                    snapshot.ErrorMessage);
            });
        }

        [TestCase("mapped", "identical", BrokerageAccountSnapshotStatus.Ready)]
        [TestCase("mapped", "conflicting", BrokerageAccountSnapshotStatus.Failed)]
        [TestCase("mapped", "average-conflicting", BrokerageAccountSnapshotStatus.Failed)]
        [TestCase("mapped", "zero", BrokerageAccountSnapshotStatus.Ready)]
        [TestCase("unmapped", "identical", BrokerageAccountSnapshotStatus.Ready)]
        [TestCase("unmapped", "conflicting", BrokerageAccountSnapshotStatus.Failed)]
        [TestCase("unmapped", "average-conflicting", BrokerageAccountSnapshotStatus.Failed)]
        [TestCase("unmapped", "zero", BrokerageAccountSnapshotStatus.Ready)]
        public async Task DuplicateAndZeroPositionsFailClosedWithoutInflatingHoldingsTest(
            string mapping,
            string shape,
            BrokerageAccountSnapshotStatus expectedStatus)
        {
            var accountId = mapping == "mapped" ? "ACC1" : "ACC2";
            var groupName = mapping == "mapped" ? "Alpha" : "Beta";
            var contract = mapping == "mapped"
                ? Scenario.MappedContract()
                : Scenario.UnmappedContract();
            using var scenario = new Scenario();
            scenario.Actions.RequestPositions = (requestId, accountOrGroup, authorize) =>
                scenario.RunAuthorized(authorize, () =>
                {
                    scenario.Requests.Add($"positions:{accountOrGroup}");
                    scenario.KeyedRequestIds.Add(requestId);
                    var quantity = shape == "zero" ? 0m : 1.25m;
                    scenario.Client.positionMulti(
                        requestId, accountId, "Model-A", contract, quantity, 100.5d);
                    if (shape != "zero")
                    {
                        scenario.Client.positionMulti(
                            requestId,
                            accountId,
                            "Model-A",
                            contract,
                            shape == "conflicting" ? quantity + 1m : quantity,
                            shape == "average-conflicting" ? 101.5d : 100.5d);
                    }
                    scenario.Client.positionMultiEnd(requestId);
                });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { groupName }));

            Assert.AreEqual(expectedStatus, snapshot.Status);
            if (expectedStatus == BrokerageAccountSnapshotStatus.Failed)
            {
                StringAssert.Contains(
                    mapping == "mapped"
                        ? "conflicting duplicate positions"
                        : "conflicting duplicate unmapped positions",
                    snapshot.ErrorMessage);
                return;
            }

            var account = snapshot.Accounts[accountId];
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    shape == "zero" || mapping == "unmapped" ? 0 : 1,
                    account.Positions.Count);
                Assert.AreEqual(
                    shape == "zero" || mapping == "mapped" ? 0 : 1,
                    account.UnmappedPositions.Count);
            });
        }

        [Test]
        public async Task PublicCallbackPayloadMutationCannotAffectSnapshotTest()
        {
            static FamilyCode[] CreateFamilyCodes() =>
            [
                new() { AccountID = "ACC1", FamilyCodeStr = "Family-A" },
                new() { AccountID = "ACC2", FamilyCodeStr = "Family-B" },
                new() { AccountID = "ACC3", FamilyCodeStr = "Family-C" }
            ];
            using var scenario = new Scenario
            {
                FamilyCodes = CreateFamilyCodes(),
                EndingFamilyCodes = CreateFamilyCodes()
            };
            var positionCallbacks = 0;
            var familyCodeCallbacks = 0;
            scenario.Client.UpdatePortfolio += (_, args) =>
            {
                if (!args.PositionsMultiRequestId.HasValue || args.Contract == null)
                {
                    return;
                }
                positionCallbacks++;
                args.Contract.ConId = -1;
                args.Contract.Symbol = "PUBLIC_MUTATION";
                args.Contract.LocalSymbol = "PUBLIC_MUTATION";
                args.Contract.SecType = "FUT";
                args.Contract.Currency = "EUR";
                args.Contract.Exchange = "PUBLIC";
                args.Contract.PrimaryExch = "PUBLIC";
                args.Contract.TradingClass = "PUBLIC";
                args.Contract.LastTradeDateOrContractMonth = "19000101";
                args.Contract.Strike = 999d;
                args.Contract.Right = "P";
                args.Contract.Multiplier = "100";
            };
            scenario.Client.FamilyCodes += (_, args) =>
            {
                familyCodeCallbacks++;
                foreach (var familyCode in args.FamilyCodes)
                {
                    familyCode.AccountID = "PUBLIC_MUTATION";
                    familyCode.FamilyCodeStr = "PUBLIC_MUTATION";
                }
            };
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state,
                () => state.RequestRefresh(Array.Empty<string>()));
            var mapped = snapshot.Accounts["ACC1"].Positions.Single();
            var unmapped = snapshot.Accounts["ACC2"].UnmappedPositions.Single();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, positionCallbacks);
                Assert.AreEqual(2, familyCodeCallbacks);
                Assert.AreEqual("SPY", mapped.Symbol.Value);
                Assert.AreEqual("202", unmapped.BrokerageContractId);
                Assert.AreEqual("UNMAPPED", unmapped.BrokerageSymbol);
                Assert.AreEqual("USD", unmapped.Currency);
                Assert.AreEqual(
                    "Family-A",
                    snapshot.AccountDirectory["ACC1"].FamilyCode);
                Assert.AreEqual(
                    "Family-B",
                    snapshot.AccountDirectory["ACC2"].FamilyCode);
                CollectionAssert.DoesNotContain(
                    snapshot.AccountDirectory.Keys,
                    "PUBLIC_MUTATION");
            });
        }

        [Test]
        public async Task NamedGroupSummariesReplaceOnlyGroupMemberAccountUpdatesTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, groupName) =>
            {
                scenario.EmitValidSummaryAccount(
                    requestId, groupName == "Alpha" ? "ACC1" : "ACC2");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, scenario.SummaryRequests.Count);
                CollectionAssert.AreEqual(
                    scenario.SummaryRequests.Select(request => request.RequestId),
                    scenario.CanceledSummaryIds);
                CollectionAssert.AreEqual(
                    new[] { "ACC3" },
                    scenario.Requests.Where(request => request.StartsWith("account:"))
                        .Select(request => request[8..]));
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC2"].NetLiquidation);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC3"].NetLiquidation);
                Assert.AreEqual(350.50m, snapshot.Accounts["ACC1"].TotalCashValue);
                Assert.AreEqual(350.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
                Assert.IsFalse(snapshot.Accounts["ACC1"].CashBalances.ContainsKey("BASE"));
                Assert.AreEqual(1, scenario.MaximumConcurrentExternalCalls);
                Assert.IsTrue(scenario.SummaryRequests.All(request =>
                    request.RequestId < 0 && state.IsServiceOwnedRequestId(request.RequestId)));
                Assert.IsTrue(scenario.SummaryRequests.All(request => request.Tags ==
                    "AccountType,NetLiquidation,TotalCashValue,AvailableFunds," +
                    "ExcessLiquidity,BuyingPower,AccountReady,$LEDGER,$LEDGER:ALL"));
                Assert.Less(
                    scenario.Requests.IndexOf(
                        $"cancel-summary:{scenario.SummaryRequests[0].RequestId}"),
                    scenario.Requests.IndexOf("summary:Beta"));
            });
        }

        [TestCase("base", true)]
        [TestCase("concrete", true)]
        [TestCase("equal-pair", true)]
        [TestCase("divergent-pair", false)]
        [TestCase("duplicate-base", false)]
        [TestCase("three-rows", false)]
        [TestCase("invalid-value", false)]
        [TestCase("foreign", false)]
        [TestCase("blank", false)]
        public async Task BareLedgerCashShapesAreAcceptedOrFailClosedTest(
            string shape, bool fastPath)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                IReadOnlyCollection<(string Currency, string Value)> cashRows = shape switch
                {
                    "base" => new[] { ("BASE", "350.50") },
                    "concrete" => new[] { ("USD", "350.50") },
                    "equal-pair" => new[] { ("BASE", "350.50"), ("USD", "350.50") },
                    "divergent-pair" => new[] { ("BASE", "351.50"), ("USD", "350.50") },
                    "duplicate-base" => new[] { ("BASE", "350.50"), ("BASE", "350.50") },
                    "three-rows" => new[]
                    {
                        ("BASE", "350.50"), ("USD", "350.50"), ("EUR", "0")
                    },
                    "invalid-value" => new[] { ("BASE", "not-a-number") },
                    "foreign" => new[] { ("EUR", "350.50") },
                    "blank" => new[] { (string.Empty, "350.50") },
                    _ => throw new ArgumentOutOfRangeException(nameof(shape))
                };
                scenario.EmitValidSummaryAccount(requestId, "ACC1", cashRows: cashRows);
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(fastPath ? 1100.25m : 1000.25m,
                    snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(fastPath ? 0 : 1, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                Assert.AreEqual(250.50m + (fastPath ? 100m : 0m),
                    snapshot.Accounts["ACC1"].CashBalances["USD"]);
            });
        }

        [TestCase("1", false)]
        [TestCase("-1", false)]
        [TestCase("0", true)]
        [TestCase("0.00", true)]
        [TestCase("-0.00", true)]
        [TestCase("invalid", false)]
        public async Task AggregateCashDetectorUsesOnlyNonZeroForeignCashTest(
            string foreignValue, bool fastPath)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId,
                    ("BASE", "350.50"),
                    ("USD", "350.50"),
                    ("EUR", foreignValue));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(fastPath ? 1100.25m : 1000.25m,
                    snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(fastPath ? 0 : 1, scenario.AccountRequestIds.Count);
                Assert.IsFalse(snapshot.Accounts["ACC1"].CashBalances.ContainsKey("EUR"));
            });
        }

        [TestCase("base", true)]
        [TestCase("concrete", true)]
        [TestCase("equal-pair", true)]
        [TestCase("divergent-pair", false)]
        public async Task AggregateBaseAndConcreteCashMustAgreeTest(
            string shape, bool fastPath)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                var rows = shape switch
                {
                    "base" => new[] { (Currency: "BASE", Value: "350.50") },
                    "concrete" => new[] { (Currency: "USD", Value: "350.50") },
                    "equal-pair" => new[]
                    {
                        (Currency: "BASE", Value: "350.50"),
                        (Currency: "USD", Value: "350.50")
                    },
                    "divergent-pair" => new[]
                    {
                        (Currency: "BASE", Value: "351.50"),
                        (Currency: "USD", Value: "350.50")
                    },
                    _ => throw new ArgumentOutOfRangeException(nameof(shape))
                };
                scenario.EmitAggregateCash(requestId, rows);
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(fastPath ? 1100.25m : 1000.25m,
                    snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(fastPath ? 0 : 1, scenario.AccountRequestIds.Count);
            });
        }

        [TestCase("missing")]
        [TestCase("missing-base")]
        [TestCase("duplicate-currency")]
        public async Task AggregateCashDetectorAmbiguityUsesExactFallbackTest(string shape)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                switch (shape)
                {
                    case "missing-base":
                        scenario.EmitAggregateCash(requestId, ("EUR", "0"));
                        break;
                    case "duplicate-currency":
                        scenario.EmitAggregateCash(
                            requestId,
                            ("BASE", "350.50"),
                            ("BASE", "350.50"),
                            ("USD", "350.50"));
                        break;
                }
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1, scenario.AccountRequestIds.Count);
            });
        }

        [TestCase("missing-ready", false)]
        [TestCase("not-ready", false)]
        [TestCase("invalid-ready", false)]
        [TestCase("missing-scalar", false)]
        [TestCase("missing-cash", false)]
        [TestCase("invalid-numeric", false)]
        [TestCase("duplicate-required", false)]
        [TestCase("blank-base-currency", false)]
        [TestCase("scalar-currency", false)]
        [TestCase("base-net-liquidation-currency", false)]
        [TestCase("no-real-currency", true)]
        [TestCase("matching-real-currency", true)]
        [TestCase("conflicting-real-currency", false)]
        public async Task SummaryIdentityOrReadinessAmbiguityFallsBackTest(
            string ambiguity, bool fastPath)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(
                    requestId,
                    "ACC1",
                    baseCurrency: ambiguity == "blank-base-currency" ? string.Empty : "USD",
                    cashRows: ambiguity == "missing-cash"
                        ? Array.Empty<(string Currency, string Value)>()
                        : null,
                    includeRealCurrency: ambiguity != "no-real-currency",
                    omittedTag: ambiguity switch
                    {
                        "missing-ready" => "AccountReady",
                        "missing-scalar" => "AvailableFunds",
                        _ => null
                    },
                    accountReadyValue: ambiguity switch
                    {
                        "not-ready" => "false",
                        "invalid-ready" => "not-a-boolean",
                        _ => "true"
                    },
                    realCurrency: ambiguity == "conflicting-real-currency" ? "EUR" : null,
                    totalCashCurrency: ambiguity == "scalar-currency" ? "EUR" : null,
                    netLiquidationCurrency:
                        ambiguity == "base-net-liquidation-currency" ? "BASE" : null,
                    netLiquidationValue: ambiguity == "invalid-numeric" ? "not-a-number" : null);
                if (ambiguity == "duplicate-required")
                {
                    scenario.Client.accountSummary(
                        requestId, "ACC1", "AccountReady", "true", string.Empty);
                }
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(fastPath ? 1100.25m : 1000.25m,
                    snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(fastPath ? 0 : 1, scenario.AccountRequestIds.Count);
            });
        }

        [TestCase("aggregate-only")]
        [TestCase("unexpected-child")]
        [TestCase("blank-account")]
        [TestCase("blank-tag")]
        public async Task SummaryAttributionCorruptionAlwaysUsesTheExactFallbackTest(
            string corruption)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                if (corruption != "aggregate-only")
                {
                    scenario.EmitValidSummaryAccount(requestId, "ACC1");
                }
                if (corruption == "unexpected-child")
                {
                    scenario.Client.accountSummary(
                        requestId, "ACC2", "NetLiquidation", "999999", "USD");
                }
                else if (corruption == "blank-account")
                {
                    scenario.Client.accountSummary(
                        requestId, string.Empty, "Currency", "BASE", string.Empty);
                }
                else if (corruption == "blank-tag")
                {
                    scenario.Client.accountSummary(
                        requestId, "OTHER", string.Empty, "BASE", string.Empty);
                }
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(250.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
            });
        }

        [Test]
        public async Task UnexpectedAccountCannotSupplyAggregateCashDetectorTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.Client.accountSummary(
                    requestId, "OTHER", "CashBalance", "350.50", "BASE");
                scenario.Client.accountSummary(
                    requestId, "OTHER", "CashBalance", "350.50", "USD");
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(250.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
            });
        }

        [Test]
        public async Task MixedChildBaseCurrenciesFallBackForTheWholeGroupTest()
        {
            const string twoMemberGroup = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts>
                      <String>ACC1</String>
                      <String>ACC2</String>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = twoMemberGroup,
                EndingGroupsDocument = twoMemberGroup
            };
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1", "USD");
                scenario.EmitValidSummaryAccount(requestId, "ACC2", "EUR");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "700"), ("USD", "350"), ("EUR", "350"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, scenario.SummaryRequests.Count);
                Assert.AreEqual(2, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC2"].NetLiquidation);
            });
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task FailedOverlappingGroupFallsBackEveryInScopeMemberTest(
            bool rejectSecondSummary)
        {
            const string overlappingGroups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts>
                      <String>ACC1</String>
                      <String>ACC2</String>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts>
                      <String>ACC2</String>
                      <String>ACC3</String>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = overlappingGroups,
                EndingGroupsDocument = overlappingGroups
            };
            scenario.EnableAccountSummaries((requestId, groupName) =>
            {
                if (groupName == "Alpha")
                {
                    scenario.EmitValidSummaryAccount(requestId, "ACC1", "USD");
                    scenario.EmitValidSummaryAccount(requestId, "ACC2", "USD");
                    scenario.EmitAggregateCash(
                        requestId, ("BASE", "700"), ("USD", "700"));
                }
                else
                {
                    if (rejectSecondSummary)
                    {
                        scenario.Client.error(
                            requestId,
                            0,
                            321,
                            "simulated overlapping-group summary rejection",
                            string.Empty);
                        return;
                    }
                    scenario.EmitValidSummaryAccount(requestId, "ACC2", "USD");
                    scenario.EmitValidSummaryAccount(requestId, "ACC3", "EUR");
                    scenario.EmitAggregateCash(
                        requestId, ("BASE", "700"), ("EUR", "700"));
                }
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(2, scenario.SummaryRequests.Count);
                CollectionAssert.AreEqual(
                    new[] { "ACC2", "ACC3" },
                    scenario.Requests.Where(request => request.StartsWith("account:"))
                        .Select(request => request[8..]));
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC2"].NetLiquidation);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC3"].NetLiquidation);
            });
        }

        [Test]
        public async Task MixedAggregateAttributionFallsBackWithoutRoutingRowsToAChildTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummary(
                    requestId, "OTHER", "Currency", "BASE", string.Empty);
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(250.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
            });
        }

        [Test]
        public async Task PrimaryAccountSummaryRowsAreIgnoredWithoutDisablingTheFastPathTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitValidSummaryAccount(requestId, "MASTER");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(0, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(350.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
            });
        }

        [Test]
        public async Task FiveHundredOneMemberGroupUsesOneSummaryRequestTest()
        {
            var accountIds = Enumerable.Range(1, 501)
                .Select(index => $"ACC{index:000}")
                .ToArray();
            var groupXml =
                "<ListOfGroups><Group><name>Large</name><defaultMethod>Equal</defaultMethod>" +
                "<ListOfAccts>" + string.Concat(accountIds.Select(
                    accountId => $"<String>{accountId}</String>")) +
                "</ListOfAccts></Group></ListOfGroups>";
            using var scenario = new Scenario
            {
                ManagedAccounts = "MASTER," + string.Join(",", accountIds),
                GroupsDocument = groupXml,
                EndingGroupsDocument = groupXml,
                AliasesDocument = "<ListOfAccountAliases />",
                FamilyCodes = Array.Empty<FamilyCode>()
            };
            scenario.EnableAccountSummaries((requestId, groupName) =>
            {
                Assert.AreEqual("Large", groupName);
                foreach (var accountId in accountIds)
                {
                    scenario.EmitValidSummaryAccount(requestId, accountId);
                }
                scenario.EmitAggregateCash(requestId, ("BASE", "0"), ("USD", "0"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(501, snapshot.Accounts.Count);
                Assert.AreEqual(1, scenario.SummaryRequests.Count);
                Assert.AreEqual("Large", scenario.SummaryRequests.Single().GroupName);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                Assert.AreEqual(0, scenario.AccountRequestIds.Count);
            });
        }

        [Test]
        public async Task TopologyChangeAfterValidSummaryPreventsPublicationTest()
        {
            using var scenario = new Scenario
            {
                EndingGroupsDocument = Scenario.GroupsXml.Replace(
                    "<name>Alpha</name>",
                    "<name>AlphaChanged</name>",
                    StringComparison.Ordinal)
            };
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains("topology changed", snapshot.ErrorMessage.ToLowerInvariant());
                Assert.AreEqual(1, scenario.SummaryRequests.Count);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                Assert.AreEqual(0, scenario.AccountRequestIds.Count);
            });
        }

        [Test]
        public async Task SummaryRowsRequireTheExactActiveRequestTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId + 1, "ACC1");
                scenario.EmitAggregateCash(
                    requestId + 1, ("BASE", "1"), ("USD", "1"));
                scenario.Client.accountSummaryEnd(requestId + 1);

                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
                scenario.Client.accountSummary(
                    requestId, "ACC1", "NetLiquidation", "999999", "USD");
            });
            scenario.SummaryCancellation = requestId => scenario.Client.accountSummary(
                requestId, "ACC1", "CashBalance", "999999", "USD");
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(350.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
                Assert.AreEqual(0, scenario.AccountRequestIds.Count);
                Assert.IsNull(GetPrivateField<object>(state, "_pendingRequest"));
            });
        }

        [Test]
        public async Task SummaryWithoutEndIsCanceledAndLateRowsCannotCorruptFallbackTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
            });
            scenario.SummaryCancellation = requestId =>
            {
                scenario.Client.accountSummary(
                    requestId, "ACC1", "NetLiquidation", "999999", "USD");
                scenario.Client.accountSummaryEnd(requestId);
            };
            using var state = scenario.CreateState(
                requestTimeout: TimeSpan.FromMilliseconds(100));

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                Assert.AreEqual(1, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(250.50m, snapshot.Accounts["ACC1"].CashBalances["USD"]);
                Assert.IsNull(GetPrivateField<object>(state, "_pendingRequest"));
            });
        }

        [Test]
        public async Task PhysicalDisconnectDiscardsOutstandingAndLateSummaryRowsTest()
        {
            using var scenario = new Scenario();
            using var firstSummaryStarted = new ManualResetEventSlim();
            var firstRequestId = 0;
            var requestCount = 0;
            scenario.EnableAccountSummaries((requestId, _) =>
            {
                if (Interlocked.Increment(ref requestCount) == 1)
                {
                    firstRequestId = requestId;
                    firstSummaryStarted.Set();
                    return;
                }
                scenario.EmitValidSummaryAccount(requestId, "ACC1");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var interrupted = RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));
            Assert.IsTrue(firstSummaryStarted.Wait(TimeSpan.FromSeconds(5)));

            scenario.Client.connectionClosed();
            scenario.Client.accountSummary(
                firstRequestId, "ACC1", "NetLiquidation", "999999", "USD");
            scenario.Client.accountSummaryEnd(firstRequestId);
            var stale = await interrupted;

            scenario.Client.nextValidId(42);
            state.NotifyBrokerageConnected();
            var recovered = await WaitForReadyGenerationAsync(state, 0);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale, stale.Status);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, recovered.Status);
                Assert.AreEqual(2, scenario.SummaryRequests.Count);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                Assert.AreEqual(1100.25m, recovered.Accounts["ACC1"].NetLiquidation);
                Assert.IsNull(GetPrivateField<object>(state, "_pendingRequest"));
            });
        }

        [Test]
        public async Task DisposeDiscardsOutstandingAndLateSummaryRowsTest()
        {
            using var scenario = new Scenario();
            using var summaryStarted = new ManualResetEventSlim();
            var requestId = 0;
            scenario.EnableAccountSummaries((id, _) =>
            {
                requestId = id;
                summaryStarted.Set();
            });
            using var state = scenario.CreateState();
            var refresh = RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));
            Assert.IsTrue(summaryStarted.Wait(TimeSpan.FromSeconds(5)));
            var worker = GetPrivateField<Task>(state, "_worker");

            state.Dispose();
            scenario.Client.accountSummary(
                requestId, "ACC1", "NetLiquidation", "999999", "USD");
            scenario.Client.accountSummaryEnd(requestId);
            var failed = await refresh;
            await worker.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, failed.Status);
                StringAssert.Contains("disposed", failed.ErrorMessage);
                Assert.AreEqual(0, scenario.CanceledSummaryIds.Count);
                Assert.IsNull(GetPrivateField<object>(state, "_pendingRequest"));
            });
        }

        [Test]
        [NonParallelizable]
        public async Task SummaryRequestRejectionUsesTheExactFallbackTest()
        {
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario();
                scenario.EnableAccountSummaries((requestId, _) =>
                {
                    scenario.Client.error(
                        requestId, 0, 321, "simulated summary rejection", string.Empty);
                });
                using var state = scenario.CreateState();

                var snapshot = await RunRefreshAsync(
                    state, () => state.RequestRefresh(new[] { "Alpha" }));
                var messages = logHandler.Logs
                    .Where(entry => entry.MessageType == LogType.Error &&
                        entry.Message.Contains(
                            "named-group account summary fallback(s)",
                            StringComparison.Ordinal))
                    .Select(entry => entry.Message)
                    .ToArray();

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                    Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                    Assert.AreEqual(1, scenario.AccountRequestIds.Count);
                    Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
                    Assert.AreEqual(1, messages.Length);
                    StringAssert.Contains(
                        "RequestFailure (AccountSummaryRequestRejectedException): " +
                        "IB rejected the account-state request (321): " +
                        "simulated summary rejection",
                        messages.Single());
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task UnexpectedSummaryExceptionFailsTheRefreshTest(
            bool invalidOperationException)
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((_, _) =>
            {
                if (invalidOperationException)
                {
                    throw new InvalidOperationException(
                        "simulated programming failure");
                }
                throw new ArgumentException("simulated programming failure");
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(new[] { "Alpha" }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, snapshot.Status);
                StringAssert.Contains("simulated programming failure", snapshot.ErrorMessage);
                Assert.AreEqual(0, scenario.AccountRequestIds.Count);
                Assert.AreEqual(1, scenario.CanceledSummaryIds.Count);
            });
        }

        [Test]
        public async Task FirstSummaryRequestFailureStopsSummaryBatchingTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, groupName) =>
                scenario.Client.error(
                    requestId,
                    0,
                    321,
                    $"simulated request failure for {groupName}",
                    string.Empty));
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEqual(
                    new[] { "Alpha" },
                    scenario.SummaryRequests.Select(request => request.GroupName));
                CollectionAssert.AreEquivalent(
                    new[] { "ACC1", "ACC2", "ACC3" },
                    scenario.Requests.Where(request => request.StartsWith("account:"))
                        .Select(request => request[8..]));
            });
        }

        [Test]
        [NonParallelizable]
        public async Task SummaryFallbackDiagnosticIsSanitizedAndBoundedTest()
        {
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario();
                var exceptionMessage =
                    "first line\r\nsecond line " + new string('x', 1000) + " END";
                scenario.EnableAccountSummaries((requestId, _) =>
                    scenario.Client.error(
                        requestId,
                        0,
                        321,
                        exceptionMessage,
                        string.Empty));
                using var state = scenario.CreateState();

                var snapshot = await RunRefreshAsync(
                    state, () => state.RequestRefresh(new[] { "Alpha" }));
                var message = logHandler.Logs.Single(entry =>
                    entry.MessageType == LogType.Error && entry.Message.Contains(
                        "named-group account summary fallback(s)",
                        StringComparison.Ordinal)).Message;

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                    StringAssert.Contains(
                        "RequestFailure (AccountSummaryRequestRejectedException): " +
                        "IB rejected the account-state request (321): " +
                        "first line  second line",
                        message);
                    Assert.IsFalse(message.Contains('\r'));
                    Assert.IsFalse(message.Contains('\n'));
                    Assert.IsFalse(message.Contains(" END", StringComparison.Ordinal));
                    Assert.Less(message.Length, 700);
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        public async Task FailedSummaryCancellationDisablesBatchingUntilPhysicalReconnectTest()
        {
            using var scenario = new Scenario();
            scenario.EnableAccountSummaries((requestId, groupName) =>
            {
                scenario.EmitValidSummaryAccount(
                    requestId, groupName == "Alpha" ? "ACC1" : "ACC2");
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "350.50"), ("USD", "350.50"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            var cancellationAttempts = 0;
            scenario.SummaryCancellation = requestId =>
            {
                scenario.Client.accountSummary(
                    requestId, "ACC1", "CashBalance", "999999", "USD");
                if (Interlocked.Increment(ref cancellationAttempts) == 1)
                {
                    throw new InvalidOperationException("simulated cancellation failure");
                }
            };
            using var state = scenario.CreateState();

            var first = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));
            var firstSummaryCount = scenario.SummaryRequests.Count;
            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            var second = await WaitForReadyGenerationAsync(state, first.Generation);
            var secondSummaryCount = scenario.SummaryRequests.Count;

            state.MarkDisconnected("simulated physical disconnect", physicalConnectionClosed: true);
            scenario.Client.nextValidId(1);
            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            var third = await WaitForReadyGenerationAsync(state, second.Generation);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, first.Status);
                Assert.AreEqual(1100.25m, first.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1000.25m, first.Accounts["ACC2"].NetLiquidation);
                Assert.AreEqual(1, firstSummaryCount);
                Assert.AreEqual(firstSummaryCount, secondSummaryCount);
                Assert.AreEqual(1000.25m, second.Accounts["ACC1"].NetLiquidation);
                Assert.Greater(scenario.SummaryRequests.Count, secondSummaryCount);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, third.Status);
                Assert.GreaterOrEqual(cancellationAttempts, 2);
            });
        }

        [Test]
        [NonParallelizable]
        public async Task GroupFallbacksProduceOneDeterministicErrorPerRefreshTest()
        {
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario();
                scenario.EnableAccountSummaries((requestId, groupName) =>
                {
                    var alpha = groupName == "Alpha";
                    scenario.EmitValidSummaryAccount(
                        requestId,
                        alpha ? "ACC1" : "ACC2",
                        omittedTag: alpha ? "AccountReady" : null);
                    scenario.EmitAggregateCash(
                        requestId,
                        ("BASE", "350.50"),
                        ("USD", "350.50"),
                        ("EUR", alpha ? "0" : "1"));
                    scenario.Client.accountSummaryEnd(requestId);
                });
                using var state = scenario.CreateState();

                var first = await RunRefreshAsync(
                    state, () => state.RequestRefresh(new[] { "Beta", "Alpha" }));
                Assert.IsTrue(state.RequestRefresh(new[] { "Beta", "Alpha" }));
                var second = await WaitForReadyGenerationAsync(state, first.Generation);

                var messages = logHandler.Logs
                    .Where(entry => entry.MessageType == LogType.Error && entry.Message.Contains(
                        "named-group account summary fallback(s)",
                        StringComparison.Ordinal))
                    .Select(entry => entry.Message)
                    .ToArray();
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, second.Status);
                    Assert.AreEqual(2, messages.Length);
                    Assert.IsTrue(messages.All(message =>
                        message.Contains("group 'Alpha': MissingRows", StringComparison.Ordinal) &&
                        message.Contains("ACC1:AccountReady", StringComparison.Ordinal) &&
                        message.Contains(
                            "group 'Beta': NonBaseAggregateCash currencies=[EUR]",
                            StringComparison.Ordinal) &&
                        message.IndexOf("group 'Alpha'", StringComparison.Ordinal) <
                        message.IndexOf("group 'Beta'", StringComparison.Ordinal) &&
                        !message.Contains("350.50", StringComparison.Ordinal)));
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        [NonParallelizable]
        public async Task SummaryFallbackDiagnosticCapsGroupDetailsTest()
        {
            const int groupCount = 12;
            var groupNames = Enumerable.Range(0, groupCount)
                .Select(index => $"Group{index:00}")
                .ToArray();
            var accountIds = Enumerable.Range(0, groupCount)
                .Select(index => $"ACC{index:00}")
                .ToArray();
            var groupsXml = "<ListOfGroups>" + string.Concat(groupNames.Select(
                (groupName, index) =>
                    $"<Group><name>{groupName}</name><defaultMethod>Equal</defaultMethod>" +
                    $"<ListOfAccts><String>{accountIds[index]}</String></ListOfAccts></Group>")) +
                "</ListOfGroups>";
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario
                {
                    ManagedAccounts = "MASTER," + string.Join(",", accountIds),
                    GroupsDocument = groupsXml,
                    EndingGroupsDocument = groupsXml,
                    AliasesDocument = "<ListOfAccountAliases />",
                    FamilyCodes = Array.Empty<FamilyCode>()
                };
                scenario.EnableAccountSummaries((requestId, groupName) =>
                {
                    var index = Array.IndexOf(groupNames, groupName);
                    scenario.EmitValidSummaryAccount(
                        requestId, accountIds[index], omittedTag: "AccountReady");
                    scenario.EmitAggregateCash(
                        requestId, ("BASE", "350.50"), ("USD", "350.50"));
                    scenario.Client.accountSummaryEnd(requestId);
                });
                using var state = scenario.CreateState();

                var snapshot = await RunRefreshAsync(
                    state, () => state.RequestRefresh(Array.Empty<string>()));
                var message = logHandler.Logs.Single(entry =>
                    entry.MessageType == LogType.Error && entry.Message.Contains(
                        "named-group account summary fallback(s)",
                        StringComparison.Ordinal)).Message;

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                    Assert.AreEqual(groupCount, scenario.SummaryRequests.Count);
                    Assert.AreEqual(groupCount, scenario.AccountRequestIds.Count);
                    foreach (var groupName in groupNames.Take(10))
                    {
                        StringAssert.Contains($"group '{groupName}'", message);
                    }
                    foreach (var groupName in groupNames.Skip(10))
                    {
                        Assert.IsFalse(message.Contains(groupName, StringComparison.Ordinal));
                    }
                    StringAssert.Contains("omittedFallbacks=2", message);
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        [NonParallelizable]
        public async Task FirstFailedRefreshThenReadyLogsOneDiagnosticTest()
        {
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = Scenario.SingleAccount();
                var requestPositions = scenario.Actions.RequestPositions;
                scenario.Actions.RequestPositions =
                    (requestId, accountOrGroup, authorize) =>
                        scenario.RunAuthorized(authorize, () =>
                            throw new InvalidOperationException(
                                "simulated initial position failure"));
                using var state = scenario.CreateState();

                var failed = await RunRefreshAsync(
                    state, () => state.RequestRefresh(Array.Empty<string>()));
                scenario.Actions.RequestPositions = requestPositions;
                var ready = await RunRefreshAsync(
                    state, () => state.RequestRefresh(Array.Empty<string>()));
                await WaitForReadyDiagnosticCountAsync(logHandler, 1);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Failed, failed.Status);
                    StringAssert.Contains(
                        "simulated initial position failure", failed.ErrorMessage);
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, ready.Status);
                    Assert.AreEqual(1, GetReadyDiagnosticMessages(logHandler).Length);
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        [NonParallelizable]
        public async Task ReadyDiagnosticLogsOncePerPhysicalConnectionEpochTest()
        {
            var originalLogHandler = Log.LogHandler;
            var logHandler = new QueueLogHandler();
            Log.LogHandler = logHandler;
            try
            {
                using var scenario = new Scenario
                {
                    ManagedAccounts = "MASTER,MASTERA,ACC1,ACC2,ACC3"
                };
                scenario.EnableAccountSummaries((requestId, groupName) =>
                {
                    scenario.EmitValidSummaryAccount(
                        requestId, groupName == "Alpha" ? "ACC1" : "ACC2");
                    scenario.EmitAggregateCash(
                        requestId, ("BASE", "350.50"), ("USD", "350.50"));
                    scenario.Client.accountSummaryEnd(requestId);
                });
                using var state = scenario.CreateState();

                var first = await RunRefreshAsync(
                    state, () => state.RequestRefresh(Array.Empty<string>()));
                Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
                var second = await WaitForReadyGenerationAsync(state, first.Generation);

                scenario.Client.error(
                    -1, 0, 1100, "Connectivity between IB and TWS was lost.", string.Empty);
                scenario.Client.error(
                    -1, 0, 1102, "Connectivity between IB and TWS was restored.", string.Empty);
                Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
                var logicalReconnect = await WaitForReadyGenerationAsync(
                    state, second.Generation);

                state.MarkDisconnected(
                    "simulated physical disconnect", physicalConnectionClosed: true);
                scenario.Client.nextValidId(1);
                Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
                await WaitForReadyGenerationAsync(
                    state, logicalReconnect.Generation);
                await WaitForReadyDiagnosticCountAsync(logHandler, 2);

                var messages = GetReadyDiagnosticMessages(logHandler);
                const string expected =
                    "FA snapshot ready: accessibleManagedAccounts=4; " +
                    "discoveredGroups=2; groups=[Alpha:1,Beta:1]; " +
                    "collectedAccounts=3; clientAccountsOutsideGroups=1; " +
                    "accountSummaryFastPathAccounts=2; accountUpdateFallbackAccounts=1; " +
                    "complete=true; observedAccountTypes=[INDIVIDUAL]";
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(2, messages.Length);
                    Assert.IsTrue(messages.All(message => message == expected));
                });
            }
            finally
            {
                Log.LogHandler = originalLogHandler;
                logHandler.Dispose();
            }
        }

        [Test]
        public void ReadyDiagnosticBoundsAndSortsGroupDetailsTest()
        {
            var groups = Enumerable.Range(0, 52)
                .Reverse()
                .Select(index => new BrokerageAccountGroup(
                    $"Group{index:00}", "Equal", new[] { "ACC1" }))
                .ToDictionary(group => group.Name, StringComparer.OrdinalIgnoreCase);
            var snapshot = new BrokerageAccountSnapshot(
                BrokerageAccountSnapshotStatus.Ready,
                1,
                DateTime.UtcNow,
                DateTime.UtcNow,
                groups,
                new Dictionary<string, BrokerageAccountState>(
                    StringComparer.OrdinalIgnoreCase),
                Array.Empty<string>(),
                "membership",
                "configuration",
                string.Empty,
                "MASTER",
                new[] { "MASTER", "MASTERA" },
                groups,
                isComplete: false);
            var listedGroups = string.Join(",", Enumerable.Range(0, 50)
                .Select(index => $"Group{index:00}:1"));

            Assert.AreEqual(
                "FA snapshot ready: accessibleManagedAccounts=1; " +
                $"discoveredGroups=52; groups=[{listedGroups}]; omittedGroups=2; " +
                "collectedAccounts=0; clientAccountsOutsideGroups=0; " +
                "accountSummaryFastPathAccounts=0; accountUpdateFallbackAccounts=0; " +
                "complete=false; observedAccountTypes=[]",
                InteractiveBrokersFinancialAdvisorAccountState
                    .FormatReadySnapshotDiagnostic(snapshot, 0, 0));
        }

        [Test]
        public async Task FailedGroupMembersRemainOnTheExactPathAcrossOverlappingGroupsTest()
        {
            const string overlappingGroups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts>
                      <String>ACC1</String>
                      <String>ACC2</String>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts>
                      <String>ACC2</String>
                      <String>ACC3</String>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new Scenario
            {
                GroupsDocument = overlappingGroups,
                EndingGroupsDocument = overlappingGroups
            };
            scenario.EnableAccountSummaries((requestId, groupName) =>
            {
                if (groupName == "Alpha")
                {
                    scenario.EmitValidSummaryAccount(
                        requestId, "ACC1", omittedTag: "AccountReady");
                    scenario.EmitValidSummaryAccount(requestId, "ACC2");
                }
                else
                {
                    scenario.EmitValidSummaryAccount(requestId, "ACC2");
                    scenario.EmitValidSummaryAccount(requestId, "ACC3");
                }
                scenario.EmitAggregateCash(
                    requestId, ("BASE", "700"), ("USD", "700"));
                scenario.Client.accountSummaryEnd(requestId);
            });
            using var state = scenario.CreateState();

            var snapshot = await RunRefreshAsync(
                state, () => state.RequestRefresh(Array.Empty<string>()));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, snapshot.Status);
                CollectionAssert.AreEquivalent(
                    new[] { "ACC1", "ACC2" },
                    scenario.Requests.Where(request => request.StartsWith("account:"))
                        .Select(request => request[8..]));
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC1"].NetLiquidation);
                Assert.AreEqual(1000.25m, snapshot.Accounts["ACC2"].NetLiquidation);
                Assert.AreEqual(1100.25m, snapshot.Accounts["ACC3"].NetLiquidation);
            });
        }

        private static async Task<BrokerageAccountSnapshot> RunRefreshAsync(
            InteractiveBrokersFinancialAdvisorAccountState state,
            Func<bool> request)
        {
            Assert.IsTrue(request(), "The refresh request was not accepted.");
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                var snapshot = state.Snapshot;
                if (snapshot.Status is BrokerageAccountSnapshotStatus.Ready or
                    BrokerageAccountSnapshotStatus.Failed or
                    BrokerageAccountSnapshotStatus.Stale)
                {
                    return snapshot;
                }
                await Task.Delay(5);
            }
            throw new TimeoutException("The refresh did not publish a terminal snapshot.");
        }

        private static async Task<BrokerageAccountSnapshot> WaitForReadyGenerationAsync(
            InteractiveBrokersFinancialAdvisorAccountState state,
            long previousGeneration)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                var snapshot = state.Snapshot;
                if (snapshot.Status == BrokerageAccountSnapshotStatus.Ready &&
                    snapshot.Generation > previousGeneration)
                {
                    return snapshot;
                }
                await Task.Delay(5);
            }
            throw new TimeoutException(
                "The reconnect refresh did not publish a newer Ready snapshot.");
        }

        private static string[] GetReadyDiagnosticMessages(QueueLogHandler logHandler) =>
            logHandler.Logs
                .Where(entry => entry.MessageType == LogType.Trace && entry.Message.Contains(
                    "FA snapshot ready:", StringComparison.Ordinal))
                .Select(entry => entry.Message)
                .ToArray();

        private static async Task WaitForReadyDiagnosticCountAsync(
            QueueLogHandler logHandler,
            int expectedCount)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
            while (DateTime.UtcNow < deadline)
            {
                if (GetReadyDiagnosticMessages(logHandler).Length >= expectedCount)
                {
                    return;
                }
                await Task.Delay(5);
            }
            Assert.Fail($"Expected {expectedCount} FA Ready diagnostics.");
        }

        private static long GetRequestVersion(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            (long)typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_requestVersion",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);

        private static T GetPrivateField<T>(
            InteractiveBrokersFinancialAdvisorAccountState state,
            string name) =>
            (T)typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);

        private static bool IsPendingRequestWireSent(
            InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var pending = typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_pendingRequest", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);
            return pending != null && (bool)pending.GetType()
                .GetProperty("WireSent", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pending);
        }

        private static bool HasQueuedRefresh(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_queuedRefresh",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state) != null;

        private static void FillRefreshQueue(
            InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var stateType = typeof(InteractiveBrokersFinancialAdvisorAccountState);
            var scopeType = stateType.GetNestedType(
                "SnapshotScope",
                BindingFlags.NonPublic);
            var workItemType = stateType.GetNestedType(
                "WorkItem",
                BindingFlags.NonPublic);
            var channel = stateType.GetField(
                    "_work",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);
            var writer = channel?.GetType().GetProperty("Writer")?.GetValue(channel);
            var tryWrite = writer?.GetType().GetMethod(
                "TryWrite",
                new[] { workItemType });

            Assert.Multiple(() =>
            {
                Assert.IsNotNull(scopeType);
                Assert.IsNotNull(workItemType);
                Assert.IsNotNull(writer);
                Assert.IsNotNull(tryWrite);
            });
            for (var index = 0; index < 8; index++)
            {
                var scope = Activator.CreateInstance(
                    scopeType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new object[]
                    {
                        Array.Empty<string>(),
                        Array.Empty<string>(),
                        false,
                        0L
                    },
                    null);
                var item = Activator.CreateInstance(
                    workItemType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new[] { scope },
                    null);
                Assert.IsTrue(
                    (bool)tryWrite.Invoke(writer, new[] { item }),
                    $"Expected bounded queue slot {index + 1} to accept a dummy item.");
            }
        }

        private static (
            IReadOnlyCollection<string> Groups,
            IReadOnlyCollection<string> AdditionalAccounts,
            bool CompleteDiscovery) GetQueuedScope(
                InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var queued = typeof(InteractiveBrokersFinancialAdvisorAccountState)
                .GetField("_queuedRefresh", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(state);
            Assert.IsNotNull(queued);
            var scope = queued.GetType()
                .GetField("Scope", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(queued);
            Assert.IsNotNull(scope);
            var scopeType = scope.GetType();
            return (
                (IReadOnlyCollection<string>)scopeType.GetProperty(
                    "GroupNames", BindingFlags.Instance | BindingFlags.NonPublic)
                    ?.GetValue(scope),
                (IReadOnlyCollection<string>)scopeType.GetProperty(
                    "AdditionalAccountIds", BindingFlags.Instance | BindingFlags.NonPublic)
                    ?.GetValue(scope),
                (bool)scopeType.GetProperty(
                    "CompleteDiscovery", BindingFlags.Instance | BindingFlags.NonPublic)
                    ?.GetValue(scope));
        }

        private static Contract CreateAirContract(
            int contractId,
            string currency,
            string primaryExchange) => new()
        {
            ConId = contractId,
            Symbol = "AIR",
            LocalSymbol = "AIR",
            SecType = "STK",
            Currency = currency,
            Exchange = "SMART",
            PrimaryExch = primaryExchange,
            TradingClass = "AIR",
            Multiplier = "1"
        };

        private sealed class AirMapFileProvider : IMapFileProvider
        {
            private readonly MapFileResolver _resolver = new(new[]
            {
                new MapFile("air", new[]
                {
                    new MapFileRow(Time.BeginningOfTime, "air", Exchange.NYSE),
                    new MapFileRow(Time.EndOfTime, "air", Exchange.NYSE)
                })
            });

            public void Initialize(IDataProvider dataProvider)
            {
            }

            public MapFileResolver Get(AuxiliaryDataKey auxiliaryDataKey) =>
                auxiliaryDataKey.Equals(AuxiliaryDataKey.EquityUsa)
                    ? _resolver
                    : MapFileResolver.Empty;
        }

        private sealed class Scenario : IDisposable
        {
            internal const string EmptyGroupsXml = "<ListOfGroups />";
            internal const string GroupsXml = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>ACC1</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            private const string AliasesXml = """
                <ListOfAccountAliases>
                  <AccountAlias><account>ACC1</account><alias>Alpha Client</alias></AccountAlias>
                  <AccountAlias><account>ACC2</account><alias>Beta Client</alias></AccountAlias>
                  <AccountAlias><account>ACC3</account><alias>Unassigned Client</alias></AccountAlias>
                </ListOfAccountAliases>
                """;

            internal InteractiveBrokersClient Client { get; }
            internal InteractiveBrokersFinancialAdvisorAccountState.RequestActions Actions { get; }
            internal List<string> Requests { get; } = new();
            internal List<int> KeyedRequestIds { get; } = new();
            internal List<int> AccountRequestIds { get; } = new();
            internal List<int> CanceledPositionIds { get; } = new();
            internal List<int> CanceledAccountIds { get; } = new();
            internal List<(int RequestId, string GroupName, string Tags)>
                SummaryRequests { get; } = new();
            internal List<int> CanceledSummaryIds { get; } = new();
            internal Action ExternalCallProbe { get; set; } = () => { };
            internal Action<int> SummaryCancellation { get; set; } = _ => { };
            internal string ManagedAccounts { get; set; } = "MASTER,ACC1,ACC2,ACC3";
            internal string EndingManagedAccounts { get; set; }
            internal string GroupsDocument { get; set; } = GroupsXml;
            internal string EndingGroupsDocument { get; set; } = GroupsXml;
            internal string AliasesDocument { get; set; } = AliasesXml;
            internal string EndingAliasesDocument { get; set; }
            internal FamilyCode[] FamilyCodes { get; set; } =
            {
                new() { AccountID = "ACC1", FamilyCodeStr = "Family-A" },
                new() { AccountID = "ACC2", FamilyCodeStr = "Family-B" },
                new() { AccountID = "ACC3", FamilyCodeStr = "Family-C" }
            };
            internal FamilyCode[] EndingFamilyCodes { get; set; }
            internal int GroupsRequestCount { get; private set; }
            internal int MaximumConcurrentExternalCalls => _maximumConcurrentExternalCalls;
            private int _managedAccountsRequestCount;
            private int _aliasesRequestCount;
            private int _familyCodesRequestCount;
            private int _activeExternalCalls;
            private int _maximumConcurrentExternalCalls;

            internal Scenario()
            {
                Client = new InteractiveBrokersClient(new EReaderMonitorSignal());
                Actions =
                    new InteractiveBrokersFinancialAdvisorAccountState.RequestActions(Client)
                    {
                        RequestManagedAccounts = authorize =>
                            RunAuthorized(authorize, () =>
                            {
                                Requests.Add("managed");
                                Client.managedAccounts(
                                    ++_managedAccountsRequestCount % 2 == 1
                                        ? ManagedAccounts
                                        : EndingManagedAccounts ?? ManagedAccounts);
                            }),
                        RequestFinancialAdvisor = (faDataType, authorize) =>
                            RunAuthorized(authorize, () =>
                            {
                                Requests.Add($"fa:{faDataType}");
                                var document = faDataType == 1
                                    ? ++GroupsRequestCount % 2 == 1
                                        ? GroupsDocument
                                        : EndingGroupsDocument
                                    : ++_aliasesRequestCount % 2 == 1
                                        ? AliasesDocument
                                        : EndingAliasesDocument ?? AliasesDocument;
                                Client.receiveFA(faDataType, document);
                            }),
                        RequestFamilyCodes = authorize =>
                            RunAuthorized(authorize, () =>
                            {
                                Requests.Add("family");
                                Client.familyCodes(
                                    ++_familyCodesRequestCount % 2 == 1
                                        ? FamilyCodes
                                        : EndingFamilyCodes ?? FamilyCodes);
                            }),
                        RequestPositions = (requestId, accountOrGroup, authorize) =>
                            RunAuthorized(
                                authorize,
                                () => EmitPositions(requestId, accountOrGroup)),
                        CancelPositions = (requestId, authorize) =>
                            RunAuthorized(
                                authorize,
                                () => CanceledPositionIds.Add(requestId)),
                        RequestAccountUpdates = (requestId, accountId, authorize) =>
                            RunAuthorized(
                                authorize,
                                () => EmitAccountValues(requestId, accountId)),
                        CancelAccountUpdates = (requestId, authorize) =>
                            RunAuthorized(
                                authorize,
                                () => CanceledAccountIds.Add(requestId)),
                        RequestAccountSummary = null,
                        CancelAccountSummary = null
                    };
            }

            internal InteractiveBrokersFinancialAdvisorAccountState CreateState(
                string configuredGroup = "",
                TimeSpan? requestTimeout = null,
                Action paceRequest = null,
                Func<Contract, Symbol> mapSymbol = null,
                Action<string> reportUnsupported = null,
                Func<bool> isConnected = null)
            {
                return new InteractiveBrokersFinancialAdvisorAccountState(
                    Client,
                    paceRequest ?? (() => { }),
                    isConnected ?? (() => true),
                    mapSymbol ?? (contract => contract.Symbol == "UNMAPPED"
                            ? throw new InvalidOperationException("No LEAN symbol mapping.")
                            : Symbol.Create(contract.Symbol, SecurityType.Equity, Market.USA)),
                    "MASTER",
                    configuredGroup,
                    requestTimeout ?? TimeSpan.FromSeconds(2),
                    reportUnsupported,
                    requestActions: Actions);
            }

            internal static Scenario SingleAccount() => new()
            {
                ManagedAccounts = "MASTER,ACC1",
                GroupsDocument = EmptyGroupsXml,
                EndingGroupsDocument = EmptyGroupsXml,
                AliasesDocument = "<ListOfAccountAliases />",
                FamilyCodes = Array.Empty<FamilyCode>()
            };

            internal void EnableAccountSummaries(Action<int, string> emit)
            {
                Actions.RequestAccountSummary =
                    (requestId, groupName, tags, authorize) =>
                        RunAuthorized(authorize, () =>
                        {
                            Requests.Add($"summary:{groupName}");
                            KeyedRequestIds.Add(requestId);
                            SummaryRequests.Add((requestId, groupName, tags));
                            emit(requestId, groupName);
                        });
                Actions.CancelAccountSummary = (requestId, authorize) =>
                    RunAuthorized(authorize, () =>
                    {
                        Requests.Add($"cancel-summary:{requestId}");
                        CanceledSummaryIds.Add(requestId);
                        SummaryCancellation(requestId);
                    });
            }

            internal void EmitValidSummaryAccount(
                int requestId,
                string accountId,
                string baseCurrency = "USD",
                IReadOnlyCollection<(string Currency, string Value)> cashRows = null,
                bool includeRealCurrency = true,
                string omittedTag = null,
                string accountReadyValue = "true",
                string realCurrency = null,
                string totalCashCurrency = null,
                string netLiquidationCurrency = null,
                string netLiquidationValue = null)
            {
                void Emit(string tag, string value, string currency)
                {
                    if (!tag.Equals(omittedTag, StringComparison.OrdinalIgnoreCase))
                    {
                        Client.accountSummary(
                            requestId, accountId, tag, value, currency);
                    }
                }

                Emit("AccountType", "INDIVIDUAL", string.Empty);
                Emit(
                    "NetLiquidation",
                    netLiquidationValue ?? "1100.25",
                    netLiquidationCurrency ?? baseCurrency);
                Emit("TotalCashValue", "350.50", totalCashCurrency ?? baseCurrency);
                Emit("AvailableFunds", "300.25", baseCurrency);
                Emit("ExcessLiquidity", "275.25", baseCurrency);
                Emit("BuyingPower", "600.50", baseCurrency);
                Emit("AccountReady", accountReadyValue, string.Empty);
                Client.accountSummary(
                    requestId, accountId, "Currency", "BASE", string.Empty);
                if (includeRealCurrency)
                {
                    Client.accountSummary(
                        requestId,
                        accountId,
                        "RealCurrency",
                        realCurrency ?? baseCurrency,
                        "BASE");
                }
                Client.accountSummary(
                    requestId, accountId, "TotalCashBalance", "999999", "BASE");
                Client.accountSummary(
                    requestId, accountId, "NetLiquidationByCurrency", "888888", baseCurrency);
                foreach (var cash in cashRows ??
                    new[] { (Currency: "BASE", Value: "350.50") })
                {
                    Client.accountSummary(
                        requestId,
                        accountId,
                        "CashBalance",
                        cash.Value,
                        cash.Currency);
                }
            }

            internal void EmitAggregateCash(
                int requestId,
                params (string Currency, string Value)[] rows)
            {
                foreach (var row in rows)
                {
                    Client.accountSummary(
                        requestId, "All", "CashBalance", row.Value, row.Currency);
                }
            }

            internal bool RunAuthorized(Func<bool> authorize, Action action)
            {
                if (!authorize())
                {
                    return false;
                }
                RunExternal(action);
                return true;
            }

            private void RunExternal(Action action)
            {
                ExternalCallProbe();
                var active = Interlocked.Increment(ref _activeExternalCalls);
                int observed;
                while (active > (observed = _maximumConcurrentExternalCalls) &&
                    Interlocked.CompareExchange(
                        ref _maximumConcurrentExternalCalls, active, observed) != observed)
                {
                }
                try
                {
                    action();
                }
                finally
                {
                    Interlocked.Decrement(ref _activeExternalCalls);
                }
            }

            private void EmitPositions(int requestId, string accountOrGroup)
            {
                Requests.Add($"positions:{accountOrGroup}");
                KeyedRequestIds.Add(requestId);
                if (accountOrGroup == "Alpha")
                {
                    Client.positionMulti(
                        requestId,
                        "ACC1",
                        "Model-A",
                        MappedContract(),
                        1.25m,
                        100.5);
                }
                else if (accountOrGroup == "Beta")
                {
                    Client.positionMulti(
                        requestId,
                        "ACC2",
                        "Model-B",
                        UnmappedContract(),
                        2.75m,
                        12.345);
                }
                Client.positionMultiEnd(requestId);
            }

            internal void EmitAccountValues(int requestId, string accountId)
            {
                Requests.Add($"account:{accountId}");
                KeyedRequestIds.Add(requestId);
                AccountRequestIds.Add(requestId);
                Client.accountUpdateMulti(
                    requestId, accountId, string.Empty, "AccountReady", "true", string.Empty);
                Client.accountUpdateMulti(
                    requestId, accountId, string.Empty, "AccountType", "INDIVIDUAL", string.Empty);
                Client.accountUpdateMulti(
                    requestId, accountId, string.Empty, "NetLiquidation", "1000.25", "USD");
                Client.accountUpdateMulti(
                    requestId, accountId, string.Empty, "TotalCashValue", "250.50", "USD");
                Client.accountUpdateMulti(
                    requestId, accountId, string.Empty, "$LEDGER-USD:CashBalance", "250.50", string.Empty);
                Client.accountUpdateMultiEnd(requestId);
            }

            internal static Contract MappedContract() => new()
            {
                ConId = 101,
                Symbol = "SPY",
                LocalSymbol = "SPY",
                SecType = "STK",
                Currency = "USD",
                Exchange = "SMART",
                PrimaryExch = "ARCA"
            };

            internal static Contract UnmappedContract() => new()
            {
                ConId = 202,
                Symbol = "UNMAPPED",
                LocalSymbol = "UNMAPPED",
                SecType = "STK",
                Currency = "USD",
                Exchange = "SMART",
                PrimaryExch = "ARCA"
            };

            public void Dispose()
            {
                Client.Dispose();
            }
        }
    }
}
