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

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersFinancialAdvisorAccountStateMutationTests
    {
        [Test]
        public async Task WriteRequiresReadySnapshotTest()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();

            Assert.Multiple(() =>
            {
                Assert.IsFalse(state.RequestGroupAssignment(
                    "ACC1", "Alpha", "membership", "configuration"));
                Assert.IsFalse(state.RequestGroupAllocationUpdate(
                    "Alpha",
                    new Dictionary<string, decimal>
                    {
                        ["ACC1"] = 1m,
                        ["ACC2"] = 2m
                    },
                    "membership",
                    "configuration"));
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Unavailable,
                    state.GroupAssignment.Status);
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Unavailable,
                    state.GroupAllocationUpdate.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });

            await ReadyAsync(state);
            state.MarkDisconnected("test disconnect");
            Assert.IsFalse(state.RequestGroupAssignment(
                "ACC1",
                "Alpha",
                state.Snapshot.MembershipHash,
                state.Snapshot.GroupConfigurationVersion));
            Assert.IsFalse(state.IsGroupTradingBlocked);
        }

        [Test]
        public async Task MutationRequiresMatchingSnapshotHashesTest()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            var version = GetRequestVersion(state);
            var prior = state.GroupAllocationUpdate;

            Assert.Multiple(() =>
            {
                Assert.IsFalse(state.RequestGroupAllocationUpdate(
                    "Alpha",
                    MutationScenario.CurrentAllocation(),
                    ready.MembershipHash + "-old",
                    ready.GroupConfigurationVersion));
                Assert.IsFalse(state.RequestGroupAllocationUpdate(
                    "Alpha",
                    MutationScenario.CurrentAllocation(),
                    ready.MembershipHash,
                    ready.GroupConfigurationVersion + "-old"));
                Assert.AreSame(prior, state.GroupAllocationUpdate);
                Assert.AreEqual(version, GetRequestVersion(state));
                Assert.AreSame(ready, state.Snapshot);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "Alpha",
                MutationScenario.CurrentAllocation(),
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var completed = await AllocationTerminalAsync(state);
            Assert.AreEqual(
                BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                completed.Status);
        }

        [Test]
        public async Task ReplacementAmbiguityStartsAtAuthorizationTest()
        {
            using (var preWireScenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.ThrowBeforeAuthorization
            })
            using (var preWireState = preWireScenario.CreateState())
            {
                var ready = await ReadyAsync(preWireState);
                Assert.IsTrue(RequestChangedAllocation(preWireState, ready));
                var failed = await AllocationTerminalAsync(preWireState);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    StringAssert.DoesNotContain(
                        "may have applied", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Ready,
                        preWireState.Snapshot.Status);
                    Assert.IsFalse(preWireState.IsGroupTradingBlocked);
                });
            }

            using (var authorizedScenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.ThrowAfterAuthorization
            })
            using (var authorizedState = authorizedScenario.CreateState())
            {
                var ready = await ReadyAsync(authorizedState);
                Assert.IsTrue(RequestChangedAllocation(authorizedState, ready));
                var failed = await AllocationTerminalAsync(authorizedState);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    StringAssert.Contains(
                        "replaceFA may have applied", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        authorizedState.Snapshot.Status);
                    Assert.IsTrue(authorizedState.IsGroupTradingBlocked);
                });
            }

            using (var postWireScenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.ReturnWithoutCompletion
            })
            using (var postWireState = postWireScenario.CreateState(
                TimeSpan.FromMilliseconds(150)))
            {
                var ready = await ReadyAsync(postWireState);
                Assert.IsTrue(RequestChangedAllocation(postWireState, ready));
                var failed = await AllocationTerminalAsync(postWireState);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    StringAssert.Contains(
                        "replaceFA may have applied", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        postWireState.Snapshot.Status);
                    Assert.IsTrue(postWireState.IsGroupTradingBlocked);
                });
                var reconciled = await ReadyAsync(postWireState);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    reconciled.Status);
                Assert.IsFalse(postWireState.IsGroupTradingBlocked);
            }
        }

        [Test]
        public async Task DisposeAfterReplacementAuthorizationPreservesAmbiguityTest()
        {
            using var scenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.BlockAfterAuthorization
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            Assert.IsTrue(
                scenario.ReplacementAuthorized.Wait(TimeSpan.FromSeconds(10)));
            var worker = GetWorker(state);
            try
            {
                state.Dispose();
                var failed = state.GroupAllocationUpdate;

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    StringAssert.Contains(
                        "replaceFA may have applied", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        state.Snapshot.Status);
                    Assert.IsTrue(state.IsGroupTradingBlocked);
                });
            }
            finally
            {
                scenario.ReleaseReplacement();
            }
            await worker.WaitAsync(TimeSpan.FromSeconds(5));
        }

        [TestCase(-1)]
        [TestCase(int.MaxValue)]
        public async Task UnsavedChangesReadbackRetriesUntilXmlConfirmsMutationTest(
            int requestId)
        {
            using var scenario = new MutationScenario
            {
                UnsavedChangesReadbacksBeforeApply = 2,
                UnsavedChangesRequestId = requestId
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var completed = await AllocationTerminalAsync(state);
            var refreshed = await ReadyAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreEqual(2, scenario.UnsavedChangesReadbackCount);
                Assert.AreEqual(1, scenario.ReplaceCount);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, refreshed.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task UnsavedChangesReadbackDeadlinePreservesAmbiguousStateTest()
        {
            using var scenario = new MutationScenario
            {
                UnsavedChangesReadbacksBeforeApply = int.MaxValue,
                UnsavedChangesRequestId = int.MaxValue
            };
            using var state = scenario.CreateState(TimeSpan.FromMilliseconds(700));
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                Assert.GreaterOrEqual(scenario.UnsavedChangesReadbackCount, 2);
                Assert.AreEqual(1, scenario.ReplaceCount);
                StringAssert.Contains("replaceFA may have applied", failed.ErrorMessage);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale, state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
            });
        }

        [Test]
        public async Task MutationUsesLeanOpenOrderPreconditionTest()
        {
            using var scenario = new MutationScenario();
            scenario.OpenOrderResult = call => call == 2;
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                Assert.AreEqual(2, scenario.OpenOrderCheckCount);
                Assert.AreEqual(0, scenario.ReplaceCount);
                StringAssert.Contains("LEAN Financial Advisor group order is open",
                    failed.ErrorMessage);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task MutationExternalCallsNeverRunUnderStateMonitor()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.CallbackStateLock = GetCallbackStateLock(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var completed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.IsFalse(scenario.OpenOrderObservedUnderMonitor);
                Assert.IsFalse(scenario.ServerVersionObservedUnderMonitor);
                Assert.IsFalse(scenario.ReplaceObservedUnderMonitor);
            });
        }

        [Test]
        public async Task MutationFailsBeforeWireOnUnsupportedServerVersionTest()
        {
            using var scenario = new MutationScenario
            {
                ServerVersion = MinServerVer.REPLACE_FA_END - 1
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                StringAssert.Contains(
                    $"requires IB server version {MinServerVer.REPLACE_FA_END} or later",
                    failed.ErrorMessage);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [TestCase(MinServerVer.REPLACE_FA_END)]
        [TestCase(0)]
        public async Task MutationSupportsMinimumOrUnknownServerVersionTest(
            int serverVersion)
        {
            using var scenario = new MutationScenario
            {
                ServerVersion = serverVersion
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var completed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreEqual(1, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task NoOpMutationStillReadsFreshAuthorityAndPublishesNewSnapshot()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            var managedBefore = scenario.ManagedRequestCount;
            var groupsBefore = scenario.GroupsRequestCount;
            scenario.BlockNextManagedRequest();

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "Alpha",
                MutationScenario.CurrentAllocation(),
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            Assert.IsTrue(scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            var pending = state.GroupAllocationUpdate;
            Assert.AreEqual(
                BrokerageAccountGroupAllocationUpdateStatus.Pending,
                pending.Status);
            var pendingGeneration = pending.Generation;
            scenario.ReleaseManagedRequest();

            var completed = await AllocationTerminalAsync(state);
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreEqual(pendingGeneration, completed.Generation);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(3, scenario.ManagedRequestCount - managedBefore);
                Assert.AreEqual(4, scenario.GroupsRequestCount - groupsBefore);
                Assert.Greater(state.Snapshot.Generation, ready.Generation);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task NoOpFinalRefreshFailureInvalidatesAuthority()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.ThrowManagedRequest(
                scenario.ManagedRequestCount + 2);

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "Alpha",
                MutationScenario.CurrentAllocation(),
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                StringAssert.Contains("reconciliation", failed.ErrorMessage);
            });

            scenario.Client.connectionClosed();
            scenario.Client.nextValidId(123);
            Assert.IsTrue(state.IsGroupTradingBlocked);
            scenario.ThrowManagedRequest(
                scenario.ManagedRequestCount + 1);
            Assert.IsTrue(state.RequestRefresh(Array.Empty<string>()));
            await SnapshotErrorAsync(state, "simulated managed-account failure");
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
            });

            scenario.ThrowManagedRequest(0);
            scenario.Client.connectionClosed();
            scenario.Client.nextValidId(124);
            var reconciled = await ReadyAsync(state);
            Assert.AreEqual(
                BrokerageAccountSnapshotStatus.Ready,
                reconciled.Status);
            Assert.IsFalse(state.IsGroupTradingBlocked);
        }

        [TestCase(1)]
        [TestCase(2)]
        public async Task MutationTopologyDriftBeforeWireInvalidatesAuthority(
            int managedReadOffset)
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.BlockManagedRequest(
                scenario.ManagedRequestCount + managedReadOffset);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            Assert.IsTrue(
                scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            scenario.SetCurrentGroupsXml(MutationScenario.DriftedGroupsXml);
            scenario.ReleaseManagedRequest();
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                StringAssert.Contains("refresh and retry", failed.ErrorMessage);
            });
        }

        [Test]
        public async Task FinalMemberAssignmentFailureKeepsReadyAuthority()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>1</amount></Account>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts><String>ACC2</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new MutationScenario();
            scenario.SetTopology("MASTER,ACC1,ACC2", groups);
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(state.RequestGroupAssignment(
                "ACC1",
                "Beta",
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var failed = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Failed,
                    failed.Status);
                StringAssert.Contains("final account", failed.ErrorMessage);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreSame(ready, state.Snapshot);
                Assert.AreEqual(
                    ready.MembershipHash,
                    state.Snapshot.MembershipHash);
                Assert.AreEqual(
                    ready.GroupConfigurationVersion,
                    state.Snapshot.GroupConfigurationVersion);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task UnsupportedTargetTemplateFailureKeepsReadyAuthority()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts>
                      <Account custom="preserve"><acct>ACC2</acct></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new MutationScenario();
            scenario.SetTopology("MASTER,ACC1,ACC2", groups);
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(state.RequestGroupAssignment(
                "ACC1",
                "Beta",
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var failed = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Failed,
                    failed.Status);
                StringAssert.Contains("unsupported metadata", failed.ErrorMessage);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreSame(ready, state.Snapshot);
                Assert.AreEqual(
                    ready.MembershipHash,
                    state.Snapshot.MembershipHash);
                Assert.AreEqual(
                    ready.GroupConfigurationVersion,
                    state.Snapshot.GroupConfigurationVersion);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [TestCase(1)]
        [TestCase(2)]
        public async Task MalformedMutationTopologyReadInvalidatesAuthority(
            int groupsReadOffset)
        {
            const string malformedGroups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>1</amount></Account>
                      <Account><acct>ACC2</acct><amount>2</amount></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.OverrideGroupsResponse(
                scenario.GroupsRequestCount + groupsReadOffset,
                malformedGroups);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                StringAssert.Contains("contained no allocation method", failed.ErrorMessage);
                StringAssert.Contains("reconciliation", failed.ErrorMessage);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task MalformedNoOpConfirmationReadInvalidatesAuthority()
        {
            const string malformedGroups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>1</amount></Account>
                      <Account><acct>ACC2</acct><amount>2</amount></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.OverrideGroupsResponse(
                scenario.GroupsRequestCount + 2,
                malformedGroups);

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "Alpha",
                MutationScenario.CurrentAllocation(),
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                StringAssert.Contains("contained no allocation method", failed.ErrorMessage);
                StringAssert.Contains("reconciliation", failed.ErrorMessage);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task DisconnectAfterFinalRefreshCannotPublishSuccess()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            using var publicationEntered = new ManualResetEventSlim();
            using var releasePublication = new ManualResetEventSlim();
            scenario.Actions.BeforeMutationPublication = () =>
            {
                publicationEntered.Set();
                if (!releasePublication.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Test mutation publication was not released.");
                }
            };

            try
            {
                Assert.IsTrue(RequestChangedAllocation(state, ready));
                Assert.IsTrue(publicationEntered.Wait(TimeSpan.FromSeconds(10)));
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Ready,
                        state.Snapshot.Status);
                    Assert.Greater(state.Snapshot.Generation, ready.Generation);
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Pending,
                        state.GroupAllocationUpdate.Status);
                    Assert.IsTrue(state.IsGroupTradingBlocked);
                });

                scenario.Client.connectionClosed();
                scenario.Client.nextValidId(123);
                releasePublication.Set();
                var failed = await AllocationTerminalAsync(state);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        state.Snapshot.Status);
                    Assert.IsTrue(state.IsGroupTradingBlocked);
                    Assert.IsEmpty(failed.ResultingGroupConfigurationVersion);
                    StringAssert.Contains("reconciliation", failed.ErrorMessage);
                    Assert.IsNull(GetPendingMutation(state));
                });
            }
            finally
            {
                releasePublication.Set();
            }
        }

        [Test]
        public async Task MutationPendingRejectsRefreshAndSecondMutation()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.BlockNextManagedRequest();

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            Assert.IsTrue(scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Pending,
                    state.GroupAllocationUpdate.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                Assert.IsFalse(state.RequestRefresh(Array.Empty<string>()));
                Assert.IsFalse(state.RequestGroupAssignment(
                    "ACC1",
                    "Alpha",
                    ready.MembershipHash,
                    ready.GroupConfigurationVersion));
                Assert.IsFalse(state.RequestGroupAllocationUpdate(
                    "Alpha",
                    MutationScenario.CurrentAllocation(),
                    ready.MembershipHash,
                    ready.GroupConfigurationVersion));
            });
            scenario.ReleaseManagedRequest();
            Assert.AreEqual(
                BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                (await AllocationTerminalAsync(state)).Status);
        }

        [Test]
        public async Task DisposeTerminalizesPendingMutationWithoutChangingInputs()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.BlockNextManagedRequest();
            var requested = new Dictionary<string, decimal>
            {
                ["ACC1"] = 3m,
                ["ACC2"] = 4m
            };

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "Alpha",
                requested,
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            Assert.IsTrue(
                scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            var pending = state.GroupAllocationUpdate;
            Assert.AreEqual(
                BrokerageAccountGroupAllocationUpdateStatus.Pending,
                pending.Status);
            var worker = GetWorker(state);
            try
            {
                state.Dispose();
                var failed = state.GroupAllocationUpdate;
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    Assert.AreEqual(pending.Generation, failed.Generation);
                    Assert.AreEqual(pending.GroupName, failed.GroupName);
                    Assert.AreEqual(
                        pending.AllocationMethod,
                        failed.AllocationMethod);
                    CollectionAssert.AreEquivalent(
                        pending.RequestedAccountAllocationValues,
                        failed.RequestedAccountAllocationValues);
                    Assert.AreEqual(
                        pending.ExpectedMembershipHash,
                        failed.ExpectedMembershipHash);
                    Assert.AreEqual(
                        pending.ExpectedGroupConfigurationVersion,
                        failed.ExpectedGroupConfigurationVersion);
                    StringAssert.Contains("disposed", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        state.Snapshot.Status);
                    StringAssert.Contains(
                        "disposed",
                        state.Snapshot.ErrorMessage);
                    Assert.IsNull(GetPendingMutation(state));
                });
            }
            finally
            {
                scenario.ReleaseManagedRequest();
            }
            await worker.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.AreEqual(
                BrokerageAccountGroupAllocationUpdateStatus.Failed,
                state.GroupAllocationUpdate.Status);
        }

        [Test]
        public async Task DisposeTerminalizesPendingAssignmentWithoutChangingInputs()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.BlockNextManagedRequest();

            Assert.IsTrue(state.RequestGroupAssignment(
                "ACC1",
                string.Empty,
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            Assert.IsTrue(
                scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            var pending = state.GroupAssignment;
            Assert.AreEqual(
                BrokerageAccountGroupAssignmentStatus.Pending,
                pending.Status);
            var worker = GetWorker(state);
            try
            {
                state.Dispose();
                var failed = state.GroupAssignment;
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAssignmentStatus.Failed,
                        failed.Status);
                    Assert.AreEqual(pending.Generation, failed.Generation);
                    Assert.AreEqual(pending.AccountId, failed.AccountId);
                    Assert.AreEqual(
                        pending.TargetGroupName,
                        failed.TargetGroupName);
                    CollectionAssert.AreEquivalent(
                        pending.PreviousGroupNames,
                        failed.PreviousGroupNames);
                    Assert.AreEqual(
                        pending.ExpectedMembershipHash,
                        failed.ExpectedMembershipHash);
                    Assert.AreEqual(
                        pending.ExpectedGroupConfigurationVersion,
                        failed.ExpectedGroupConfigurationVersion);
                    Assert.AreEqual(
                        pending.TargetAllocationValue,
                        failed.TargetAllocationValue);
                    StringAssert.Contains("disposed", failed.ErrorMessage);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        state.Snapshot.Status);
                    StringAssert.Contains(
                        "disposed",
                        state.Snapshot.ErrorMessage);
                    Assert.IsNull(GetPendingMutation(state));
                });
            }
            finally
            {
                scenario.ReleaseManagedRequest();
            }
            await worker.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.AreEqual(
                BrokerageAccountGroupAssignmentStatus.Failed,
                state.GroupAssignment.Status);
        }

        [Test]
        public async Task AllocationPreservesCallerOrderWithCanonicalAccountCasing()
        {
            using var scenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.WrongIdsThenSuccess
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            scenario.BlockNextManagedRequest();
            var requested = new Dictionary<string, decimal>
            {
                ["acc2"] = 4m,
                ["acc1"] = 3m
            };

            Assert.IsTrue(state.RequestGroupAllocationUpdate(
                "alpha",
                requested,
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            Assert.IsTrue(scenario.ManagedRequestEntered.Wait(TimeSpan.FromSeconds(10)));
            var pending = state.GroupAllocationUpdate;
            CollectionAssert.AreEqual(
                new[] { "ACC2", "ACC1" },
                pending.RequestedAccountAllocationValues.Keys);
            var generation = pending.Generation;
            scenario.ReleaseManagedRequest();
            Assert.IsTrue(
                scenario.WrongReplacementCallbacksSent.Wait(
                    TimeSpan.FromSeconds(10)));
            var pendingReplacement = GetPendingRequest(state);
            Assert.Multiple(() =>
            {
                Assert.LessOrEqual(scenario.ReplacementRequestId, -2);
                Assert.LessOrEqual(scenario.WrongReplacementRequestId, -2);
                Assert.AreNotEqual(
                    scenario.ReplacementRequestId,
                    scenario.WrongReplacementRequestId);
                Assert.IsNotNull(pendingReplacement);
                Assert.AreEqual(
                    scenario.ReplacementRequestId,
                    GetPendingRequestId(pendingReplacement));
                Assert.IsFalse(GetPendingFinished(pendingReplacement));
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Pending,
                    state.GroupAllocationUpdate.Status);
            });
            scenario.CompleteExactReplacement();

            var completed = await AllocationTerminalAsync(state);
            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreEqual(generation, completed.Generation);
                CollectionAssert.AreEqual(
                    new[] { "ACC2", "ACC1" },
                    completed.RequestedAccountAllocationValues.Keys);
                Assert.AreEqual(1, scenario.ReplaceCount);
                Assert.AreEqual(1, scenario.MaximumConcurrentExternalCalls);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
            });
        }

        [Test]
        public async Task AssignmentMutationUsesSemanticReadbackAndFullRefresh()
        {
            const string groups = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>1</amount></Account>
                      <Account><acct>ACC2</acct><amount>2</amount></Account>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Beta</name>
                    <defaultMethod>Equal</defaultMethod>
                    <ListOfAccts><String>ACC3</String><String>ACC4</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            using var scenario = new MutationScenario();
            scenario.SetTopology(
                "MASTER,ACC1,ACC2,ACC3,ACC4",
                groups);
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);

            Assert.IsTrue(state.RequestGroupAssignment(
                "acc1",
                "beta",
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var pending = state.GroupAssignment;
            var terminal = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Succeeded,
                    terminal.Status);
                Assert.AreEqual(pending.Generation, terminal.Generation);
                Assert.AreEqual("ACC1", terminal.AccountId);
                Assert.AreEqual("Beta", terminal.TargetGroupName);
                CollectionAssert.AreEqual(
                    new[] { "Alpha" }, terminal.PreviousGroupNames);
                CollectionAssert.AreEqual(
                    new[] { "Beta" }, terminal.ResultingGroupNames);
                Assert.AreEqual(1, scenario.ReplaceCount);
                Assert.AreEqual(2, scenario.OpenOrderCheckCount);
                Assert.Greater(state.Snapshot.Generation, ready.Generation);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task CorrelatedEndAloneCannotReplaceSemanticReadback()
        {
            using var scenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.EndWithoutApplying
            };
            using var state = scenario.CreateState(
                TimeSpan.FromMilliseconds(150));
            var ready = await ReadyAsync(state);

            var elapsed = Stopwatch.StartNew();
            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);
            elapsed.Stop();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                StringAssert.Contains("may have applied", failed.ErrorMessage);
                StringAssert.Contains(
                    "did not confirm the requested", failed.ErrorMessage);
                Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                Assert.Less(elapsed.Elapsed, TimeSpan.FromSeconds(5));
            });
        }

        [Test]
        public async Task SemanticReadbackRetriesStaleResponsesThenSucceeds()
        {
            using var scenario = new MutationScenario
            {
                StaleReadbacksBeforeApply = 2
            };
            using var state = scenario.CreateState(TimeSpan.FromSeconds(5));
            var ready = await ReadyAsync(state);

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var completed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreEqual(2, scenario.StaleReadbackCount);
                Assert.AreEqual(1, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
                Assert.IsFalse(state.IsGroupTradingBlocked);
            });
        }

        [Test]
        public async Task OnlyInvalidAccountsErrorIsDefinitiveReplacementRejection()
        {
            using (var rejectedScenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.InvalidAccountsError
            })
            using (var rejectedState = rejectedScenario.CreateState())
            {
                var ready = await ReadyAsync(rejectedState);
                Assert.IsTrue(RequestChangedAllocation(rejectedState, ready));
                var failed = await AllocationTerminalAsync(rejectedState);
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    StringAssert.Contains("10231", failed.ErrorMessage);
                    StringAssert.DoesNotContain("may have applied", failed.ErrorMessage);
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready,
                        rejectedState.Snapshot.Status);
                    Assert.IsFalse(rejectedState.IsGroupTradingBlocked);
                });
            }

            using (var uncertainScenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.OtherError
            })
            using (var uncertainState = uncertainScenario.CreateState())
            {
                var ready = await ReadyAsync(uncertainState);
                Assert.IsTrue(RequestChangedAllocation(uncertainState, ready));
                var failed = await AllocationTerminalAsync(uncertainState);
                Assert.Multiple(() =>
                {
                    StringAssert.Contains("may have applied", failed.ErrorMessage);
                    Assert.AreEqual(BrokerageAccountSnapshotStatus.Stale,
                        uncertainState.Snapshot.Status);
                    Assert.IsTrue(uncertainState.IsGroupTradingBlocked);
                });
            }
        }

        [Test]
        public async Task FailedMutationPublishesTerminalStateAtomically()
        {
            using var scenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.ThrowBeforeAuthorization
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            using var publicationEntered = new ManualResetEventSlim();
            using var releasePublication = new ManualResetEventSlim();
            scenario.Actions.BeforeMutationPublication = () =>
            {
                publicationEntered.Set();
                if (!releasePublication.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Test mutation publication was not released.");
                }
            };
            var callbackLock = GetCallbackStateLock(state);

            try
            {
                Assert.IsTrue(RequestChangedAllocation(state, ready));
                Assert.IsTrue(publicationEntered.Wait(TimeSpan.FromSeconds(10)));
                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Pending,
                        state.GroupAllocationUpdate.Status);
                    Assert.IsFalse(state.RequestGroupAllocationUpdate(
                        "Alpha",
                        MutationScenario.CurrentAllocation(),
                        ready.MembershipHash,
                        ready.GroupConfigurationVersion));
                    Assert.IsTrue(state.IsGroupTradingBlocked);
                    Assert.IsNotNull(GetPendingMutation(state));
                });
                releasePublication.Set();
                var failed = await AllocationTerminalAsync(state);
                lock (callbackLock)
                {
                    Assert.Multiple(() =>
                    {
                        Assert.AreEqual(
                            BrokerageAccountGroupAllocationUpdateStatus.Failed,
                            failed.Status);
                        Assert.IsNull(GetPendingMutation(state));
                        Assert.AreEqual(
                            BrokerageAccountSnapshotStatus.Ready,
                            state.Snapshot.Status);
                        Assert.IsFalse(state.IsGroupTradingBlocked);
                    });
                }
            }
            finally
            {
                releasePublication.Set();
            }
        }

        [TestCase(false, 0)]
        [TestCase(true, 2)]
        public async Task MutationReconciliationUsesSummaryOrExactFallback(
            bool forceSummaryFallback,
            int expectedAccountUpdateRequests)
        {
            using var scenario = new MutationScenario();
            scenario.EnableAccountSummaries();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            var summaryRequests = scenario.AccountSummaryRequestCount;
            var summaryCancellations = scenario.AccountSummaryCancellationCount;
            var accountUpdateRequests = scenario.AccountUpdateRequestCount;
            scenario.EmitIncompleteAccountSummary = forceSummaryFallback;

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var completed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.Greater(
                    scenario.AccountSummaryRequestCount,
                    summaryRequests);
                Assert.AreEqual(
                    scenario.AccountSummaryRequestCount - summaryRequests,
                    scenario.AccountSummaryCancellationCount - summaryCancellations);
                Assert.AreEqual(
                    expectedAccountUpdateRequests,
                    scenario.AccountUpdateRequestCount - accountUpdateRequests);
                Assert.Greater(state.Snapshot.Generation, ready.Generation);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Ready,
                    state.Snapshot.Status);
            });
        }

        [Test]
        public async Task ScopedAssignmentAddsPreviouslyUnassignedAccountToGroup()
        {
            using var scenario = new MutationScenario();
            scenario.SetManagedAccounts("MASTER,ACC1,ACC2,ACC3");
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(
                state,
                new[] { "Alpha" },
                new[] { "ACC3" });

            Assert.IsTrue(state.RequestGroupAssignment(
                "acc3",
                "alpha",
                ready.MembershipHash,
                ready.GroupConfigurationVersion,
                3m));
            var completed = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Succeeded,
                    completed.Status);
                CollectionAssert.Contains(
                    state.Snapshot.Groups["Alpha"].AccountIds,
                    "ACC3");
                Assert.IsTrue(state.Snapshot.Accounts.ContainsKey("ACC3"));
                CollectionAssert.DoesNotContain(
                    state.Snapshot.UnassignedAccountIds,
                    "ACC3");
            });
        }

        [Test]
        public async Task ScopedAssignmentToNoGroupRetainsAccountState()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state, new[] { "Alpha" });

            Assert.IsTrue(state.RequestGroupAssignment(
                "ACC1",
                string.Empty,
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var completed = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Succeeded,
                    completed.Status);
                CollectionAssert.DoesNotContain(
                    state.Snapshot.Groups["Alpha"].AccountIds,
                    "ACC1");
                Assert.IsTrue(state.Snapshot.Accounts.ContainsKey("ACC1"));
                CollectionAssert.Contains(
                    state.Snapshot.UnassignedAccountIds,
                    "ACC1");
            });
        }

        [TestCase("managed accounts")]
        [TestCase("aliases")]
        [TestCase("family codes")]
        public async Task IdentityDriftBeforeMutationWireInvalidatesAuthority(
            string drift)
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            switch (drift)
            {
                case "managed accounts":
                    scenario.SetManagedAccounts("MASTER,ACC1,ACC2,ACC3");
                    break;
                case "aliases":
                    scenario.SetAliasesXml("""
                        <ListOfAccountAliases>
                          <AccountAlias><account>ACC1</account><alias>Client One</alias></AccountAlias>
                        </ListOfAccountAliases>
                        """);
                    break;
                default:
                    scenario.SetFamilyCodes(new FamilyCode
                    {
                        AccountID = "ACC1",
                        FamilyCodeStr = "Family-One"
                    });
                    break;
            }

            Assert.IsTrue(RequestChangedAllocation(state, ready));
            var failed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Failed,
                    failed.Status);
                Assert.AreEqual(0, scenario.ReplaceCount);
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Stale,
                    state.Snapshot.Status);
                Assert.IsTrue(state.IsGroupTradingBlocked);
                StringAssert.Contains("refresh and retry", failed.ErrorMessage);
            });
        }

        [Test]
        public async Task BrokerageWrappersPreserveConfiguredGroupScope()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState(configuredGroup: "Alpha");
            var ready = await ReadyAsync(state);
            using var brokerage = CreateBrokerageWithFinancialAdvisorState(
                state,
                "Alpha");

            Assert.Multiple(() =>
            {
                Assert.IsFalse(brokerage.RequestAccountSnapshotRefresh(
                    new[] { "Beta" },
                    Array.Empty<string>()));
                Assert.IsFalse(brokerage.RequestAccountSnapshotRefresh(
                    new[] { "alpha" },
                    new[] { "ACC1" }));
                Assert.IsFalse(brokerage.RequestAccountGroupAllocationUpdate(
                    "Beta",
                    MutationScenario.CurrentAllocation(),
                    ready.MembershipHash,
                    ready.GroupConfigurationVersion));
            });

            Assert.IsTrue(brokerage.RequestAccountSnapshotRefresh(
                new[] { "alpha" },
                Array.Empty<string>()));
            var refreshed = await WaitForReadyGenerationAsync(
                state,
                ready.Generation);
            Assert.AreEqual(BrokerageAccountSnapshotStatus.Ready, refreshed.Status);
        }

        [Test]
        public async Task BrokerageWrappersForwardEnabledMutationsIncludingEmptyTarget()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState(configuredGroup: "Alpha");
            var ready = await ReadyAsync(state);
            using var brokerage = CreateBrokerageWithFinancialAdvisorState(
                state,
                "Alpha");

            Assert.IsTrue(brokerage.RequestAccountGroupAssignment(
                "ACC1",
                string.Empty,
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var completed = await AssignmentTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Succeeded,
                    completed.Status);
                Assert.AreSame(
                    completed,
                    brokerage.GetAccountGroupAssignment());
                Assert.AreSame(state.Snapshot, brokerage.GetAccountSnapshot());
            });
        }

        [Test]
        public async Task BrokerageWrapperForwardsEnabledAllocationUpdate()
        {
            using var scenario = new MutationScenario();
            using var state = scenario.CreateState(configuredGroup: "Alpha");
            var ready = await ReadyAsync(state);
            using var brokerage = CreateBrokerageWithFinancialAdvisorState(
                state,
                "Alpha");

            Assert.IsTrue(brokerage.RequestAccountGroupAllocationUpdate(
                "alpha",
                new Dictionary<string, decimal>
                {
                    ["ACC1"] = 3m,
                    ["ACC2"] = 4m
                },
                ready.MembershipHash,
                ready.GroupConfigurationVersion));
            var completed = await AllocationTerminalAsync(state);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded,
                    completed.Status);
                Assert.AreSame(
                    completed,
                    brokerage.GetAccountGroupAllocationUpdate());
                Assert.AreSame(state.Snapshot, brokerage.GetAccountSnapshot());
            });
        }

        [Test]
        public void BrokerageWrappersAreUnavailableWhenManagementIsDisabled()
        {
            using var brokerage = new InteractiveBrokersBrokerage();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(
                    BrokerageAccountSnapshotStatus.Unavailable,
                    brokerage.GetAccountSnapshot().Status);
                Assert.AreEqual(
                    BrokerageAccountGroupAssignmentStatus.Unavailable,
                    brokerage.GetAccountGroupAssignment().Status);
                Assert.AreEqual(
                    BrokerageAccountGroupAllocationUpdateStatus.Unavailable,
                    brokerage.GetAccountGroupAllocationUpdate().Status);
                Assert.IsFalse(brokerage.RequestAccountSnapshotRefresh(
                    Array.Empty<string>(),
                    Array.Empty<string>()));
                Assert.IsFalse(brokerage.RequestAccountGroupAssignment(
                    "ACC1", "Alpha", "membership", "configuration"));
                Assert.IsFalse(brokerage.RequestAccountGroupAllocationUpdate(
                    "Alpha",
                    MutationScenario.CurrentAllocation(),
                    "membership",
                    "configuration"));
            });
        }

        [Test]
        public async Task FastReconnectVersionChangeInvalidatesFailedMutation()
        {
            using var scenario = new MutationScenario
            {
                Replacement = ReplacementBehavior.ThrowBeforeAuthorization
            };
            using var state = scenario.CreateState();
            var ready = await ReadyAsync(state);
            using var publicationEntered = new ManualResetEventSlim();
            using var releasePublication = new ManualResetEventSlim();
            scenario.Actions.BeforeMutationPublication = () =>
            {
                publicationEntered.Set();
                if (!releasePublication.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Test mutation publication was not released.");
                }
            };

            try
            {
                Assert.IsTrue(RequestChangedAllocation(state, ready));
                Assert.IsTrue(publicationEntered.Wait(TimeSpan.FromSeconds(10)));
                scenario.Client.connectionClosed();
                scenario.Client.nextValidId(124);
                releasePublication.Set();
                var failed = await AllocationTerminalAsync(state);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountGroupAllocationUpdateStatus.Failed,
                        failed.Status);
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Stale,
                        state.Snapshot.Status);
                    Assert.IsTrue(state.IsGroupTradingBlocked);
                    StringAssert.Contains("reconciliation", failed.ErrorMessage);
                });
            }
            finally
            {
                releasePublication.Set();
            }
        }

        private static bool RequestChangedAllocation(
            InteractiveBrokersFinancialAdvisorAccountState state,
            BrokerageAccountSnapshot snapshot) =>
            state.RequestGroupAllocationUpdate(
                "Alpha",
                new Dictionary<string, decimal>
                {
                    ["ACC1"] = 3m,
                    ["ACC2"] = 4m
                },
                snapshot.MembershipHash,
                snapshot.GroupConfigurationVersion);

        private static async Task<BrokerageAccountSnapshot> ReadyAsync(
            InteractiveBrokersFinancialAdvisorAccountState state,
            IReadOnlyCollection<string> groupNames = null,
            IReadOnlyCollection<string> additionalAccountIds = null)
        {
            var previousGeneration = state.Snapshot.Generation;
            var requested = additionalAccountIds == null
                ? state.RequestRefresh(groupNames ?? Array.Empty<string>())
                : state.RequestRefresh(
                    groupNames ?? Array.Empty<string>(),
                    additionalAccountIds);
            Assert.IsTrue(requested);
            return await WaitForReadyGenerationAsync(state, previousGeneration);
        }

        private static async Task<BrokerageAccountSnapshot> WaitForReadyGenerationAsync(
            InteractiveBrokersFinancialAdvisorAccountState state,
            long previousGeneration)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                var snapshot = state.Snapshot;
                if (snapshot.Generation > previousGeneration &&
                    snapshot.Status == BrokerageAccountSnapshotStatus.Ready)
                {
                    return snapshot;
                }
                if (snapshot.Generation > previousGeneration &&
                    snapshot.Status == BrokerageAccountSnapshotStatus.Failed)
                {
                    Assert.Fail(snapshot.ErrorMessage);
                }
                await Task.Delay(5);
            }
            throw new TimeoutException("A Ready Financial Advisor snapshot was not published.");
        }

        private static InteractiveBrokersBrokerage
            CreateBrokerageWithFinancialAdvisorState(
                InteractiveBrokersFinancialAdvisorAccountState state,
                string configuredGroup)
        {
            var brokerage = new InteractiveBrokersBrokerage();
            SetBrokerageField(
                brokerage,
                "_financialAdvisorAccountState",
                state);
            SetBrokerageField(
                brokerage,
                "_financialAdvisorGroupManagementEnabled",
                true);
            SetBrokerageField(
                brokerage,
                "_financialAdvisorsGroupFilter",
                configuredGroup);
            return brokerage;
        }

        private static void SetBrokerageField(
            InteractiveBrokersBrokerage brokerage,
            string name,
            object value)
        {
            var field = typeof(InteractiveBrokersBrokerage).GetField(
                name,
                BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(field, $"Expected brokerage field '{name}'.");
            field.SetValue(brokerage, value);
        }

        private static async Task<BrokerageAccountGroupAllocationUpdate>
            AllocationTerminalAsync(
                InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                var result = state.GroupAllocationUpdate;
                if (result.Status is
                    BrokerageAccountGroupAllocationUpdateStatus.Succeeded or
                    BrokerageAccountGroupAllocationUpdateStatus.Failed)
                {
                    return result;
                }
                await Task.Delay(5);
            }
            throw new TimeoutException(
                "The Financial Advisor allocation mutation did not become terminal.");
        }

        private static async Task<BrokerageAccountGroupAssignment>
            AssignmentTerminalAsync(
                InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                var result = state.GroupAssignment;
                if (result.Status is
                    BrokerageAccountGroupAssignmentStatus.Succeeded or
                    BrokerageAccountGroupAssignmentStatus.Failed)
                {
                    return result;
                }
                await Task.Delay(5);
            }
            throw new TimeoutException(
                "The Financial Advisor assignment mutation did not become terminal.");
        }

        private static async Task SnapshotErrorAsync(
            InteractiveBrokersFinancialAdvisorAccountState state,
            string expectedError)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                var snapshot = state.Snapshot;
                if ((snapshot.Status is BrokerageAccountSnapshotStatus.Failed or
                    BrokerageAccountSnapshotStatus.Stale) &&
                    snapshot.ErrorMessage.Contains(
                        expectedError,
                        StringComparison.Ordinal))
                {
                    return;
                }
                await Task.Delay(5);
            }
            throw new TimeoutException(
                "The expected Financial Advisor snapshot failure was not published.");
        }

        private static long GetRequestVersion(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            (long)GetRequestVersionField().GetValue(state);

        private static FieldInfo GetRequestVersionField() =>
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_requestVersion",
                BindingFlags.Instance | BindingFlags.NonPublic);

        private static object GetCallbackStateLock(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_callbackStateLock",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(state);

        private static object GetPendingMutation(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_pendingMutation",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(state);

        private static Task GetWorker(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            (Task)typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_worker",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(state);

        private static object GetPendingRequest(
            InteractiveBrokersFinancialAdvisorAccountState state) =>
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_pendingRequest",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(state);

        private static int GetPendingRequestId(object pendingRequest) =>
            (int)pendingRequest.GetType().GetProperty(
                "RequestId",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(pendingRequest);

        private static bool GetPendingFinished(object pendingRequest) =>
            (bool)pendingRequest.GetType().GetProperty(
                "Finished",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(pendingRequest);

        private enum ReplacementBehavior
        {
            Success,
            WrongIdsThenSuccess,
            ThrowBeforeAuthorization,
            ThrowAfterAuthorization,
            BlockAfterAuthorization,
            ReturnWithoutCompletion,
            EndWithoutApplying,
            InvalidAccountsError,
            OtherError
        }

        private sealed class MutationScenario : IDisposable
        {
            internal const string GroupsXml = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>1</amount></Account>
                      <Account><acct>ACC2</acct><amount>2</amount></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            internal const string DriftedGroupsXml = """
                <ListOfGroups>
                  <Group>
                    <name>Alpha</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts>
                      <Account><acct>ACC1</acct><amount>9</amount></Account>
                      <Account><acct>ACC2</acct><amount>2</amount></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;
            private const string EmptyAliasesXml = "<ListOfAccountAliases />";

            internal InteractiveBrokersClient Client { get; }
            internal InteractiveBrokersFinancialAdvisorAccountState.RequestActions Actions
                { get; }
            internal ReplacementBehavior Replacement { get; set; }
            internal int ServerVersion { get; set; } = int.MaxValue;
            internal Func<int, bool> OpenOrderResult { get; set; } = _ => false;
            internal int StaleReadbacksBeforeApply { get; set; }
            internal int UnsavedChangesReadbacksBeforeApply
            {
                get => Volatile.Read(ref _unsavedChangesReadbacksRemaining);
                set => Volatile.Write(ref _unsavedChangesReadbacksRemaining, value);
            }
            internal int UnsavedChangesRequestId { get; set; } = -1;
            internal object CallbackStateLock { get; set; }
            internal ManualResetEventSlim ManagedRequestEntered { get; } = new();
            internal ManualResetEventSlim ReplacementAuthorized { get; } = new();
            internal ManualResetEventSlim WrongReplacementCallbacksSent { get; } = new();
            internal int ManagedRequestCount => Volatile.Read(ref _managedRequestCount);
            internal int GroupsRequestCount => Volatile.Read(ref _groupsRequestCount);
            internal int ReplaceCount => Volatile.Read(ref _replaceCount);
            internal int OpenOrderCheckCount => Volatile.Read(ref _openOrderCheckCount);
            internal int MaximumConcurrentExternalCalls =>
                Volatile.Read(ref _maximumConcurrentExternalCalls);
            internal int ReplacementRequestId =>
                Volatile.Read(ref _replacementRequestId);
            internal int WrongReplacementRequestId =>
                Volatile.Read(ref _wrongReplacementRequestId);
            internal bool OpenOrderObservedUnderMonitor =>
                Volatile.Read(ref _openOrderObservedUnderMonitor) != 0;
            internal bool ServerVersionObservedUnderMonitor =>
                Volatile.Read(ref _serverVersionObservedUnderMonitor) != 0;
            internal bool ReplaceObservedUnderMonitor =>
                Volatile.Read(ref _replaceObservedUnderMonitor) != 0;
            internal int StaleReadbackCount =>
                Volatile.Read(ref _staleReadbackCount);
            internal int UnsavedChangesReadbackCount =>
                Volatile.Read(ref _unsavedChangesReadbackCount);
            internal int AccountSummaryRequestCount =>
                Volatile.Read(ref _accountSummaryRequestCount);
            internal int AccountSummaryCancellationCount =>
                Volatile.Read(ref _accountSummaryCancellationCount);
            internal int AccountUpdateRequestCount =>
                Volatile.Read(ref _accountUpdateRequestCount);
            internal bool EmitIncompleteAccountSummary { get; set; }
            private string _currentGroupsXml = GroupsXml;
            private string _replacementXml;
            private int _managedRequestCount;
            private int _groupsRequestCount;
            private int _replaceCount;
            private int _openOrderCheckCount;
            private int _activeExternalCalls;
            private int _maximumConcurrentExternalCalls;
            private int _blockManagedRequestNumber;
            private int _throwManagedRequestNumber;
            private int _replacementRequestId;
            private int _wrongReplacementRequestId;
            private int _openOrderObservedUnderMonitor;
            private int _serverVersionObservedUnderMonitor;
            private int _replaceObservedUnderMonitor;
            private int _staleReadbacksRemaining;
            private int _staleReadbackCount;
            private int _unsavedChangesReadbacksRemaining;
            private int _unsavedChangesReadbackCount;
            private int _accountSummaryRequestCount;
            private int _accountSummaryCancellationCount;
            private int _accountUpdateRequestCount;
            private int _overrideGroupsRequestNumber;
            private string _overrideGroupsXml;
            private string _managedAccounts = "MASTER,ACC1,ACC2";
            private string _aliasesXml = EmptyAliasesXml;
            private FamilyCode[] _familyCodes = Array.Empty<FamilyCode>();
            private readonly ManualResetEventSlim _releaseManagedRequest = new(true);
            private readonly ManualResetEventSlim _releaseReplacement = new();

            internal MutationScenario()
            {
                Client = new InteractiveBrokersClient(new EReaderMonitorSignal());
                Actions =
                    new InteractiveBrokersFinancialAdvisorAccountState.RequestActions(Client)
                    {
                        RequestManagedAccounts = authorize =>
                            RunAuthorized(authorize, () =>
                            {
                                var requestNumber =
                                    Interlocked.Increment(ref _managedRequestCount);
                                if (requestNumber ==
                                    Volatile.Read(ref _blockManagedRequestNumber))
                                {
                                    ManagedRequestEntered.Set();
                                    if (!_releaseManagedRequest.Wait(TimeSpan.FromSeconds(10)))
                                    {
                                        throw new TimeoutException(
                                            "Test managed-account request was not released.");
                                    }
                                }
                                if (requestNumber ==
                                    Volatile.Read(ref _throwManagedRequestNumber))
                                {
                                    throw new InvalidOperationException(
                                        "simulated managed-account failure");
                                }
                                Client.managedAccounts(_managedAccounts);
                            }),
                        RequestFinancialAdvisor = (faDataType, authorize) =>
                            RunAuthorized(authorize, () =>
                            {
                                if (faDataType == 1)
                                {
                                    var requestNumber = Interlocked.Increment(
                                        ref _groupsRequestCount);
                                    if (requestNumber == Volatile.Read(
                                            ref _overrideGroupsRequestNumber))
                                    {
                                        Client.receiveFA(
                                            faDataType,
                                            Volatile.Read(ref _overrideGroupsXml));
                                        return;
                                    }
                                    var replacementXml =
                                        Volatile.Read(ref _replacementXml);
                                    if (replacementXml != null)
                                    {
                                        if (Volatile.Read(
                                            ref _unsavedChangesReadbacksRemaining) > 0)
                                        {
                                            Interlocked.Decrement(
                                                ref _unsavedChangesReadbacksRemaining);
                                            Interlocked.Increment(
                                                ref _unsavedChangesReadbackCount);
                                            Client.error(
                                                UnsavedChangesRequestId, 0, 10230,
                                                "unsaved FA changes", string.Empty);
                                            return;
                                        }
                                        if (Volatile.Read(
                                            ref _staleReadbacksRemaining) > 0)
                                        {
                                            Interlocked.Decrement(
                                                ref _staleReadbacksRemaining);
                                            Interlocked.Increment(
                                                ref _staleReadbackCount);
                                        }
                                        else
                                        {
                                            Volatile.Write(
                                                ref _currentGroupsXml,
                                                replacementXml);
                                            Volatile.Write(
                                                ref _replacementXml,
                                                null);
                                        }
                                    }
                                    Client.receiveFA(
                                        faDataType,
                                        Volatile.Read(ref _currentGroupsXml));
                                }
                                else
                                {
                                    Client.receiveFA(
                                        faDataType,
                                        Volatile.Read(ref _aliasesXml));
                                }
                            }),
                        RequestFamilyCodes = authorize =>
                            RunAuthorized(
                                authorize,
                                () => Client.familyCodes(
                                    Volatile.Read(ref _familyCodes))),
                        RequestPositions = (requestId, accountOrGroup, authorize) =>
                            RunAuthorized(authorize, () =>
                            {
                                Client.positionMultiEnd(requestId);
                            }),
                        CancelPositions = (_, authorize) =>
                            RunAuthorized(authorize, () => { }),
                        RequestAccountUpdates = (requestId, accountId, authorize) =>
                            RunAuthorized(
                                authorize,
                                () => EmitAccountValues(requestId, accountId)),
                        CancelAccountUpdates = (_, authorize) =>
                            RunAuthorized(authorize, () => { }),
                        RequestAccountSummary = null,
                        CancelAccountSummary = null,
                        GetServerVersion = GetServerVersion,
                        ReplaceFinancialAdvisor = ReplaceFinancialAdvisor
                    };
            }

            internal InteractiveBrokersFinancialAdvisorAccountState CreateState(
                TimeSpan? timeout = null,
                string configuredGroup = "") =>
                new(
                    Client,
                    () => { },
                    () => true,
                    contract => Symbol.Create(
                        contract.Symbol,
                        SecurityType.Equity,
                        Market.USA),
                    "MASTER",
                    configuredGroup,
                    requestTimeout: timeout ?? TimeSpan.FromSeconds(2),
                    hasOpenFinancialAdvisorOrders: HasOpenFinancialAdvisorOrders,
                    requestActions: Actions);

            internal static IReadOnlyDictionary<string, decimal> CurrentAllocation() =>
                new Dictionary<string, decimal>
                {
                    ["ACC1"] = 1m,
                    ["ACC2"] = 2m
                };

            internal void SetTopology(string managedAccounts, string groupsXml)
            {
                _managedAccounts = managedAccounts;
                Volatile.Write(ref _currentGroupsXml, groupsXml);
            }

            internal void SetCurrentGroupsXml(string groupsXml) =>
                Volatile.Write(ref _currentGroupsXml, groupsXml);

            internal void OverrideGroupsResponse(int requestNumber, string groupsXml)
            {
                Volatile.Write(ref _overrideGroupsXml, groupsXml);
                Volatile.Write(ref _overrideGroupsRequestNumber, requestNumber);
            }

            internal void SetManagedAccounts(string managedAccounts) =>
                _managedAccounts = managedAccounts;

            internal void SetAliasesXml(string aliasesXml) =>
                Volatile.Write(ref _aliasesXml, aliasesXml);

            internal void SetFamilyCodes(params FamilyCode[] familyCodes) =>
                Volatile.Write(ref _familyCodes, familyCodes);

            internal void EnableAccountSummaries()
            {
                Actions.RequestAccountSummary =
                    (requestId, _, _, authorize) => RunAuthorized(authorize, () =>
                    {
                        Interlocked.Increment(ref _accountSummaryRequestCount);
                        EmitAccountSummaryValues(
                            requestId,
                            "ACC1",
                            EmitIncompleteAccountSummary);
                        EmitAccountSummaryValues(requestId, "ACC2", false);
                        Client.accountSummary(
                            requestId, "All", "CashBalance", "250", "BASE");
                        Client.accountSummary(
                            requestId, "All", "CashBalance", "250", "USD");
                        Client.accountSummaryEnd(requestId);
                    });
                Actions.CancelAccountSummary = (requestId, authorize) =>
                    RunAuthorized(authorize, () =>
                        Interlocked.Increment(
                            ref _accountSummaryCancellationCount));
            }

            internal void BlockManagedRequest(int requestNumber)
            {
                ManagedRequestEntered.Reset();
                _releaseManagedRequest.Reset();
                Volatile.Write(ref _blockManagedRequestNumber, requestNumber);
            }

            internal void BlockNextManagedRequest() =>
                BlockManagedRequest(ManagedRequestCount + 1);

            internal void ThrowManagedRequest(int requestNumber) =>
                Volatile.Write(ref _throwManagedRequestNumber, requestNumber);

            internal void ReleaseManagedRequest() => _releaseManagedRequest.Set();

            internal void ReleaseReplacement() => _releaseReplacement.Set();

            internal void CompleteExactReplacement()
            {
                var replacementXml = Volatile.Read(ref _replacementXml);
                Assert.IsNotNull(replacementXml);
                Volatile.Write(ref _currentGroupsXml, replacementXml);
                Volatile.Write(ref _replacementXml, null);
                Client.replaceFAEnd(ReplacementRequestId, "saved");
            }

            private bool HasOpenFinancialAdvisorOrders()
            {
                var call = Interlocked.Increment(ref _openOrderCheckCount);
                var result = false;
                RunExternal(() =>
                {
                    ObserveMonitor(ref _openOrderObservedUnderMonitor);
                    result = OpenOrderResult(call);
                });
                return result;
            }

            private int GetServerVersion()
            {
                var serverVersion = 0;
                RunExternal(() =>
                {
                    ObserveMonitor(ref _serverVersionObservedUnderMonitor);
                    serverVersion = ServerVersion;
                });
                return serverVersion;
            }

            private bool ReplaceFinancialAdvisor(
                int requestId,
                int faDataType,
                string xml,
                Func<bool> authorize)
            {
                if (Replacement == ReplacementBehavior.ThrowBeforeAuthorization)
                {
                    throw new InvalidOperationException(
                        "replaceFA failed before authorization.");
                }
                return RunAuthorized(authorize, () =>
                {
                    ObserveMonitor(ref _replaceObservedUnderMonitor);
                    Interlocked.Increment(ref _replaceCount);
                    Volatile.Write(ref _replacementRequestId, requestId);
                    switch (Replacement)
                    {
                        case ReplacementBehavior.ThrowAfterAuthorization:
                            throw new InvalidOperationException(
                                "replaceFA failed after authorization.");

                        case ReplacementBehavior.BlockAfterAuthorization:
                            ReplacementAuthorized.Set();
                            if (!_releaseReplacement.Wait(TimeSpan.FromSeconds(10)))
                            {
                                throw new TimeoutException(
                                    "Test replacement request was not released.");
                            }
                            break;

                        case ReplacementBehavior.ReturnWithoutCompletion:
                            break;

                        case ReplacementBehavior.EndWithoutApplying:
                            Client.replaceFAEnd(requestId, "saved");
                            break;

                        case ReplacementBehavior.InvalidAccountsError:
                            Client.error(
                                requestId,
                                0,
                                10231,
                                "invalid account list",
                                string.Empty);
                            break;

                        case ReplacementBehavior.OtherError:
                            Client.error(
                                requestId,
                                0,
                                500,
                                "uncertain replacement failure",
                                string.Empty);
                            break;

                        case ReplacementBehavior.WrongIdsThenSuccess:
                            var wrongRequestId = requestId - 1;
                            Volatile.Write(
                                ref _wrongReplacementRequestId,
                                wrongRequestId);
                            Volatile.Write(ref _replacementXml, xml);
                            Client.error(
                                wrongRequestId,
                                0,
                                10231,
                                "wrong request",
                                string.Empty);
                            Client.replaceFAEnd(
                                wrongRequestId,
                                "wrong request");
                            WrongReplacementCallbacksSent.Set();
                            break;

                        default:
                            if (StaleReadbacksBeforeApply > 0 ||
                                UnsavedChangesReadbacksBeforeApply > 0)
                            {
                                Volatile.Write(ref _replacementXml, xml);
                                Volatile.Write(
                                    ref _staleReadbacksRemaining,
                                    StaleReadbacksBeforeApply);
                            }
                            else
                            {
                                Volatile.Write(ref _currentGroupsXml, xml);
                            }
                            Client.replaceFAEnd(requestId, "saved");
                            break;
                    }
                });
            }

            private void ObserveMonitor(ref int observed)
            {
                if (CallbackStateLock != null &&
                    Monitor.IsEntered(CallbackStateLock))
                {
                    Volatile.Write(ref observed, 1);
                }
            }

            private void EmitAccountValues(int requestId, string accountId)
            {
                Interlocked.Increment(ref _accountUpdateRequestCount);
                Client.accountUpdateMulti(
                    requestId,
                    accountId,
                    string.Empty,
                    "AccountReady",
                    "true",
                    string.Empty);
                Client.accountUpdateMulti(
                    requestId,
                    accountId,
                    string.Empty,
                    "AccountType",
                    "INDIVIDUAL",
                    string.Empty);
                Client.accountUpdateMulti(
                    requestId,
                    accountId,
                    string.Empty,
                    "NetLiquidation",
                    "1000",
                    "USD");
                Client.accountUpdateMulti(
                    requestId,
                    accountId,
                    string.Empty,
                    "TotalCashValue",
                    "250",
                    "USD");
                Client.accountUpdateMulti(
                    requestId,
                    accountId,
                    string.Empty,
                    "$LEDGER-USD:CashBalance",
                    "250",
                    string.Empty);
                Client.accountUpdateMultiEnd(requestId);
            }

            private void EmitAccountSummaryValues(
                int requestId,
                string accountId,
                bool omitAccountReady)
            {
                Client.accountSummary(
                    requestId, accountId, "AccountType", "INDIVIDUAL", string.Empty);
                Client.accountSummary(
                    requestId, accountId, "NetLiquidation", "1000", "USD");
                Client.accountSummary(
                    requestId, accountId, "TotalCashValue", "250", "USD");
                Client.accountSummary(
                    requestId, accountId, "AvailableFunds", "200", "USD");
                Client.accountSummary(
                    requestId, accountId, "ExcessLiquidity", "175", "USD");
                Client.accountSummary(
                    requestId, accountId, "BuyingPower", "400", "USD");
                if (!omitAccountReady)
                {
                    Client.accountSummary(
                        requestId, accountId, "AccountReady", "true", string.Empty);
                }
                Client.accountSummary(
                    requestId, accountId, "CashBalance", "250", "BASE");
            }

            private bool RunAuthorized(Func<bool> authorize, Action action)
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
                var active = Interlocked.Increment(ref _activeExternalCalls);
                var observed = Volatile.Read(ref _maximumConcurrentExternalCalls);
                while (active > observed)
                {
                    var prior = Interlocked.CompareExchange(
                        ref _maximumConcurrentExternalCalls,
                        active,
                        observed);
                    if (prior == observed)
                    {
                        break;
                    }
                    observed = prior;
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

            public void Dispose()
            {
                _releaseManagedRequest.Set();
                _releaseReplacement.Set();
                ManagedRequestEntered.Dispose();
                ReplacementAuthorized.Dispose();
                WrongReplacementCallbacksSent.Dispose();
                _releaseManagedRequest.Dispose();
                _releaseReplacement.Dispose();
                Client.Dispose();
            }
        }
    }
}
