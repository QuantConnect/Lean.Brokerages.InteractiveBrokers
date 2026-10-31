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
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Read-only protocol assertions that complement the deterministic FA state-service tests.
    /// These tests use the real IB client and production request delegates against paper TWS.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class InteractiveBrokersFinancialAdvisorProtocolTests
    {
        private const string Host = "127.0.0.1";
        private const int Port = 7497;
        private const string AllowlistEnvironmentVariable =
            "IB_FA_PAPER_ACCOUNT_ALLOWLIST";
        private const string GroupEnvironmentVariable =
            "IB_FA_PAPER_ACCOUNT_SUMMARY_GROUP";
        private static readonly Regex PaperAccountPattern = new(
            "^D(?:F|FM|MF)[0-9]+A?$|^DUM[0-9]+$",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex FinancialAdvisorMasterPattern = new(
            "^D(?:F|FM|MF)[0-9]+A?$",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly string[] RequiredSummaryTags =
        {
            "AccountType",
            "NetLiquidation",
            "TotalCashValue",
            "AvailableFunds",
            "ExcessLiquidity",
            "BuyingPower",
            "AccountReady"
        };

        [Test]
        [Explicit(
            "Requires a locally logged-in TWS paper FA session on 127.0.0.1:7497, " +
            "an external account allowlist, and a named group.")]
        public async Task NamedGroupSnapshotUsesProductionSummaryProtocol()
        {
            var allowlist = ParseAccounts(Environment.GetEnvironmentVariable(
                AllowlistEnvironmentVariable));
            Assert.IsNotEmpty(
                allowlist,
                $"Set {AllowlistEnvironmentVariable} before running this explicit test.");
            Assert.IsTrue(allowlist.All(PaperAccountPattern.IsMatch));
            var requestedGroup = Environment.GetEnvironmentVariable(
                GroupEnvironmentVariable)?.Trim();
            Assert.IsNotEmpty(
                requestedGroup,
                $"Set {GroupEnvironmentVariable} to a named base-currency-only paper FA group.");
            Assert.IsFalse(requestedGroup.Equals(
                "All", StringComparison.OrdinalIgnoreCase));

            using var session = PaperSession.Connect();
            var managedAccounts = session.VerifyPaperAllowlist(allowlist);
            var masterAccount = managedAccounts.Single(account =>
                FinancialAdvisorMasterPattern.IsMatch(account));
            var childAccounts = managedAccounts
                .Where(account => !account.Equals(
                    masterAccount, StringComparison.OrdinalIgnoreCase))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(
                session.RequestGroupsXml());
            var group = groups.Values.FirstOrDefault(candidate =>
                candidate.Name.Equals(
                    requestedGroup, StringComparison.OrdinalIgnoreCase) &&
                !candidate.Name.Equals("All", StringComparison.OrdinalIgnoreCase) &&
                candidate.AccountIds.Count != 0 &&
                candidate.AccountIds.All(childAccounts.Contains));
            Assert.IsNotNull(
                group,
                "No non-All FA group was wholly contained in the verified paper allowlist. " +
                "Available groups: " + string.Join(", ", groups.Values.Select(candidate =>
                    $"{candidate.Name} ({candidate.AccountIds.Count} accounts)")));

            var summaryRequests = new ConcurrentQueue<SummaryRequest>();
            var summaryCancellations = new ConcurrentQueue<int>();
            var accountUpdateRequests = new ConcurrentQueue<int>();
            var summaryRows = new ConcurrentQueue<AccountSummaryEventArgs>();
            var errors = new ConcurrentQueue<ErrorEventArgs>();
            var requestActions =
                new InteractiveBrokersFinancialAdvisorAccountState.RequestActions(
                    session.Client);
            var requestSummary = requestActions.RequestAccountSummary;
            var cancelSummary = requestActions.CancelAccountSummary;
            var requestAccountUpdates = requestActions.RequestAccountUpdates;
            requestActions.RequestAccountSummary =
                (requestId, groupName, tags, authorize) =>
                {
                    var sent = requestSummary(
                        requestId, groupName, tags, authorize);
                    if (sent)
                    {
                        summaryRequests.Enqueue(new SummaryRequest(
                            requestId, groupName, tags));
                    }
                    return sent;
                };
            requestActions.CancelAccountSummary = (requestId, authorize) =>
            {
                var sent = cancelSummary(requestId, authorize);
                if (sent)
                {
                    summaryCancellations.Enqueue(requestId);
                }
                return sent;
            };
            requestActions.RequestAccountUpdates =
                (requestId, accountId, authorize) =>
                {
                    var sent = requestAccountUpdates(
                        requestId, accountId, authorize);
                    if (sent)
                    {
                        accountUpdateRequests.Enqueue(requestId);
                    }
                    return sent;
                };

            void SummaryHandler(object _, AccountSummaryEventArgs args)
            {
                if (args.RequestId < 0)
                {
                    summaryRows.Enqueue(args);
                }
            }
            void ErrorHandler(object _, ErrorEventArgs args) => errors.Enqueue(args);

            session.Client.AccountSummary += SummaryHandler;
            session.Client.Error += ErrorHandler;
            try
            {
                using var state = new InteractiveBrokersFinancialAdvisorAccountState(
                    session.Client,
                    () => { },
                    () => session.Client.Connected,
                    contract => Symbol.Create(
                        $"IB-{contract.ConId}", SecurityType.Base, Market.USA),
                    NormalizeManagedAccount(masterAccount),
                    group.Name,
                    requestTimeout: TimeSpan.FromSeconds(30),
                    requestActions: requestActions);

                Assert.IsTrue(state.RequestRefresh(new[] { group.Name }));
                var snapshot = await WaitForSnapshotAsync(state);

                Assert.Multiple(() =>
                {
                    Assert.AreEqual(
                        BrokerageAccountSnapshotStatus.Ready,
                        snapshot.Status,
                        "The production state service did not publish a Ready snapshot.");
                    Assert.IsTrue(snapshot.Groups.ContainsKey(group.Name));
                    Assert.AreEqual(group.AccountIds.Count, snapshot.Accounts.Count);
                    Assert.IsTrue(group.AccountIds.All(snapshot.Accounts.ContainsKey));
                    Assert.IsTrue(snapshot.Accounts.Values.All(account =>
                        account.CashBalances.Count != 0));
                    Assert.IsEmpty(
                        accountUpdateRequests,
                        "The verified named group unexpectedly used the per-account fallback.");
                });

                var request = summaryRequests.Single();
                var rows = summaryRows
                    .Where(row => row.RequestId == request.RequestId)
                    .ToArray();
                var requestedTags = request.Tags.Split(',');
                Assert.Multiple(() =>
                {
                    Assert.Less(request.RequestId, 0);
                    Assert.AreEqual(group.Name, request.GroupName);
                    Assert.AreNotEqual("All", request.GroupName);
                    Assert.IsTrue(requestedTags.Contains(
                        "$LEDGER", StringComparer.OrdinalIgnoreCase));
                    Assert.IsTrue(requestedTags.Contains(
                        "$LEDGER:ALL", StringComparer.OrdinalIgnoreCase));
                    Assert.IsTrue(RequiredSummaryTags.All(tag =>
                        requestedTags.Contains(tag, StringComparer.OrdinalIgnoreCase)));
                    CollectionAssert.Contains(
                        summaryCancellations,
                        request.RequestId);
                    Assert.IsFalse(errors.Any(error =>
                        error.Id == request.RequestId));
                });

                foreach (var accountId in group.AccountIds)
                {
                    var accountRows = rows.Where(row => row.Account.Equals(
                        accountId, StringComparison.OrdinalIgnoreCase)).ToArray();
                    foreach (var tag in RequiredSummaryTags)
                    {
                        Assert.AreEqual(
                            1,
                            accountRows.Count(row => row.Tag.Equals(
                                tag, StringComparison.OrdinalIgnoreCase)),
                            $"Expected one '{tag}' row for each selected group member.");
                    }
                    Assert.IsTrue(string.Equals(
                        accountRows.Single(row => row.Tag.Equals(
                            "AccountReady", StringComparison.OrdinalIgnoreCase)).Value,
                        "true",
                        StringComparison.OrdinalIgnoreCase));
                    var cashRows = accountRows.Where(row => row.Tag.Equals(
                        "CashBalance", StringComparison.OrdinalIgnoreCase)).ToArray();
                    Assert.That(cashRows.Length, Is.InRange(1, 2));
                    Assert.AreEqual(
                        cashRows.Length,
                        cashRows.Select(row => row.Currency)
                            .Distinct(StringComparer.OrdinalIgnoreCase).Count());
                }

                var allowedSummaryAccounts = group.AccountIds
                    .Concat(new[] { "All", masterAccount, masterAccount + "A" })
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                Assert.IsEmpty(rows.Where(row =>
                    !allowedSummaryAccounts.Contains(row.Account.Trim())));

                var aggregateCashRows = rows.Where(row =>
                    row.Account.Trim().Equals(
                        "All", StringComparison.OrdinalIgnoreCase) &&
                    row.Tag.Equals(
                        "CashBalance", StringComparison.OrdinalIgnoreCase)).ToArray();
                Assert.IsNotEmpty(aggregateCashRows);
            }
            finally
            {
                session.Client.AccountSummary -= SummaryHandler;
                session.Client.Error -= ErrorHandler;
            }
        }

        private static async Task<BrokerageAccountSnapshot> WaitForSnapshotAsync(
            InteractiveBrokersFinancialAdvisorAccountState state)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
            while (DateTime.UtcNow < deadline)
            {
                var snapshot = state.Snapshot;
                if (snapshot.Generation > 0 &&
                    snapshot.Status == BrokerageAccountSnapshotStatus.Ready)
                {
                    return snapshot;
                }
                if (snapshot.Generation > 0 &&
                    snapshot.Status is BrokerageAccountSnapshotStatus.Failed or
                        BrokerageAccountSnapshotStatus.Stale)
                {
                    Assert.Fail(
                        "The paper FA account snapshot failed before publication.");
                }
                await Task.Delay(100);
            }
            throw new TimeoutException(
                "Timed out waiting for the paper FA account snapshot.");
        }

        private static string[] ParseAccounts(string value) =>
            (value ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries |
                    StringSplitOptions.TrimEntries)
                .Select(NormalizeManagedAccount)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(account => account, StringComparer.OrdinalIgnoreCase)
                .ToArray();

        private static string NormalizeManagedAccount(string accountId)
        {
            if (accountId != null &&
                accountId.EndsWith("A", StringComparison.OrdinalIgnoreCase) &&
                FinancialAdvisorMasterPattern.IsMatch(accountId))
            {
                return accountId[..^1];
            }
            return accountId;
        }

        private sealed class PaperSession : IDisposable
        {
            private readonly EReaderMonitorSignal _signal = new();
            private readonly TaskCompletionSource<int> _nextValidId =
                NewCompletionSource<int>();
            private readonly TaskCompletionSource<string[]> _managedAccounts =
                NewCompletionSource<string[]>();
            private Thread _readerThread;

            internal InteractiveBrokersClient Client { get; }

            private PaperSession()
            {
                Client = new InteractiveBrokersClient(_signal);
                Client.NextValidId += (_, args) =>
                    _nextValidId.TrySetResult(args.OrderId);
                Client.ManagedAccounts += (_, args) =>
                    _managedAccounts.TrySetResult(ParseAccounts(args.AccountList));
            }

            internal static PaperSession Connect()
            {
                var session = new PaperSession();
                try
                {
                    session.Client.ClientSocket.eConnect(
                        Host,
                        Port,
                        Random.Shared.Next(2000, 1000000));
                    var reader = new EReader(
                        session.Client.ClientSocket,
                        session._signal);
                    reader.Start();
                    session._readerThread = new Thread(() =>
                    {
                        while (session.Client.ClientSocket.IsConnected())
                        {
                            session._signal.waitForSignal();
                            reader.processMsgs();
                        }
                    })
                    {
                        IsBackground = true,
                        Name = "IB FA protocol test reader"
                    };
                    session._readerThread.Start();
                    session._nextValidId.Task
                        .WaitAsync(TimeSpan.FromSeconds(20))
                        .GetAwaiter().GetResult();
                    session.Client.ClientSocket.reqManagedAccts();
                    session._managedAccounts.Task
                        .WaitAsync(TimeSpan.FromSeconds(20))
                        .GetAwaiter().GetResult();
                    return session;
                }
                catch
                {
                    session.Dispose();
                    throw;
                }
            }

            internal string[] VerifyPaperAllowlist(
                IReadOnlyCollection<string> allowlist)
            {
                var managed = _managedAccounts.Task.GetAwaiter().GetResult();
                Assert.IsTrue(managed.All(PaperAccountPattern.IsMatch));
                Assert.IsTrue(managed.ToHashSet(
                    StringComparer.OrdinalIgnoreCase).SetEquals(allowlist));
                Assert.AreEqual(
                    1,
                    managed.Count(FinancialAdvisorMasterPattern.IsMatch));
                return managed;
            }

            internal string RequestGroupsXml()
            {
                var completion = NewCompletionSource<string>();
                void Handler(object _, ReceiveFaEventArgs args)
                {
                    if (args.FaDataType == 1)
                    {
                        completion.TrySetResult(args.FaXmlData);
                    }
                }

                Client.ReceiveFa += Handler;
                try
                {
                    Client.ClientSocket.requestFA(1);
                    return completion.Task
                        .WaitAsync(TimeSpan.FromSeconds(20))
                        .GetAwaiter().GetResult();
                }
                finally
                {
                    Client.ReceiveFa -= Handler;
                }
            }

            public void Dispose()
            {
                Client.Dispose();
                _readerThread?.Join(TimeSpan.FromSeconds(5));
            }

            private static TaskCompletionSource<T> NewCompletionSource<T>() =>
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private sealed record SummaryRequest(
            int RequestId,
            string GroupName,
            string Tags);
    }
}
