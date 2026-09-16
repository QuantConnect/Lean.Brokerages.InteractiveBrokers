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
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Util;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersFinancialAdvisorLegacyAccountDataTests
    {
        private const BindingFlags InstanceNonPublic =
            BindingFlags.Instance | BindingFlags.NonPublic;
        private const string AccountId = "DU-LEGACY";
        private const string GroupName = "Alpha";
        private const decimal CashBalance = 123.45m;
        private const decimal ExactPosition = 1.75m;
        private const int PositiveRequestId = 41;
        private const int UnallocatedNegativeRequestId = -2;

        private static readonly FieldInfo AccountField =
            GetRequiredField("_account");
        private static readonly FieldInfo AccountDataField =
            GetRequiredField("_accountData");
        private static readonly FieldInfo AlgorithmField =
            GetRequiredField("_algorithm");
        private static readonly FieldInfo ClientField =
            GetRequiredField("_client");
        private static readonly FieldInfo FinancialAdvisorGroupFilterField =
            GetRequiredField("_financialAdvisorsGroupFilter");
        private static readonly FieldInfo FinancialAdvisorAccountStateField =
            GetRequiredField("_financialAdvisorAccountState");
        private static readonly FieldInfo LoadExistingHoldingsField =
            GetRequiredField("_loadExistingHoldings");
        private static readonly FieldInfo SymbolMapperField =
            GetRequiredField("_symbolMapper");
        private static readonly MethodInfo ConfigureFinancialAdvisorFeaturesMethod =
            GetRequiredMethod("ConfigureFinancialAdvisorFeatures");
        private static readonly MethodInfo DisposeFinancialAdvisorAccountStateMethod =
            GetRequiredMethod("DisposeFinancialAdvisorAccountState");
        private static readonly MethodInfo HandlePortfolioUpdatesMethod =
            GetRequiredMethod("HandlePortfolioUpdates");
        private static readonly MethodInfo HandleUpdateAccountValueMethod =
            GetRequiredMethod("HandleUpdateAccountValue");
        private static readonly MethodInfo InitializeFinancialAdvisorAccountStateMethod =
            GetRequiredMethod("InitializeFinancialAdvisorAccountState");
        private static readonly MethodInfo NextServiceRequestIdMethod =
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetMethod(
                "NextRequestId", InstanceNonPublic)
            ?? throw new InvalidOperationException(
                "Missing Financial Advisor service request ID allocator.");
        private static readonly FieldInfo HasOpenFinancialAdvisorOrdersField =
            typeof(InteractiveBrokersFinancialAdvisorAccountState).GetField(
                "_hasOpenFinancialAdvisorOrders",
                InstanceNonPublic)
            ?? throw new InvalidOperationException(
                "Missing Financial Advisor open-order predicate.");

        [Test]
        public void UnifiedGroupsAffectOnlyFinancialAdvisorAccountsTest()
        {
            using var upstream = LegacyAccountScenario.CreateUpstream(
                GroupName,
                isFinancialAdvisor: false);
            using var unified = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true,
                isFinancialAdvisor: false);

            Assert.IsFalse(unified.Brokerage.IsFinancialAdvisor);
            upstream.EmitLegacyRows();
            unified.EmitLegacyRows();
            upstream.EmitPublicServiceRowsWithoutInternalCallbacks();
            unified.EmitPublicServiceRowsWithoutInternalCallbacks();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(upstream.GetCashBalance(), unified.GetCashBalance());
                Assert.AreEqual(9999.99m, unified.GetCashBalance());
                Assert.AreEqual(
                    upstream.GetHoldingQuantity(),
                    unified.GetHoldingQuantity());
                Assert.AreEqual(
                    Convert.ToInt32(ExactPosition) + Convert.ToInt32(999.5m),
                    unified.GetHoldingQuantity(),
                    "Unified FA settings must preserve upstream negative-ID rows for a non-FA account.");
            });
        }

        [TestCase(false, false, false)]
        [TestCase(false, true, false)]
        [TestCase(true, false, false)]
        [TestCase(true, true, true)]
        public void FinancialAdvisorServiceOwnsOnlyUnifiedFinancialAdvisorStartupRequestsTest(
            bool unifiedGroupsEnabled,
            bool isFinancialAdvisor,
            bool expected)
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                string.Empty,
                unifiedGroupsEnabled,
                isFinancialAdvisor);

            Assert.AreEqual(
                expected,
                scenario.Brokerage.FinancialAdvisorServiceOwnsStartupRequests);
        }

        [TestCase(false, false, "", true)]
        [TestCase(false, false, GroupName, true)]
        [TestCase(true, false, "", true)]
        [TestCase(true, false, GroupName, true)]
        [TestCase(false, true, "", false)]
        [TestCase(false, true, GroupName, true)]
        [TestCase(true, true, "", false)]
        [TestCase(true, true, GroupName, false)]
        public void StartupAccountSummaryPreservesOnlySafeLegacyRequestsTest(
            bool unifiedGroupsEnabled,
            bool isFinancialAdvisor,
            string groupFilter,
            bool expected)
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                groupFilter,
                unifiedGroupsEnabled,
                isFinancialAdvisor);

            Assert.AreEqual(
                expected,
                scenario.Brokerage.ShouldRequestStartupAccountSummary);
        }

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void LegacyAccountDataMatrixTest(
            bool filterSet,
            bool unifiedGroupsEnabled)
        {
            var filter = filterSet
                ? GroupName
                : unifiedGroupsEnabled
                    ? " \t "
                    : string.Empty;
            using var scenario = LegacyAccountScenario.CreateConfigured(
                filter,
                unifiedGroupsEnabled);

            if (!filterSet && unifiedGroupsEnabled)
            {
                Assert.AreEqual(
                    string.Empty,
                    FinancialAdvisorGroupFilterField.GetValue(scenario.Brokerage),
                    "Whitespace-only filters must normalize to the blank-filter path.");
            }

            scenario.EmitLegacyRows();
            if (unifiedGroupsEnabled)
            {
                scenario.EmitServiceRows();
            }

            Assert.Multiple(() =>
            {
                Assert.AreEqual(CashBalance, scenario.GetCashBalance());
                Assert.AreEqual(
                    unifiedGroupsEnabled
                        ? ExactPosition
                        : Convert.ToInt32(ExactPosition),
                    scenario.GetHoldingQuantity());
                Assert.AreEqual(
                    "SPY",
                    scenario.GetHoldingSymbol().Value);
            });
        }

        [Test]
        public void FinancialAdvisorServiceRowsNeverReachLegacyAccountDataTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);

            scenario.EmitLegacyRows();
            scenario.EmitPublicServiceRowsWithoutInternalCallbacks();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(CashBalance, scenario.GetCashBalance());
                Assert.AreEqual(ExactPosition, scenario.GetHoldingQuantity());
            });
        }

        [Test]
        public void OutOfRangeFractionalOptionPortfolioCallbacksPreserveExactQuantityAndFilterServiceRowsTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);
            var exactPosition = (decimal)int.MaxValue + 0.5m;
            var notificationCount = 0;
            var notificationPosition = 0m;
            scenario.Brokerage.OptionNotification += (_, eventArgs) =>
            {
                ++notificationCount;
                notificationPosition = eventArgs.Position;
            };

            scenario.EmitNonServiceFractionalOptionRow(exactPosition);
            scenario.EmitServiceFractionalOptionRow();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(exactPosition, scenario.GetHoldingQuantity());
                Assert.AreEqual(1, notificationCount);
                Assert.AreEqual(exactPosition, notificationPosition);
            });
        }

        [Test]
        public void NonServiceRowsAreNeverDroppedWhenCallbacksInterleaveTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);

            scenario.EmitNonServiceRowsInsideServiceCallbacks();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(CashBalance, scenario.GetCashBalance());
                Assert.AreEqual(ExactPosition, scenario.GetHoldingQuantity());
            });
        }

        [Test]
        public void UnallocatedNegativeRequestRowsReachLegacyAccountDataTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);

            scenario.EmitPublicRowsWithoutInternalCallbacks(
                UnallocatedNegativeRequestId);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(9999.99m, scenario.GetCashBalance());
                Assert.AreEqual(999.5m, scenario.GetHoldingQuantity());
            });
        }

        [Test]
        public void ServiceRequestIdFromClosedConnectionReachesLegacyAccountDataAfterHandshakeTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);

            scenario.CloseAndRestorePhysicalConnection();
            scenario.EmitPublicServiceRowsWithoutInternalCallbacks();

            Assert.Multiple(() =>
            {
                Assert.AreEqual(9999.99m, scenario.GetCashBalance());
                Assert.AreEqual(999.5m, scenario.GetHoldingQuantity());
            });
        }

        [Test]
        public void ConfiguredGroupFilterRejectsAdditionalAccountRefreshWithoutThrowingTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);
            var accepted = true;

            Assert.DoesNotThrow(() =>
                accepted = scenario.Brokerage.RequestAccountSnapshotRefresh(
                    new[] { GroupName },
                    new[] { AccountId }));
            Assert.IsFalse(accepted);
        }

        [Test]
        public void FinancialAdvisorOpenOrderPredicateHandlesMissingOrderProviderTest()
        {
            using var scenario = LegacyAccountScenario.CreateConfigured(
                GroupName,
                unifiedGroupsEnabled: true);
            var state = (InteractiveBrokersFinancialAdvisorAccountState)
                FinancialAdvisorAccountStateField.GetValue(scenario.Brokerage);
            var hasOpenOrders = (Func<bool>)
                HasOpenFinancialAdvisorOrdersField.GetValue(state);

            Assert.DoesNotThrow(() => hasOpenOrders());
            Assert.IsFalse(hasOpenOrders());
        }

        private static FieldInfo GetRequiredField(string name)
        {
            return typeof(InteractiveBrokersBrokerage).GetField(name, InstanceNonPublic)
                ?? throw new InvalidOperationException($"Missing brokerage field '{name}'.");
        }

        private static MethodInfo GetRequiredMethod(string name)
        {
            return typeof(InteractiveBrokersBrokerage).GetMethod(name, InstanceNonPublic)
                ?? throw new InvalidOperationException($"Missing brokerage method '{name}'.");
        }

        private sealed class LegacyAccountScenario : IDisposable
        {
            private readonly int _serviceRequestId;
            private bool _disposed;

            public InteractiveBrokersBrokerage Brokerage { get; }
            public ReorderableInteractiveBrokersClient Client { get; }

            private LegacyAccountScenario(
                string financialAdvisorGroupFilter,
                bool unifiedGroupsEnabled,
                bool configureFeatures,
                bool isFinancialAdvisor)
            {
                Brokerage = new InteractiveBrokersBrokerage();
                Client = new ReorderableInteractiveBrokersClient();

                AccountField.SetValue(
                    Brokerage,
                    isFinancialAdvisor ? "F-MASTER" : AccountId);
                AlgorithmField.SetValue(Brokerage, new QCAlgorithm());
                ClientField.SetValue(Brokerage, Client);
                LoadExistingHoldingsField.SetValue(Brokerage, true);
                SymbolMapperField.SetValue(
                    Brokerage,
                    new InteractiveBrokersSymbolMapper(
                        Composer.Instance.GetPart<IMapFileProvider>()));

                if (configureFeatures)
                {
                    ConfigureFinancialAdvisorFeaturesMethod.Invoke(
                        Brokerage,
                        new object[]
                        {
                            financialAdvisorGroupFilter,
                            false,
                            unifiedGroupsEnabled
                        });
                }
                else
                {
                    FinancialAdvisorGroupFilterField.SetValue(
                        Brokerage,
                        financialAdvisorGroupFilter);
                }

                InitializeFinancialAdvisorAccountStateMethod.Invoke(Brokerage, null);
                var accountState = FinancialAdvisorAccountStateField.GetValue(Brokerage)
                    as InteractiveBrokersFinancialAdvisorAccountState;
                _serviceRequestId = accountState == null
                    ? UnallocatedNegativeRequestId
                    : (int)NextServiceRequestIdMethod.Invoke(accountState, null);
                Client.UpdateAccountValue += HandleUpdateAccountValue;
                Client.UpdatePortfolio += HandlePortfolioUpdate;
                if (!string.IsNullOrEmpty(
                    (string)FinancialAdvisorGroupFilterField.GetValue(Brokerage)))
                {
                    Client.AccountUpdateMulti += HandleUpdateAccountValue;
                }
            }

            public static LegacyAccountScenario CreateConfigured(
                string financialAdvisorGroupFilter,
                bool unifiedGroupsEnabled,
                bool isFinancialAdvisor = true)
            {
                return new LegacyAccountScenario(
                    financialAdvisorGroupFilter,
                    unifiedGroupsEnabled,
                    configureFeatures: true,
                    isFinancialAdvisor: isFinancialAdvisor);
            }

            public static LegacyAccountScenario CreateUpstream(
                string financialAdvisorGroupFilter,
                bool isFinancialAdvisor = true)
            {
                return new LegacyAccountScenario(
                    financialAdvisorGroupFilter,
                    unifiedGroupsEnabled: false,
                    configureFeatures: false,
                    isFinancialAdvisor: isFinancialAdvisor);
            }

            public void EmitLegacyRows()
            {
                if (string.IsNullOrEmpty(
                    (string)FinancialAdvisorGroupFilterField.GetValue(Brokerage)))
                {
                    Client.updatePortfolio(
                        CreateContract(),
                        ExactPosition,
                        101,
                        176.75,
                        100.5,
                        0,
                        0,
                        AccountId);
                    Client.updateAccountValue(
                        "CashBalance",
                        CashBalance.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        Currencies.USD,
                        AccountId);
                    return;
                }

                Client.positionMulti(
                    PositiveRequestId,
                    AccountId,
                    string.Empty,
                    CreateContract(),
                    ExactPosition,
                    100.5);
                Client.accountUpdateMulti(
                    PositiveRequestId,
                    AccountId,
                    string.Empty,
                    "CashBalance",
                    CashBalance.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    Currencies.USD);
            }

            public void EmitServiceRows()
            {
                Client.positionMulti(
                    _serviceRequestId,
                    "DU-SERVICE",
                    string.Empty,
                    CreateContract(),
                    999.5m,
                    900.5);
                Client.accountUpdateMulti(
                    _serviceRequestId,
                    AccountId,
                    string.Empty,
                    "CashBalance",
                    "9999.99",
                    Currencies.USD);
            }

            public void EmitNonServiceFractionalOptionRow(decimal position)
            {
                Client.updatePortfolio(
                    CreateOptionContract(),
                    position,
                    101,
                    176.75,
                    100.5,
                    0,
                    0,
                    AccountId);
            }

            public void EmitServiceFractionalOptionRow()
            {
                Client.positionMulti(
                    _serviceRequestId,
                    "DU-SERVICE",
                    string.Empty,
                    CreateOptionContract(),
                    999.5m,
                    900.5);
            }

            public void EmitPublicServiceRowsWithoutInternalCallbacks()
            {
                EmitPublicRowsWithoutInternalCallbacks(_serviceRequestId);
            }

            public void EmitPublicRowsWithoutInternalCallbacks(int requestId)
            {
                Client.EmitPublicPortfolio(
                    new IB.UpdatePortfolioEventArgs(
                        CreateContract(),
                        999.5m,
                        900.5,
                        0,
                        900.5,
                        0,
                        0,
                        "DU-SERVICE",
                        requestId));
                Client.EmitPublicAccountUpdate(
                    new IB.UpdateAccountValueEventArgs(
                        "CashBalance",
                        "9999.99",
                        Currencies.USD,
                        AccountId,
                        requestId));
            }

            public void EmitNonServiceRowsInsideServiceCallbacks()
            {
                var accountRowEmitted = false;
                var positionRowEmitted = false;
                Client.AccountUpdateMultiWithRequestId += (_, args) =>
                {
                    if (args.RequestId == _serviceRequestId && !accountRowEmitted)
                    {
                        accountRowEmitted = true;
                        Client.EmitPublicAccountUpdate(
                            new IB.UpdateAccountValueEventArgs(
                                "CashBalance",
                                CashBalance.ToString(
                                    System.Globalization.CultureInfo.InvariantCulture),
                                Currencies.USD,
                                AccountId,
                                PositiveRequestId));
                    }
                };
                Client.PositionMulti += (_, args) =>
                {
                    if (args.RequestId == _serviceRequestId && !positionRowEmitted)
                    {
                        positionRowEmitted = true;
                        Client.EmitPublicPortfolio(
                            new IB.UpdatePortfolioEventArgs(
                                CreateContract(),
                                ExactPosition,
                                101,
                                176.75,
                                100.5,
                                0,
                                0,
                                AccountId,
                                PositiveRequestId));
                    }
                };

                EmitServiceRows();
            }

            public void CloseAndRestorePhysicalConnection()
            {
                Client.connectionClosed();
                Client.nextValidId(1);
            }

            public decimal GetCashBalance()
            {
                return ((InteractiveBrokersAccountData)AccountDataField.GetValue(Brokerage))
                    .CashBalances[Currencies.USD];
            }

            public decimal GetHoldingQuantity()
            {
                return ((InteractiveBrokersAccountData)AccountDataField.GetValue(Brokerage))
                    .AccountHoldings.Values.Single().Holding.Quantity;
            }

            public Symbol GetHoldingSymbol()
            {
                return ((InteractiveBrokersAccountData)AccountDataField.GetValue(Brokerage))
                    .AccountHoldings.Values.Single().Holding.Symbol;
            }

            private void HandlePortfolioUpdate(
                object sender,
                IB.UpdatePortfolioEventArgs eventArgs)
            {
                HandlePortfolioUpdatesMethod.Invoke(
                    Brokerage,
                    new object[] { sender, eventArgs });
            }

            private void HandleUpdateAccountValue(
                object sender,
                IB.UpdateAccountValueEventArgs eventArgs)
            {
                HandleUpdateAccountValueMethod.Invoke(
                    Brokerage,
                    new object[] { sender, eventArgs });
            }

            private static Contract CreateContract()
            {
                return new Contract
                {
                    ConId = 101,
                    Symbol = "SPY",
                    LocalSymbol = "SPY",
                    SecType = IB.SecurityType.Stock,
                    Currency = Currencies.USD,
                    Exchange = "SMART",
                    PrimaryExch = "ARCA",
                    Multiplier = "1"
                };
            }

            private static Contract CreateOptionContract()
            {
                return new Contract
                {
                    ConId = 102,
                    Symbol = "SPY",
                    LocalSymbol = "SPY   301220C00500000",
                    LastTradeDateOrContractMonth = "20301220",
                    SecType = IB.SecurityType.Option,
                    Currency = Currencies.USD,
                    Exchange = "SMART",
                    PrimaryExch = "ARCA",
                    Multiplier = "100",
                    Right = IB.RightType.Call,
                    Strike = 500
                };
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                DisposeFinancialAdvisorAccountStateMethod.Invoke(Brokerage, null);
                Client.Dispose();
                ClientField.SetValue(Brokerage, null);
                Brokerage.Dispose();
            }
        }

        private sealed class ReorderableInteractiveBrokersClient :
            IB.InteractiveBrokersClient
        {
            public ReorderableInteractiveBrokersClient()
                : base(new EReaderMonitorSignal())
            {
            }

            public void EmitPublicAccountUpdate(
                IB.UpdateAccountValueEventArgs eventArgs)
            {
                OnAccountUpdateMulti(eventArgs);
            }

            public void EmitPublicPortfolio(
                IB.UpdatePortfolioEventArgs eventArgs)
            {
                OnUpdatePortfolio(eventArgs);
            }
        }
    }
}
