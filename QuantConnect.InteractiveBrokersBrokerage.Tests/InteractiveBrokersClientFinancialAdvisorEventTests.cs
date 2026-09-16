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
using System.Globalization;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersClientFinancialAdvisorEventTests
    {
        [Test]
        public void PublicClientEventsRemainAdditiveTest()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            AccountUpdateMultiEventArgs accountUpdate = null;
            AccountUpdateMultiEndEventArgs accountUpdateEnd = null;
            PositionMultiEventArgs positionUpdate = null;
            RequestEndEventArgs positionUpdateEnd = null;
            UpdateAccountValueEventArgs legacyAccountUpdate = null;
            AccountUpdateMultiEndEventArgs legacyAccountUpdateEnd = null;
            UpdatePortfolioEventArgs legacyPositionUpdate = null;
            var legacyPositionUpdateEndCount = 0;
            var invocationOrder = new List<string>();

            client.AccountUpdateMultiWithRequestId += (_, args) =>
            {
                accountUpdate = args;
                invocationOrder.Add("account-update-internal");
            };
            client.AccountUpdateMultiEndWithRequestId += (_, args) =>
            {
                accountUpdateEnd = args;
                invocationOrder.Add("account-end-internal");
            };
            client.PositionMulti += (_, args) =>
            {
                positionUpdate = args;
                invocationOrder.Add("position-update-internal");
            };
            client.PositionMultiEndWithRequestId += (_, args) =>
            {
                positionUpdateEnd = args;
                invocationOrder.Add("position-end-internal");
            };
            client.AccountUpdateMulti += (_, args) =>
            {
                legacyAccountUpdate = args;
                invocationOrder.Add("account-update-public");
            };
            client.AccountUpdateMultiEnd += (_, args) =>
            {
                legacyAccountUpdateEnd = args;
                invocationOrder.Add("account-end-public");
            };
            client.UpdatePortfolio += (_, args) =>
            {
                legacyPositionUpdate = args;
                invocationOrder.Add("position-update-public");
            };
            client.PositionMultiEnd += (_, _) =>
            {
                legacyPositionUpdateEndCount++;
                invocationOrder.Add("position-end-public");
            };

            var contract = new Contract { Symbol = "SPY", SecType = "STK" };
            client.accountUpdateMulti(17, "DU123", "ModelA", "NetLiquidation", "1000", "USD");
            client.accountUpdateMultiEnd(17);
            client.positionMulti(18, "DU123", "ModelA", contract, 1.25m, 100.5);
            client.positionMultiEnd(18);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(17, accountUpdate.RequestId);
                Assert.AreEqual("DU123", accountUpdate.Account);
                Assert.AreEqual("ModelA", accountUpdate.ModelCode);
                Assert.AreEqual("NetLiquidation", accountUpdate.Key);
                Assert.AreEqual("1000", accountUpdate.Value);
                Assert.AreEqual("USD", accountUpdate.Currency);
                Assert.AreEqual("NetLiquidation", legacyAccountUpdate.Key);
                Assert.AreEqual("1000", legacyAccountUpdate.Value);
                Assert.AreEqual("USD", legacyAccountUpdate.Currency);
                Assert.AreEqual("DU123", legacyAccountUpdate.AccountName);
                Assert.AreEqual(17, legacyAccountUpdate.AccountUpdatesMultiRequestId);
                Assert.AreEqual(17, accountUpdateEnd.RequestId);
                Assert.AreEqual(17, legacyAccountUpdateEnd.RequestId);

                Assert.AreEqual(18, positionUpdate.RequestId);
                Assert.AreEqual("DU123", positionUpdate.Account);
                Assert.AreEqual("ModelA", positionUpdate.ModelCode);
                Assert.AreSame(contract, positionUpdate.Contract);
                Assert.AreEqual(1.25m, positionUpdate.Position);
                Assert.AreEqual(100.5, positionUpdate.AverageCost);
                Assert.AreEqual(1, legacyPositionUpdate.Position);
                Assert.AreEqual(1.25m, legacyPositionUpdate.PositionQuantity);
                Assert.AreEqual("DU123", legacyPositionUpdate.AccountName);
                Assert.AreEqual(18, legacyPositionUpdate.PositionsMultiRequestId);
                Assert.AreEqual(18, positionUpdateEnd.RequestId);
                Assert.AreEqual(1, legacyPositionUpdateEndCount);
            });
            CollectionAssert.AreEqual(
                new[]
                {
                    "account-update-internal",
                    "account-update-public",
                    "account-end-internal",
                    "account-end-public",
                    "position-update-internal",
                    "position-update-public",
                    "position-end-internal",
                    "position-end-public"
                },
                invocationOrder);
        }

        [Test]
        public void PublicKeyedCallbacksRemainAvailableWithoutInternalSubscribers()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            UpdateAccountValueEventArgs accountUpdate = null;
            UpdatePortfolioEventArgs positionUpdate = null;
            client.AccountUpdateMulti += (_, args) => accountUpdate = args;
            client.UpdatePortfolio += (_, args) => positionUpdate = args;

            var contract = new Contract { ConId = 756733, Symbol = "SPY", SecType = "STK" };
            client.accountUpdateMulti(17, "DU123", "ModelA", "NetLiquidation", "1000", "USD");
            client.positionMulti(18, "DU123", "ModelA", contract, 1.25m, 100.5);

            Assert.Multiple(() =>
            {
                Assert.AreEqual("NetLiquidation", accountUpdate.Key);
                Assert.AreEqual("1000", accountUpdate.Value);
                Assert.AreEqual("USD", accountUpdate.Currency);
                Assert.AreEqual("DU123", accountUpdate.AccountName);
                Assert.AreEqual(17, accountUpdate.AccountUpdatesMultiRequestId);

                Assert.AreSame(contract, positionUpdate.Contract);
                Assert.AreEqual(1.25m, positionUpdate.PositionQuantity);
                Assert.AreEqual(100.5, positionUpdate.AverageCost);
                Assert.AreEqual("DU123", positionUpdate.AccountName);
                Assert.AreEqual(18, positionUpdate.PositionsMultiRequestId);
            });
        }

        [Test]
        public void UnkeyedCallbacksPreservePublicEventsAndAddInternalEvents()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var publicErrorCount = 0;
            var internalErrorCount = 0;
            var publicReceiveFaCount = 0;
            var internalReceiveFaCount = 0;
            var publicManagedAccountsCount = 0;
            var internalManagedAccountsCount = 0;
            var publicFamilyCodesCount = 0;
            var internalFamilyCodesCount = 0;
            var invocationOrder = new List<string>();
            ReplaceFaEndEventArgs replacement = null;

            client.Error += (_, _) =>
            {
                publicErrorCount++;
                invocationOrder.Add("error-public");
            };
            client.InternalError += (_, _) =>
            {
                internalErrorCount++;
                invocationOrder.Add("error-internal");
            };
            client.ReceiveFa += (_, _) =>
            {
                publicReceiveFaCount++;
                invocationOrder.Add("receive-fa-public");
            };
            client.InternalReceiveFa += (_, _) =>
            {
                internalReceiveFaCount++;
                invocationOrder.Add("receive-fa-internal");
            };
            client.ManagedAccounts += (_, _) =>
            {
                publicManagedAccountsCount++;
                invocationOrder.Add("managed-accounts-public");
            };
            client.InternalManagedAccounts += (_, _) =>
            {
                internalManagedAccountsCount++;
                invocationOrder.Add("managed-accounts-internal");
            };
            client.FamilyCodes += (_, _) =>
            {
                publicFamilyCodesCount++;
                invocationOrder.Add("family-codes-public");
            };
            client.InternalFamilyCodes += (_, _) =>
            {
                internalFamilyCodesCount++;
                invocationOrder.Add("family-codes-internal");
            };
            client.ReplaceFaEnd += (_, args) => replacement = args;

            client.error(17, 123, 10230, "configuration pending", string.Empty);
            client.receiveFA(1, "<ListOfGroups />");
            client.managedAccounts("DU123");
            client.familyCodes(new[]
            {
                new FamilyCode { AccountID = "DU123", FamilyCodeStr = "FamilyA" }
            });
            client.replaceFAEnd(19, null);

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1, publicErrorCount);
                Assert.AreEqual(1, internalErrorCount);
                Assert.AreEqual(1, publicReceiveFaCount);
                Assert.AreEqual(1, internalReceiveFaCount);
                Assert.AreEqual(1, publicManagedAccountsCount);
                Assert.AreEqual(1, internalManagedAccountsCount);
                Assert.AreEqual(1, publicFamilyCodesCount);
                Assert.AreEqual(1, internalFamilyCodesCount);
                Assert.AreEqual(19, replacement.RequestId);
                Assert.AreEqual(string.Empty, replacement.Message);
            });
            CollectionAssert.AreEqual(
                new[]
                {
                    "error-public",
                    "error-internal",
                    "receive-fa-internal",
                    "receive-fa-public",
                    "managed-accounts-internal",
                    "managed-accounts-public",
                    "family-codes-internal",
                    "family-codes-public"
                },
                invocationOrder);
        }

        [Test]
        public void UnkeyedInternalCallbackExceptionsDoNotSuppressPublicEvents()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var publicReceiveFaCount = 0;
            var publicManagedAccountsCount = 0;
            var publicFamilyCodesCount = 0;

            client.InternalReceiveFa += (_, _) => throw new InvalidOperationException("internal receiveFA");
            client.InternalManagedAccounts += (_, _) =>
                throw new InvalidOperationException("internal managedAccounts");
            client.InternalFamilyCodes += (_, _) => throw new InvalidOperationException("internal familyCodes");
            client.ReceiveFa += (_, _) => publicReceiveFaCount++;
            client.ManagedAccounts += (_, _) => publicManagedAccountsCount++;
            client.FamilyCodes += (_, _) => publicFamilyCodesCount++;

            Assert.Throws<InvalidOperationException>(() => client.receiveFA(1, "<ListOfGroups />"));
            Assert.Throws<InvalidOperationException>(() => client.managedAccounts("DU123"));
            Assert.Throws<InvalidOperationException>(() => client.familyCodes(new[]
            {
                new FamilyCode { AccountID = "DU123", FamilyCodeStr = "FamilyA" }
            }));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1, publicReceiveFaCount);
                Assert.AreEqual(1, publicManagedAccountsCount);
                Assert.AreEqual(1, publicFamilyCodesCount);
            });
        }

        [Test]
        public void PublicErrorCallbackExceptionDoesNotSuppressInternalEvent()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var internalErrorCount = 0;
            client.Error += (_, _) => throw new InvalidOperationException("public error");
            client.InternalError += (_, _) => internalErrorCount++;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                client.error(17, 123, 10230, "configuration pending", string.Empty));

            Assert.Multiple(() =>
            {
                Assert.AreEqual("public error", exception.Message);
                Assert.AreEqual(1, internalErrorCount);
            });
        }

        [Test]
        public void InternalErrorCallbackExceptionOccursAfterPublicEvent()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var publicErrorCount = 0;
            client.Error += (_, _) => publicErrorCount++;
            client.InternalError += (_, _) => throw new InvalidOperationException("internal error");

            var exception = Assert.Throws<InvalidOperationException>(() =>
                client.error(17, 123, 10230, "configuration pending", string.Empty));

            Assert.Multiple(() =>
            {
                Assert.AreEqual("internal error", exception.Message);
                Assert.AreEqual(1, publicErrorCount);
            });
        }

        [Test]
        public void KeyedPublicCallbackExceptionsDoNotSuppressInternalEvents()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var internalAccountUpdateCount = 0;
            var internalAccountEndCount = 0;
            var internalPositionUpdateCount = 0;
            var internalPositionEndCount = 0;
            client.AccountUpdateMultiWithRequestId += (_, _) => internalAccountUpdateCount++;
            client.AccountUpdateMultiEndWithRequestId += (_, _) => internalAccountEndCount++;
            client.PositionMulti += (_, _) => internalPositionUpdateCount++;
            client.PositionMultiEndWithRequestId += (_, _) => internalPositionEndCount++;
            client.AccountUpdateMulti += (_, _) => throw new InvalidOperationException("public accountUpdateMulti");
            client.AccountUpdateMultiEnd += (_, _) => throw new InvalidOperationException("public accountUpdateMultiEnd");
            client.UpdatePortfolio += (_, _) => throw new InvalidOperationException("public positionMulti");
            client.PositionMultiEnd += (_, _) => throw new InvalidOperationException("public positionMultiEnd");

            Assert.Throws<InvalidOperationException>(() =>
                client.accountUpdateMulti(17, "DU123", "ModelA", "NetLiquidation", "1000", "USD"));
            Assert.Throws<InvalidOperationException>(() => client.accountUpdateMultiEnd(17));
            Assert.Throws<InvalidOperationException>(() =>
                client.positionMulti(18, "DU123", "ModelA", new Contract(), 1.25m, 100.5));
            Assert.Throws<InvalidOperationException>(() => client.positionMultiEnd(18));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1, internalAccountUpdateCount);
                Assert.AreEqual(1, internalAccountEndCount);
                Assert.AreEqual(1, internalPositionUpdateCount);
                Assert.AreEqual(1, internalPositionEndCount);
            });
        }

        [Test]
        public void KeyedInternalCallbackExceptionsDoNotSuppressPublicEvents()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var publicAccountUpdateCount = 0;
            var publicAccountEndCount = 0;
            var publicPositionUpdateCount = 0;
            var publicPositionEndCount = 0;
            client.AccountUpdateMultiWithRequestId += (_, _) =>
                throw new InvalidOperationException("internal accountUpdateMulti");
            client.AccountUpdateMultiEndWithRequestId += (_, _) =>
                throw new InvalidOperationException("internal accountUpdateMultiEnd");
            client.PositionMulti += (_, _) => throw new InvalidOperationException("internal positionMulti");
            client.PositionMultiEndWithRequestId += (_, _) =>
                throw new InvalidOperationException("internal positionMultiEnd");
            client.AccountUpdateMulti += (_, _) => publicAccountUpdateCount++;
            client.AccountUpdateMultiEnd += (_, _) => publicAccountEndCount++;
            client.UpdatePortfolio += (_, _) => publicPositionUpdateCount++;
            client.PositionMultiEnd += (_, _) => publicPositionEndCount++;

            Assert.Throws<InvalidOperationException>(() =>
                client.accountUpdateMulti(17, "DU123", "ModelA", "NetLiquidation", "1000", "USD"));
            Assert.Throws<InvalidOperationException>(() => client.accountUpdateMultiEnd(17));
            Assert.Throws<InvalidOperationException>(() =>
                client.positionMulti(18, "DU123", "ModelA", new Contract(), 1.25m, 100.5));
            Assert.Throws<InvalidOperationException>(() => client.positionMultiEnd(18));

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1, publicAccountUpdateCount);
                Assert.AreEqual(1, publicAccountEndCount);
                Assert.AreEqual(1, publicPositionUpdateCount);
                Assert.AreEqual(1, publicPositionEndCount);
            });
        }

        [Test]
        public void PortfolioCallbacksPreserveExactQuantityAndLegacyConversion()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            UpdatePortfolioEventArgs update = null;
            UpdateAccountValueEventArgs accountUpdate = null;
            client.UpdatePortfolio += (_, args) => update = args;
            client.UpdateAccountValue += (_, args) => accountUpdate = args;

            client.updatePortfolio(new Contract(), 1.25m, 2, 3, 4, 5, 6, "DU123");
            client.updateAccountValue("CashBalance", "123.45", "USD", "DU123");

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1, update.Position);
                Assert.AreEqual(1.25m, update.PositionQuantity);
                Assert.IsNull(update.PositionsMultiRequestId);
                Assert.IsNull(accountUpdate.AccountUpdatesMultiRequestId);
            });
        }

        [TestCase(1.5, 2)]
        [TestCase(2.5, 2)]
        [TestCase(0.5, 0)]
        [TestCase(-1.5, -2)]
        [TestCase(-2.5, -2)]
        [TestCase(-0.5, 0)]
        public void LegacyPositionProjectionUsesUpstreamRounding(
            double position,
            int expected)
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            UpdatePortfolioEventArgs update = null;
            client.UpdatePortfolio += (_, args) => update = args;

            client.updatePortfolio(
                new Contract(),
                Convert.ToDecimal(position),
                2,
                3,
                4,
                5,
                6,
                "DU123");

            Assert.Multiple(() =>
            {
                Assert.AreEqual(expected, update.Position);
                Assert.AreEqual(Convert.ToDecimal(position), update.PositionQuantity);
            });
        }

        [Test]
        public void PublicIntegerPositionConstructorPreservesBothProjections()
        {
            var update = new UpdatePortfolioEventArgs(
                new Contract(),
                123,
                2,
                3,
                4,
                5,
                6,
                "DU123");

            Assert.Multiple(() =>
            {
                Assert.AreEqual(123, update.Position);
                Assert.AreEqual(123m, update.PositionQuantity);
            });
        }

        [Test]
        public void PositionMultiPreservesExactOverflowAndThrowsOnlyOnLegacyRead()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var exactUpdates = new List<PositionMultiEventArgs>();
            var legacyUpdates = new List<UpdatePortfolioEventArgs>();
            var internalEndRequestIds = new List<int>();
            var publicEndCount = 0;
            client.PositionMulti += (_, args) => exactUpdates.Add(args);
            client.UpdatePortfolio += (_, args) => legacyUpdates.Add(args);
            client.PositionMultiEndWithRequestId +=
                (_, args) => internalEndRequestIds.Add(args.RequestId);
            client.PositionMultiEnd += (_, _) => ++publicEndCount;

            Assert.DoesNotThrow(() =>
            {
                client.positionMulti(
                    41, "DU123", string.Empty, new Contract(), decimal.MaxValue, 1);
                client.positionMultiEnd(41);
                client.positionMulti(
                    42, "DU123", string.Empty, new Contract(), decimal.MinValue, 1);
                client.positionMultiEnd(42);
            });

            Assert.Multiple(() =>
            {
                Assert.AreEqual(2, exactUpdates.Count);
                Assert.AreEqual(decimal.MaxValue, exactUpdates[0].Position);
                Assert.AreEqual(decimal.MinValue, exactUpdates[1].Position);
                Assert.AreEqual(2, legacyUpdates.Count);
                Assert.AreEqual(decimal.MaxValue, legacyUpdates[0].PositionQuantity);
                Assert.AreEqual(41, legacyUpdates[0].PositionsMultiRequestId);
                Assert.AreEqual(decimal.MinValue, legacyUpdates[1].PositionQuantity);
                Assert.AreEqual(42, legacyUpdates[1].PositionsMultiRequestId);
                CollectionAssert.AreEqual(new[] { 41, 42 }, internalEndRequestIds);
                Assert.AreEqual(2, publicEndCount);
            });
            Assert.Throws<OverflowException>(() => _ = legacyUpdates[0].Position);
            Assert.Throws<OverflowException>(() => _ = legacyUpdates[1].Position);
        }

        [Test]
        public void OrdinaryPortfolioUpdatePreservesExactOverflowAndThrowsOnlyOnLegacyRead()
        {
            using var client = new InteractiveBrokersClient(new EReaderMonitorSignal());
            var updates = new List<UpdatePortfolioEventArgs>();
            client.UpdatePortfolio += (_, args) => updates.Add(args);

            Assert.DoesNotThrow(() =>
            {
                client.updatePortfolio(new Contract(), decimal.MaxValue, 2, 3, 4, 5, 6, "DU123");
                client.updatePortfolio(new Contract(), decimal.MinValue, 2, 3, 4, 5, 6, "DU123");
            });

            Assert.Multiple(() =>
            {
                Assert.AreEqual(2, updates.Count);
                Assert.AreEqual(decimal.MaxValue, updates[0].PositionQuantity);
                Assert.AreEqual(decimal.MinValue, updates[1].PositionQuantity);
                Assert.IsNull(updates[0].PositionsMultiRequestId);
                Assert.IsNull(updates[1].PositionsMultiRequestId);
            });
            Assert.Throws<OverflowException>(() => _ = updates[0].Position);
            Assert.Throws<OverflowException>(() => _ = updates[1].Position);
        }

        private static readonly decimal[] DiagnosticPositionQuantities =
        {
            (decimal)int.MaxValue + 0.5m,
            (decimal)int.MinValue - 0.5m,
            1.25m
        };

        [TestCaseSource(nameof(DiagnosticPositionQuantities))]
        public void PortfolioEventDiagnosticsUseExactPositionQuantity(decimal position)
        {
            var update = new UpdatePortfolioEventArgs(
                new Contract(),
                position,
                2,
                3,
                4,
                5,
                6,
                "DU123");
            string diagnostic = null;

            Assert.DoesNotThrow(() => diagnostic = update.ToString());
            StringAssert.Contains(
                $"Position: {position.ToString(CultureInfo.CurrentCulture)}, MarketPrice:",
                diagnostic);
        }
    }
}
