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
using System.Linq;
using System.Xml.Linq;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture, Parallelizable(ParallelScope.All)]
    public class InteractiveBrokersFinancialAdvisorAccountStateHelperTests
    {
        [Test]
        public void ParsesCurrentFinancialAdvisorGroupXml()
        {
            const string xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ListOfGroups>
                  <Group>
                    <name>GroupOne</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts varName="list"><String>PaperA</String><String>PaperB</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>GroupTwo</name>
                    <defaultMethod>EqualQuantity</defaultMethod>
                    <ListOfAccts varName="list"><String>PaperC</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;

            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml);

            Assert.AreEqual(2, groups.Count);
            Assert.AreEqual("NetLiq", groups["GroupOne"].AllocationMethod);
            Assert.AreEqual("Equal", groups["GroupTwo"].AllocationMethod);
            CollectionAssert.AreEqual(new[] { "PaperA", "PaperB" }, groups["GroupOne"].AccountIds);
        }

        [Test]
        public void MutationsCanonicalizeLegacyEqualQuantityAcrossCompleteDocument()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Legacy</name><defaultMethod>EqualQuantity</defaultMethod>
                    <ListOfAccts><String>PaperA</String></ListOfAccts></Group>
                  <Group><name>Target</name><defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts><Account><acct>PaperB</acct><amount>1</amount></Account></ListOfAccts></Group>
                </ListOfGroups>
                """;
            var equalXml = xml.Replace("EqualQuantity", "Equal");

            var allocationXml =
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAllocationsXml(
                    xml,
                    "Target",
                    new Dictionary<string, decimal> { ["PaperB"] = 2m });
            var assignmentXml =
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml,
                    "PaperA",
                    "Legacy");

            StringAssert.DoesNotContain("EqualQuantity", allocationXml);
            StringAssert.DoesNotContain("EqualQuantity", assignmentXml);
            Assert.AreEqual(
                "Equal",
                InteractiveBrokersFinancialAdvisorAccountState
                    .ParseGroups(allocationXml)["Legacy"].AllocationMethod);
            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(xml),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(equalXml));
        }

        [Test]
        public void EmptyFinancialAdvisorGroupConfigurationIsValidForCompleteDiscovery()
        {
            const string xml = "<ListOfGroups></ListOfGroups>";

            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml);
            var unchanged = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", string.Empty);

            Assert.IsEmpty(groups);
            Assert.IsEmpty(InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(unchanged));
            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperA", "MissingGroup"));
        }

        [Test]
        public void ParsesFinancialAdvisorAccountAliases()
        {
            const string xml = """
                <ListOfAccountAliases xmlns="urn:ib-test">
                  <AccountAlias><account> PaperMaster </account><alias> Advisor </alias></AccountAlias>
                  <AccountAlias><account>PaperA</account><alias> Client [FA:GroupOne] </alias></AccountAlias>
                  <AccountAlias><account>PaperB</account><alias></alias></AccountAlias>
                </ListOfAccountAliases>
                """;

            var aliases = InteractiveBrokersFinancialAdvisorAccountState.ParseAliases(xml);

            Assert.AreEqual(3, aliases.Count);
            Assert.AreEqual("Advisor", aliases["papermaster"]);
            Assert.AreEqual("Client [FA:GroupOne]", aliases["PaperA"]);
            Assert.IsEmpty(aliases["PaperB"]);
            Assert.IsEmpty(InteractiveBrokersFinancialAdvisorAccountState.ParseAliases(
                "<ListOfAccountAliases></ListOfAccountAliases>"));
        }

        [Test]
        public void ConflictingOrInvalidFinancialAdvisorAccountAliasesAreRejected()
        {
            const string conflicting = """
                <ListOfAccountAliases>
                  <AccountAlias><account>PaperA</account><alias>One</alias></AccountAlias>
                  <AccountAlias><account>papera</account><alias>Two</alias></AccountAlias>
                </ListOfAccountAliases>
                """;
            const string blankAccount = """
                <ListOfAccountAliases>
                  <AccountAlias><account> </account><alias>One</alias></AccountAlias>
                </ListOfAccountAliases>
                """;

            StringAssert.Contains("conflicting aliases", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseAliases(conflicting)).Message);
            StringAssert.Contains("blank account identifier", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseAliases(blankAccount)).Message);
            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseAliases(string.Empty));
        }

        [Test]
        public void ParsesUserSpecifiedAccountAllocationValues()
        {
            const string xml = """
                <ListOfGroups>
                  <Group>
                    <name>RatioGroup</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts varName="list">
                      <Account><acct>PaperA</acct><amount>1.0</amount></Account>
                      <Account><acct>PaperB</acct><amount>2.0</amount></Account>
                    </ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;

            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml);

            CollectionAssert.AreEqual(new[] { "PaperA", "PaperB" }, groups["RatioGroup"].AccountIds);
            Assert.AreEqual(1m, groups["RatioGroup"].AccountAllocationValues["PaperA"]);
            Assert.AreEqual(2m, groups["RatioGroup"].AccountAllocationValues["PaperB"]);
        }

        [Test]
        public void ContractsOrSharesAcceptsZeroButOtherUserSpecifiedMethodsRemainPositive()
        {
            const string contractsOrShares = """
                <ListOfGroups><Group><name>G</name><defaultMethod>ContractsOrShares</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>1</amount></Account>
                  <Account><acct>PaperB</acct><amount>0</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string ratio = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>1</amount></Account>
                  <Account><acct>PaperB</acct><amount>0</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string percent = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>100</amount></Account>
                  <Account><acct>PaperB</acct><amount>0</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string negative = """
                <ListOfGroups><Group><name>G</name><defaultMethod>ContractsOrShares</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>-1</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;

            var parsed = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(contractsOrShares);
            Assert.AreEqual(0m, parsed["G"].AccountAllocationValues["PaperB"]);
            StringAssert.Contains("positive", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(ratio)).Message);
            StringAssert.Contains("positive", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(percent)).Message);
            StringAssert.Contains("non-negative", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(negative)).Message);
        }

        [Test]
        public void UnrelatedInvalidAllocationGroupDoesNotBlockTargetedUpdate()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Execution</name><defaultMethod>ContractsOrShares</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>1</amount></Account>
                    <Account><acct>PaperB</acct><amount>0</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>InvalidPercent</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperC</acct><amount>40</amount></Account>
                    <Account><acct>PaperD</acct><amount>40</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            StringAssert.Contains("must total 100", Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml)).Message);
            var structurallyParsed = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(
                xml,
                validateAllocationConfiguration: false);
            Assert.AreEqual(2, structurallyParsed.Count);

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAllocationsXml(
                xml,
                "Execution",
                new Dictionary<string, decimal>
                {
                    ["PaperA"] = 0m,
                    ["PaperB"] = 2m
                });
            var updated = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(
                updatedXml,
                validateAllocationConfiguration: false);

            Assert.AreEqual(0m, updated["Execution"].AccountAllocationValues["PaperA"]);
            Assert.AreEqual(2m, updated["Execution"].AccountAllocationValues["PaperB"]);
            Assert.AreEqual(80m, updated["InvalidPercent"].AccountAllocationValues.Values.Sum());
        }

        [Test]
        public void DuplicateGroupAccountIsStructurallyRejected()
        {
            const string xml = """
                <ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                  <String>PaperA</String><String>papera</String>
                </ListOfAccts></Group></ListOfGroups>
                """;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(
                    xml,
                    validateAllocationConfiguration: false));

            StringAssert.Contains("duplicate account", exception.Message);
        }

        [Test]
        public void MalformedGroupTopologyFailsTheWholeParse()
        {
            var malformed = new[]
            {
                "<ListOfGroups><Group><name> </name><defaultMethod>NetLiq</defaultMethod>" +
                    "<ListOfAccts><String>PaperA</String></ListOfAccts></Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod> </defaultMethod>" +
                    "<ListOfAccts><String>PaperA</String></ListOfAccts></Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>" +
                    "</Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>" +
                    "<ListOfAccts><String>PaperA</String></ListOfAccts>" +
                    "<Accounts><String>PaperB</String></Accounts></Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>" +
                    "<ListOfAccts><Unsupported>PaperA</Unsupported></ListOfAccts></Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>" +
                    "<ListOfAccts><String> </String></ListOfAccts></Group></ListOfGroups>",
                "<ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>" +
                    "<ListOfAccts><String><amount>1</amount></String></ListOfAccts></Group></ListOfGroups>"
            };

            foreach (var xml in malformed)
            {
                Assert.Catch<InvalidOperationException>(() =>
                    InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(
                        xml,
                        validateAllocationConfiguration: false));
            }
        }

        [Test]
        public void GroupAllocationUpdateReplacesCompleteVectorAndPreservesMembershipAndOtherGroups()
        {
            const string xml = """
                <ListOfGroups xmlns="urn:ib-test">
                  <Group><name>Execution</name><defaultMethod>ContractsOrShares</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>0.4</amount></Account>
                    <Account><acct>PaperB</acct><amount>0.6</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>Other</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <Account><acct>PaperC</acct></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;
            var allocations = new Dictionary<string, decimal>
            {
                ["papera"] = 1m,
                ["PaperB"] = 0m
            };

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAllocationsXml(
                xml, "execution", allocations);
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);

            CollectionAssert.AreEquivalent(new[] { "PaperA", "PaperB" }, groups["Execution"].AccountIds);
            Assert.AreEqual(1m, groups["Execution"].AccountAllocationValues["PaperA"]);
            Assert.AreEqual(0m, groups["Execution"].AccountAllocationValues["PaperB"]);
            CollectionAssert.AreEqual(new[] { "PaperC" }, groups["Other"].AccountIds);
            Assert.AreEqual("NetLiq", groups["Other"].AllocationMethod);
        }

        [Test]
        public void UserSpecifiedUpdatesPreserveAttributeBasedAccountRepresentation()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Execution</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account acct="PaperA" amount="1" />
                    <Account acct="PaperB" amount="2"><amount>2</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>Source</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <String>PaperC</String><String>PaperD</String>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var allocationXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAllocationsXml(
                xml,
                "Execution",
                new Dictionary<string, decimal> { ["PaperA"] = 3m, ["PaperB"] = 4m });
            var allocationAccounts = XDocument.Parse(allocationXml).Descendants("Account").ToArray();

            Assert.AreEqual("3", allocationAccounts[0].Attribute("amount")?.Value);
            Assert.IsNull(allocationAccounts[0].Element("amount"));
            Assert.AreEqual("4", allocationAccounts[1].Attribute("amount")?.Value);
            Assert.AreEqual("4", allocationAccounts[1].Element("amount")?.Value);

            var assignmentXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                allocationXml, "PaperC", "Execution", 5m);
            var addedAccount = XDocument.Parse(assignmentXml).Descendants("Account")
                .Single(element => element.Attribute("acct")?.Value == "PaperC");

            Assert.AreEqual("5", addedAccount.Attribute("amount")?.Value);
            Assert.IsNull(addedAccount.Element("amount"));
            Assert.AreEqual(5m, InteractiveBrokersFinancialAdvisorAccountState
                .ParseGroups(assignmentXml)["Execution"].AccountAllocationValues["PaperC"]);
        }

        [Test]
        public void ComputedAssignmentPreservesAttributeBasedAccountIdentifier()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <Account acct="PaperA" /><Account acct="PaperB" />
                  </ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <Account acct="PaperC" />
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var assignmentXml =
                InteractiveBrokersFinancialAdvisorAccountState
                    .UpdateAccountGroupAssignmentXml(
                        xml,
                        "PaperB",
                        "Destination");
            var account = XDocument.Parse(assignmentXml).Descendants("Account")
                .Single(element => element.Attribute("acct")?.Value == "PaperB");

            Assert.IsEmpty(account.Value);
            CollectionAssert.AreEquivalent(
                new[] { "PaperB", "PaperC" },
                InteractiveBrokersFinancialAdvisorAccountState
                    .ParseGroups(assignmentXml)["Destination"].AccountIds);
        }

        [Test]
        public void GroupAllocationUpdateRequiresExactMembersAndValidMethodSpecificValues()
        {
            var contracts = new BrokerageAccountGroup(
                "Contracts", "ContractsOrShares", new[] { "PaperA", "PaperB" },
                new Dictionary<string, decimal> { ["PaperA"] = 0.5m, ["PaperB"] = 0.5m });
            var ratio = new BrokerageAccountGroup(
                "Ratio", "Ratio", new[] { "PaperA", "PaperB" },
                new Dictionary<string, decimal> { ["PaperA"] = 1m, ["PaperB"] = 1m });
            var percent = new BrokerageAccountGroup(
                "Percent", "Percent", new[] { "PaperA", "PaperB" },
                new Dictionary<string, decimal> { ["PaperA"] = 50m, ["PaperB"] = 50m });
            var computed = new BrokerageAccountGroup("Computed", "NetLiq", new[] { "PaperA", "PaperB" });
            var groups = new Dictionary<string, BrokerageAccountGroup>
            {
                [contracts.Name] = contracts,
                [ratio.Name] = ratio,
                [percent.Name] = percent,
                [computed.Name] = computed
            };

            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                contracts.Name,
                new Dictionary<string, decimal> { ["PaperA"] = 1m, ["PaperB"] = 0m },
                groups));
            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                contracts.Name,
                new Dictionary<string, decimal> { ["PaperA"] = 0m, ["PaperB"] = 0m },
                groups));
            StringAssert.Contains("exactly match", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                    contracts.Name, new Dictionary<string, decimal> { ["PaperA"] = 1m }, groups)).Message);
            StringAssert.Contains("non-negative", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                    contracts.Name,
                    new Dictionary<string, decimal> { ["PaperA"] = 1m, ["PaperB"] = -1m },
                    groups)).Message);
            StringAssert.Contains("positive", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                    ratio.Name,
                    new Dictionary<string, decimal> { ["PaperA"] = 1m, ["PaperB"] = 0m },
                    groups)).Message);
            StringAssert.Contains("total 100", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                    percent.Name,
                    new Dictionary<string, decimal> { ["PaperA"] = 60m, ["PaperB"] = 30m },
                    groups)).Message);
            StringAssert.Contains("does not accept", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAllocationUpdate(
                    computed.Name,
                    new Dictionary<string, decimal> { ["PaperA"] = 1m, ["PaperB"] = 1m },
                    groups)).Message);
        }

        [Test]
        public void DuplicateUnifiedGroupNamesAreRejectedExplicitly()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>SameName</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><Account><acct>PaperA</acct></Account></ListOfAccts></Group>
                  <Group><name>SameName</name><defaultMethod>ContractsOrShares</defaultMethod>
                    <ListOfAccts><Account><acct>PaperA</acct><amount>0.5</amount></Account></ListOfAccts></Group>
                </ListOfGroups>
                """;

            var exception = Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml));

            StringAssert.Contains("duplicate group name 'SameName'", exception.Message);
        }

        [Test]
        public void AssignmentRejectsAmbiguousDuplicateUnifiedGroupNames()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>SameName</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><Account><acct>PaperA</acct></Account></ListOfAccts></Group>
                  <Group><name>SameName</name><defaultMethod>ContractsOrShares</defaultMethod>
                    <ListOfAccts><Account><acct>PaperA</acct><amount>0.5</amount></Account></ListOfAccts></Group>
                </ListOfGroups>
                """;

            var exception = Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperB", "SameName"));

            StringAssert.Contains("assignment is ambiguous", exception.Message);
        }

        [Test]
        public void AssignmentRemovesEveryExistingMembershipAndPreservesUnrelatedConfiguration()
        {
            const string xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ListOfGroups>
                  <Group>
                    <name>NetLiqGroup</name>
                    <defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts varName="list"><String>PaperA</String><String>PaperB</String></ListOfAccts>
                  </Group>
                  <Group>
                    <name>RatioGroup</name>
                    <defaultMethod>Ratio</defaultMethod>
                    <ListOfAccts varName="list">
                      <Account><acct>PaperB</acct><amount>1.25</amount></Account>
                      <Account><acct>PaperE</acct><amount>2.75</amount></Account>
                    </ListOfAccts>
                  </Group>
                  <Group>
                    <name>Destination</name>
                    <defaultMethod>AvailableEquity</defaultMethod>
                    <ListOfAccts varName="list"><String>PaperC</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperB", "Destination");
            var updated = XDocument.Parse(updatedXml);
            var ratioAfter = updated.Descendants("Group")
                .Single(group => group.Element("name")?.Value == "RatioGroup").ToString(SaveOptions.DisableFormatting);
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);

            CollectionAssert.AreEqual(new[] { "PaperA" }, groups["NetLiqGroup"].AccountIds);
            CollectionAssert.AreEqual(new[] { "PaperE" }, groups["RatioGroup"].AccountIds);
            CollectionAssert.AreEquivalent(new[] { "PaperC", "PaperB" }, groups["Destination"].AccountIds);
            CollectionAssert.AreEqual(new[] { "Destination" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(groups, "PaperB"));
            Assert.IsTrue(ratioAfter.Contains("2.75"));
            Assert.IsFalse(ratioAfter.Contains("1.25"));
        }

        [Test]
        public void AssignmentPreservesAccountSpecificMetadataFromTheSourceGroup()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account clientTag="retain"><acct>PaperA</acct><amount>1</amount><custom>value</custom></Account>
                    <Account><acct>PaperB</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account><acct>PaperC</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "Destination", 2m);
            var account = XDocument.Parse(updatedXml).Descendants("Account")
                .Single(element => element.Element("acct")?.Value == "PaperA");

            Assert.AreEqual("retain", account.Attribute("clientTag")?.Value);
            Assert.AreEqual("value", account.Element("custom")?.Value);
            Assert.AreEqual("2", account.Element("amount")?.Value);
        }

        [Test]
        public void AssignmentPreservesDirectTextAccountIdentifiersAndAllocationMetadata()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account clientTag="retain" amount="1">PaperA</Account>
                    <Account><acct>PaperB</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account><acct>PaperC</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "Destination", 2m);
            var account = XDocument.Parse(updatedXml).Descendants("Account")
                .Single(element => string.Concat(
                    element.Nodes().OfType<XText>().Select(text => text.Value)).Trim() == "PaperA");

            Assert.AreEqual("retain", account.Attribute("clientTag")?.Value);
            Assert.AreEqual("2", account.Attribute("amount")?.Value);
            Assert.IsFalse(account.Elements().Any());
        }

        [TestCase("<Account acct=\"PaperA\">PaperA</Account>")]
        [TestCase("<Account><acct>PaperA</acct>PaperA</Account>")]
        public void MixedStructuredAndDirectTextAccountIdentifiersAreRejected(string accountElement)
        {
            var xml = $"""
                <ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>
                  <ListOfAccts>{accountElement}</ListOfAccts>
                </Group></ListOfGroups>
                """;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml));

            StringAssert.Contains("structured account identifier with direct text", exception.Message);
        }

        [TestCase("<Account acct=\"PaperA\">\n  </Account>")]
        [TestCase("<Account>\n  <acct>PaperA</acct>\n</Account>")]
        public void StructuredAccountIdentifiersAllowFormattingWhitespace(string accountElement)
        {
            var xml = $"""
                <ListOfGroups><Group><name>Group</name><defaultMethod>NetLiq</defaultMethod>
                  <ListOfAccts>{accountElement}</ListOfAccts>
                </Group></ListOfGroups>
                """;

            var group = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(xml)["Group"];

            CollectionAssert.AreEqual(new[] { "PaperA" }, group.AccountIds);
        }

        [Test]
        public void AssignmentPreflightRejectsMixedAccountIdentifiersOutsideTheMutationTarget()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <Account acct="PaperA">PaperA</Account><String>PaperB</String>
                  </ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>NetLiq</defaultMethod><ListOfAccts>
                    <String>PaperC</String>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml,
                    "PaperB",
                    "Destination"));

            StringAssert.Contains("structured account identifier with direct text", exception.Message);
        }

        [Test]
        public void AssignmentRejectsUnsupportedSiblingMetadataForANewAccount()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Destination</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account clientTag="belongs-to-PaperA"><acct>PaperA</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperB", "Destination", 2m));

            StringAssert.Contains("unsupported metadata", exception.Message);
        }

        [TestCase("NetLiq", "<String>PaperA</String>")]
        [TestCase("AvailableEquity", "<String>PaperA</String>")]
        [TestCase("ContractsOrShares", "<Account><acct>PaperA</acct><amount>1</amount></Account>")]
        [TestCase("Ratio", "<Account><acct>PaperA</acct><amount>1</amount></Account>")]
        [TestCase("Percent", "<Account><acct>PaperA</acct><amount>100</amount></Account>")]
        public void AssignmentRejectsRemovingTheFinalAccountFromAnyGroup(
            string allocationMethod,
            string accountElement)
        {
            var xml = $"""
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>{allocationMethod}</defaultMethod>
                    <ListOfAccts>{accountElement}</ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperB</String></ListOfAccts></Group>
                </ListOfGroups>
                """;

            var removeException = Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperA", string.Empty));
            var moveException = Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperA", "Destination"));

            StringAssert.Contains("final account", removeException.Message);
            StringAssert.Contains("final account", moveException.Message);
        }

        [TestCase("Percent", "40", "60")]
        [TestCase("Ratio", "7.5", "2.5")]
        [TestCase("ContractsOrShares", "19.75", "10.25")]
        public void AssignmentPreservesExistingTargetAllocationAndRemovesDuplicateMemberships(
            string allocationMethod,
            string accountAAllocation,
            string accountBAllocation)
        {
            var xml = $"""
                <ListOfGroups>
                  <Group><name>Computed</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperA</String><String>PaperC</String></ListOfAccts></Group>
                  <Group><name>ValueBasedTarget</name><defaultMethod>{allocationMethod}</defaultMethod>
                    <ListOfAccts>
                      <Account><acct>PaperA</acct><amount>{accountAAllocation}</amount></Account>
                      <Account><acct>PaperB</acct><amount>{accountBAllocation}</amount></Account>
                    </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "ValueBasedTarget");
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);
            var target = groups["ValueBasedTarget"];

            CollectionAssert.AreEqual(new[] { "ValueBasedTarget" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(groups, "PaperA"));
            Assert.AreEqual(
                decimal.Parse(accountAAllocation, CultureInfo.InvariantCulture),
                target.AccountAllocationValues["PaperA"]);
            Assert.AreEqual(
                decimal.Parse(accountBAllocation, CultureInfo.InvariantCulture),
                target.AccountAllocationValues["PaperB"]);
        }

        [TestCase("ContractsOrShares")]
        [TestCase("Ratio")]
        public void AssignmentAddsAndUpdatesExplicitUserSpecifiedAllocation(string allocationMethod)
        {
            var xml = $"""
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperB</String><String>PaperC</String></ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>{allocationMethod}</defaultMethod>
                    <ListOfAccts><Account><acct>PaperA</acct><amount>1.25</amount></Account></ListOfAccts></Group>
                </ListOfGroups>
                """;

            var addedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperB", "Destination", 2.75m);
            var added = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(addedXml)["Destination"];

            CollectionAssert.AreEquivalent(new[] { "PaperA", "PaperB" }, added.AccountIds);
            Assert.AreEqual(1.25m, added.AccountAllocationValues["PaperA"]);
            Assert.AreEqual(2.75m, added.AccountAllocationValues["PaperB"]);

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                addedXml, "PaperB", "Destination", 4.5m);
            var updated = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml)["Destination"];

            Assert.AreEqual(1.25m, updated.AccountAllocationValues["PaperA"]);
            Assert.AreEqual(4.5m, updated.AccountAllocationValues["PaperB"]);
        }

        [Test]
        public void PercentAssignmentNormalizesSourceAndDestinationWhilePreservingRelativeWeights()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Source</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>60</amount></Account>
                    <Account><acct>PaperB</acct><amount>40</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>Destination</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperC</acct><amount>70</amount></Account>
                    <Account><acct>PaperD</acct><amount>30</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "Destination", 20m);
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);

            Assert.AreEqual(100m, groups["Source"].AccountAllocationValues["PaperB"]);
            Assert.AreEqual(20m, groups["Destination"].AccountAllocationValues["PaperA"]);
            Assert.AreEqual(56m, groups["Destination"].AccountAllocationValues["PaperC"]);
            Assert.AreEqual(24m, groups["Destination"].AccountAllocationValues["PaperD"]);
            Assert.AreEqual(100m, groups["Destination"].AccountAllocationValues.Values.Sum());
            CollectionAssert.AreEqual(new[] { "Destination" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(groups, "PaperA"));
        }

        [Test]
        public void PercentRemovalNormalizesRemainingMembers()
        {
            const string xml = """
                <ListOfGroups><Group><name>PercentGroup</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>33.3333</amount></Account>
                  <Account><acct>PaperB</acct><amount>66.6667</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", string.Empty);
            var group = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml)["PercentGroup"];

            CollectionAssert.AreEqual(new[] { "PaperB" }, group.AccountIds);
            Assert.AreEqual(100m, group.AccountAllocationValues["PaperB"]);
        }

        [Test]
        public void AssignmentRepairsMultiplePercentMembershipsAndNormalizesEverySource()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>PercentOne</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>25</amount></Account>
                    <Account><acct>PaperB</acct><amount>75</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>PercentTwo</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>60</amount></Account>
                    <Account><acct>PaperC</acct><amount>40</amount></Account>
                  </ListOfAccts></Group>
                  <Group><name>RatioTarget</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                    <Account><acct>PaperD</acct><amount>1</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "RatioTarget", 2m);
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);

            Assert.AreEqual(100m, groups["PercentOne"].AccountAllocationValues["PaperB"]);
            Assert.AreEqual(100m, groups["PercentTwo"].AccountAllocationValues["PaperC"]);
            Assert.AreEqual(2m, groups["RatioTarget"].AccountAllocationValues["PaperA"]);
            CollectionAssert.AreEqual(new[] { "RatioTarget" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(groups, "PaperA"));
        }

        [Test]
        public void ExistingPercentTargetWithoutValuePreservesConfigurationWhileRemovingOtherMemberships()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>Computed</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperA</String><String>PaperC</String></ListOfAccts></Group>
                  <Group><name>PercentTarget</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                    <Account><acct>PaperA</acct><amount>30</amount></Account>
                    <Account><acct>PaperB</acct><amount>70</amount></Account>
                  </ListOfAccts></Group>
                </ListOfGroups>
                """;

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "PercentTarget");
            var groups = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml);

            Assert.AreEqual(30m, groups["PercentTarget"].AccountAllocationValues["PaperA"]);
            Assert.AreEqual(70m, groups["PercentTarget"].AccountAllocationValues["PaperB"]);
            CollectionAssert.AreEqual(new[] { "PercentTarget" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(groups, "PaperA"));
        }

        [Test]
        public void PercentAssignmentToEmptyGroupRequiresOneHundredPercent()
        {
            const string xml = """
                <ListOfGroups><Group><name>PercentGroup</name><defaultMethod>Percent</defaultMethod>
                  <ListOfAccts></ListOfAccts></Group></ListOfGroups>
                """;

            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                    xml, "PaperA", "PercentGroup", 25m));

            var updatedXml = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml, "PaperA", "PercentGroup", 100m);
            var group = InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updatedXml)["PercentGroup"];
            Assert.AreEqual(100m, group.AccountAllocationValues["PaperA"]);
        }

        [Test]
        public void InvalidUserSpecifiedGroupConfigurationsAreRejected()
        {
            const string missing = """
                <ListOfGroups><Group><name>RatioGroup</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string badPercentTotal = """
                <ListOfGroups><Group><name>PercentGroup</name><defaultMethod>Percent</defaultMethod><ListOfAccts>
                  <Account><acct>PaperA</acct><amount>20</amount></Account>
                  <Account><acct>PaperB</acct><amount>70</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;

            StringAssert.Contains("has no allocation value", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(missing)).Message);
            StringAssert.Contains("must total 100", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(badPercentTotal)).Message);
        }

        [Test]
        public void AssignmentRejectsUnsupportedOrInapplicableAllocationValues()
        {
            var managed = new[] { "PaperMaster", "PaperA", "PaperB" };
            var computed = new BrokerageAccountGroup("Computed", "NetLiq", new[] { "PaperA" });
            var monetary = new BrokerageAccountGroup(
                "Monetary",
                "MonetaryAmount",
                new[] { "PaperA" },
                new Dictionary<string, decimal> { ["PaperA"] = 100m });

            StringAssert.Contains("does not accept", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperB", computed.Name,
                    new Dictionary<string, BrokerageAccountGroup> { [computed.Name] = computed },
                    managed, "PaperMaster", 1m)).Message);
            StringAssert.Contains("not supported", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperB", monetary.Name,
                    new Dictionary<string, BrokerageAccountGroup> { [monetary.Name] = monetary },
                    managed, "PaperMaster", 100m)).Message);
            StringAssert.Contains("cannot be supplied", Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperB", string.Empty,
                    new Dictionary<string, BrokerageAccountGroup>(),
                    managed, "PaperMaster", 1m)).Message);
        }

        [Test]
        public void ConfigurationHashIgnoresFormattingAndGroupAccountOrderingButIncludesAllocationValues()
        {
            const string first = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                <Account><acct>A</acct><amount>1</amount></Account><Account><acct>B</acct><amount>2</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string reordered = """
                <ListOfGroups>
                  <Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                  <Account><acct>B</acct><amount>2</amount></Account>
                  <Account><acct>A</acct><amount>1</amount></Account></ListOfAccts></Group>
                </ListOfGroups>
                """;
            const string changedAmount = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                <Account><acct>A</acct><amount>1</amount></Account><Account><acct>B</acct><amount>3</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string equivalentNumericFormatting = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                <Account><acct>A</acct><amount>1.0</amount></Account><Account><acct>B</acct><amount>2.000</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string attributeNumericFormatting = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                <Account acct="A" amount="1" /><Account acct="B" amount="2" />
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string equivalentAttributeNumericFormatting = """
                <ListOfGroups><Group><name>G</name><defaultMethod>Ratio</defaultMethod><ListOfAccts>
                <Account acct="A" amount="1.0" /><Account acct="B" amount="2.000" />
                </ListOfAccts></Group></ListOfGroups>
                """;

            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(reordered));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(changedAmount));
            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(equivalentNumericFormatting));
            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(attributeNumericFormatting),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(
                    equivalentAttributeNumericFormatting));
        }

        [Test]
        public void ConfigurationHashKeepsMixedContentAccountValuesPairedWhenSortingAccounts()
        {
            const string first = """
                <ListOfGroups><Group><ListOfAccts>
                <Account>A<amount>1</amount></Account><Account>B<amount>2</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string reordered = """
                <ListOfGroups><Group><ListOfAccts>
                <Account>B<amount>2.0</amount></Account><Account>A<amount>1.000</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;
            const string swappedAmounts = """
                <ListOfGroups><Group><ListOfAccts>
                <Account>A<amount>2</amount></Account><Account>B<amount>1</amount></Account>
                </ListOfAccts></Group></ListOfGroups>
                """;

            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(reordered));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(swappedAmounts));
        }

        [Test]
        public void ConfigurationHashIncludesNamespacesAndUnknownElementOrder()
        {
            const string namespaceA =
                "<ListOfGroups xmlns='urn:a'><Extension><A/><B/></Extension></ListOfGroups>";
            const string namespaceB =
                "<ListOfGroups xmlns='urn:b'><Extension><A/><B/></Extension></ListOfGroups>";
            const string reorderedExtension =
                "<ListOfGroups xmlns='urn:a'><Extension><B/><A/></Extension></ListOfGroups>";

            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(namespaceA),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(namespaceB));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(namespaceA),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeConfigurationHash(
                    reorderedExtension));
        }

        [Test]
        public void AssignmentValidationRejectsSavedPctChangeAndSupportsExplicitUserSpecifiedValues()
        {
            var computed = new BrokerageAccountGroup("Computed", "NetLiq", new[] { "PaperA" });
            var computedGroups = new Dictionary<string, BrokerageAccountGroup> { [computed.Name] = computed };
            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                "PaperB",
                computed.Name,
                computedGroups,
                new[] { "PaperMaster", "PaperA", "PaperB" },
                "PaperMaster"));

            var savedPctChange = new BrokerageAccountGroup(
                "SavedPctChange",
                "PctChange",
                new[] { "PaperA" });
            StringAssert.Contains(
                "not supported",
                Assert.Throws<System.InvalidOperationException>(() =>
                    InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                        "PaperB",
                        savedPctChange.Name,
                        new Dictionary<string, BrokerageAccountGroup>
                        {
                            [savedPctChange.Name] = savedPctChange
                        },
                        new[] { "PaperMaster", "PaperA", "PaperB" },
                        "PaperMaster")).Message);

            var valueBased = new BrokerageAccountGroup(
                "ValueBased",
                "Ratio",
                new[] { "PaperA" },
                new Dictionary<string, decimal> { ["PaperA"] = 1m });
            var valueBasedGroups = new Dictionary<string, BrokerageAccountGroup> { [valueBased.Name] = valueBased };
            var exception = Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperB",
                    valueBased.Name,
                    valueBasedGroups,
                    new[] { "PaperMaster", "PaperA", "PaperB" },
                    "PaperMaster"));
            StringAssert.Contains("explicit allocation value", exception.Message);

            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                "PaperB", valueBased.Name, valueBasedGroups,
                new[] { "PaperMaster", "PaperA", "PaperB" }, "PaperMaster", 2.5m));

            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                "PaperA", valueBased.Name, valueBasedGroups,
                new[] { "PaperMaster", "PaperA", "PaperB" }, "PaperMaster"));
            Assert.DoesNotThrow(() => InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                "PaperA", string.Empty, valueBasedGroups,
                new[] { "PaperMaster", "PaperA", "PaperB" }, "PaperMaster"));
        }

        [Test]
        public void AssignmentValidationRejectsMasterUnknownAndUnmanagedTargetGroup()
        {
            var target = new BrokerageAccountGroup("GroupOne", "NetLiq", new[] { "PaperA" });
            var other = new BrokerageAccountGroup("GroupTwo", "NetLiq", new[] { "PaperB" });
            var groups = new Dictionary<string, BrokerageAccountGroup>
            {
                [target.Name] = target,
                [other.Name] = other
            };
            var managed = new[] { "PaperMaster", "PaperA", "PaperB", "PaperC" };

            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperMaster", target.Name, groups, managed, "PaperMaster"));
            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "Unknown", target.Name, groups, managed, "PaperMaster"));
            Assert.Throws<System.InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateGroupAssignment(
                    "PaperA", other.Name,
                    new Dictionary<string, BrokerageAccountGroup> { [target.Name] = target },
                    managed, "PaperMaster"));
        }

        [Test]
        public void ManagedAccountIdentifiersUseBrokerageCanonicalCasing()
        {
            var accountId = InteractiveBrokersFinancialAdvisorAccountState.GetCanonicalManagedAccountId(
                new[] { "PaperAccountA", "PaperAccountB" },
                " paperaccounta ".Trim());

            Assert.AreEqual("PaperAccountA", accountId);
        }

        [Test]
        public void AmbiguousManagedAccountIdentifierCasingIsRejected()
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.GetCanonicalManagedAccountId(
                    new[] { "PaperAccountA", "paperaccounta" },
                    "PAPERACCOUNTA"));

            StringAssert.Contains("ambiguous", exception.Message.ToLowerInvariant());
        }

        [Test]
        public void UnassignedAccountsAreComputedAgainstEveryBrokerageGroup()
        {
            var groups = new Dictionary<string, BrokerageAccountGroup>
            {
                ["SelectedGroup"] = new("SelectedGroup", "NetLiq", new[] { "PaperA" }),
                ["OtherGroup"] = new("OtherGroup", "NetLiq", new[] { "PaperB" })
            };

            var unassigned = InteractiveBrokersFinancialAdvisorAccountState.ComputeUnassignedAccountIds(
                groups,
                new[] { "PaperMaster", "PaperMasterA", "PaperA", "PaperB", "PaperC" },
                "PaperMaster");

            CollectionAssert.AreEqual(new[] { "PaperC" }, unassigned);
        }

        [Test]
        public void CompleteDirectoryDistinguishesPrimaryAggregateManagedAndUnknownAccounts()
        {
            var groups = new Dictionary<string, BrokerageAccountGroup>
            {
                ["GroupOne"] = new("GroupOne", "NetLiq", new[] { "PaperA", "Orphan" }),
                ["GroupTwo"] = new("GroupTwo", "NetLiq", new[] { "PaperA", "PaperB" })
            };
            var accountStates = new Dictionary<string, BrokerageAccountState>
            {
                ["PaperA"] = new("PaperA", new[] { "GroupOne", "GroupTwo" }, "INDIVIDUAL",
                    1m, 1m, 1m, 1m, 1m, "USD", null, null)
            };

            var directory = InteractiveBrokersFinancialAdvisorAccountState.BuildAccountDirectory(
                "PaperMaster",
                new[] { "PaperMaster", "PaperMasterA", "PaperA", "PaperB" },
                groups,
                new Dictionary<string, string>
                {
                    ["PaperA"] = "PaperMasterA",
                    ["Associated"] = "OtherFamilyA"
                },
                accountStates,
                new Dictionary<string, string>
                {
                    ["PaperMaster"] = "Advisor",
                    ["PaperA"] = "Client [FA:GroupOne]",
                    ["AliasOnly"] = "Associated Alias"
                });

            Assert.AreEqual(BrokerageAccountRelationship.Primary, directory["PaperMaster"].Relationship);
            Assert.AreEqual(BrokerageAccountRelationship.Aggregate, directory["PaperMasterA"].Relationship);
            Assert.AreEqual(BrokerageAccountRelationship.Managed, directory["PaperA"].Relationship);
            Assert.AreEqual(BrokerageAccountRelationship.Unknown, directory["Orphan"].Relationship);
            Assert.AreEqual(BrokerageAccountRelationship.Unknown, directory["Associated"].Relationship);
            Assert.AreEqual(BrokerageAccountRelationship.Unknown, directory["AliasOnly"].Relationship);
            Assert.AreEqual("OtherFamilyA", directory["Associated"].FamilyCode);
            CollectionAssert.AreEqual(new[] { "GroupOne", "GroupTwo" }, directory["PaperA"].GroupNames);
            Assert.AreEqual("INDIVIDUAL", directory["PaperA"].AccountType);
            Assert.AreEqual("PaperMasterA", directory["PaperA"].FamilyCode);
            Assert.AreEqual("Advisor", directory["PaperMaster"].AccountAlias);
            Assert.AreEqual("Client [FA:GroupOne]", directory["PaperA"].AccountAlias);
            Assert.AreEqual("Associated Alias", directory["AliasOnly"].AccountAlias);
            Assert.IsEmpty(directory["PaperB"].AccountAlias);
            CollectionAssert.IsEmpty(directory["PaperMaster"].GroupNames);
        }

        [Test]
        public void MembershipHashChangesForMembershipOrAliasButNotOrdering()
        {
            var initial = new Dictionary<string, BrokerageAccountGroup>
            {
                ["GroupOne"] = new("GroupOne", "NetLiq", new[] { "PaperA", "PaperB" })
            };
            var reordered = new Dictionary<string, BrokerageAccountGroup>
            {
                ["GroupOne"] = new("GroupOne", "NetLiq", new[] { "PaperB", "PaperA" })
            };
            var changed = new Dictionary<string, BrokerageAccountGroup>
            {
                ["GroupOne"] = new("GroupOne", "NetLiq", new[] { "PaperA", "PaperC" })
            };

            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(reordered));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(changed));
            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial, new[] { "PaperB", "PaperA", "PaperUnassigned" }),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial, new[] { "paperunassigned", "papera", "paperb" }));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial, new[] { "PaperA", "PaperB" }),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(initial, new[] { "PaperA", "PaperB", "PaperUnassigned" }));
            Assert.AreEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperA", "PaperB" },
                    new Dictionary<string, string> { ["PaperA"] = "Alias A", ["PaperB"] = "Alias B" }),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperB", "PaperA" },
                    new Dictionary<string, string> { ["paperb"] = "Alias B", ["papera"] = "Alias A" }));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperA", "PaperB" },
                    new Dictionary<string, string> { ["PaperA"] = "Alias A" }),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperA", "PaperB" },
                    new Dictionary<string, string> { ["PaperA"] = "Alias Changed" }));
            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperA", "PaperB" },
                    familyCodes: new Dictionary<string, string> { ["PaperA"] = "FamilyA" }),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(
                    initial,
                    new[] { "PaperA", "PaperB" },
                    familyCodes: new Dictionary<string, string> { ["PaperA"] = "FamilyB" }));
        }

        [Test]
        public void MembershipHashEncodingDoesNotCollideOnDelimiterText()
        {
            var first = new Dictionary<string, BrokerageAccountGroup>
            {
                ["A:B"] = new("A:B", "C", new[] { "D" })
            };
            var second = new Dictionary<string, BrokerageAccountGroup>
            {
                ["A"] = new("A", "B", new[] { "C:D" })
            };

            Assert.AreNotEqual(
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(first),
                InteractiveBrokersFinancialAdvisorAccountState.ComputeMembershipHash(second));
        }

        [Test]
        public void SelectedGroupsPreserveOverlappingAccountOwnershipForRepair()
        {
            var groups = new Dictionary<string, BrokerageAccountGroup>
            {
                ["GroupOne"] = new("GroupOne", "NetLiq", new[] { "PaperA", "PaperB" }),
                ["GroupTwo"] = new("GroupTwo", "NetLiq", new[] { "PaperB", "PaperC" })
            };

            var selected = InteractiveBrokersFinancialAdvisorAccountState.SelectGroups(
                groups,
                new[] { "GroupOne", "GroupTwo" });

            Assert.AreEqual(2, selected.Count);
            CollectionAssert.AreEqual(new[] { "GroupOne", "GroupTwo" },
                InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(selected, "PaperB"));
        }

        [Test]
        public void NormalizesDerivativeAverageCostByContractMultiplier()
        {
            var contract = new Contract { Multiplier = "100" };

            Assert.AreEqual(12.5m,
                InteractiveBrokersFinancialAdvisorAccountState.NormalizeAveragePrice(contract, 1250d));
            Assert.AreEqual(1250m,
                InteractiveBrokersFinancialAdvisorAccountState.NormalizeAveragePrice(new Contract(), 1250d));
        }

        [Test]
        public void PreservesRawPositionWhenContractCannotBeMapped()
        {
            var row = new PositionMultiEventArgs(
                1,
                "PaperA",
                "Model",
                new Contract
                {
                    ConId = 123,
                    Symbol = "UNKNOWN",
                    LocalSymbol = "UNKNOWN DEC26",
                    SecType = "FUT",
                    Currency = "USD",
                    Exchange = "EXCHANGE",
                    PrimaryExch = "PRIMARY",
                    TradingClass = "CLASS",
                    LastTradeDateOrContractMonth = "202612",
                    Strike = 10d,
                    Right = "C",
                    Multiplier = "50"
                },
                2.5m,
                5000d);

            var position = InteractiveBrokersFinancialAdvisorAccountState.CreateUnmappedPosition(
                row,
                "mapping failed");

            Assert.AreEqual("123", position.BrokerageContractId);
            Assert.AreEqual("UNKNOWN", position.BrokerageSymbol);
            Assert.AreEqual("FUT", position.BrokerageSecurityType);
            Assert.AreEqual(2.5m, position.Quantity);
            Assert.AreEqual(100m, position.AveragePrice);
            Assert.AreEqual("Model", position.ModelCode);
            Assert.AreEqual("mapping failed", position.ErrorMessage);
        }

        [Test]
        public void PreservesNonDecimalBrokerageValuesWithoutThrowing()
        {
            var row = new PositionMultiEventArgs(
                1,
                "PaperA",
                string.Empty,
                new Contract { Symbol = "UNKNOWN", Strike = double.PositiveInfinity },
                1m,
                double.NaN);

            var position = InteractiveBrokersFinancialAdvisorAccountState.CreateUnmappedPosition(
                row,
                "mapping failed");

            Assert.AreEqual(0m, position.Strike);
            Assert.AreEqual(0m, position.AveragePrice);
            Assert.AreEqual("Infinity", position.BrokerageStrike);
            Assert.AreEqual("NaN", position.BrokerageAverageCost);
            StringAssert.Contains("could not be represented", position.ErrorMessage);
            StringAssert.Contains("could not be normalized", position.ErrorMessage);
        }

        [Test]
        public void AssignmentRepairsDuplicateMembershipTest()
        {
            const string xml = """
                <ListOfGroups>
                  <Group><name>SourceA</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperA</String><String>PaperB</String></ListOfAccts>
                  </Group>
                  <Group><name>SourceB</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperA</String><String>PaperC</String></ListOfAccts>
                  </Group>
                  <Group><name>Destination</name><defaultMethod>NetLiq</defaultMethod>
                    <ListOfAccts><String>PaperD</String></ListOfAccts>
                  </Group>
                </ListOfGroups>
                """;

            var updated = InteractiveBrokersFinancialAdvisorAccountState.UpdateAccountGroupAssignmentXml(
                xml,
                "PaperA",
                "Destination");
            var resultingGroups = InteractiveBrokersFinancialAdvisorAccountState.GetAccountGroupNames(
                InteractiveBrokersFinancialAdvisorAccountState.ParseGroups(updated),
                "PaperA");

            Assert.DoesNotThrow(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateResultingAssignment(
                    "PaperA",
                    "Destination",
                    resultingGroups));
            CollectionAssert.AreEqual(new[] { "Destination" }, resultingGroups);
            Assert.DoesNotThrow(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateResultingAssignment(
                    "PaperA",
                    string.Empty,
                    Array.Empty<string>()));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                InteractiveBrokersFinancialAdvisorAccountState.ValidateResultingAssignment(
                    "PaperA",
                    "Destination",
                    new[] { "Source", "Destination" }));

            StringAssert.Contains("only group 'Destination'", exception.Message);
        }

        [Test]
        public void AccountValueBuilderAcceptsOrdinaryCashBalanceTest()
        {
            var builder =
                new InteractiveBrokersFinancialAdvisorAccountState.AccountValueBuilder(
                    "PaperA",
                    Array.Empty<string>());
            builder.Apply("AccountReady", "true", string.Empty);
            builder.Apply("NetLiquidation", "1000", "USD");
            builder.Apply("TotalCashValue", "250", "USD");
            builder.Apply("CashBalance", "250", "USD");

            var account = builder.Build(Array.Empty<BrokerageAccountPosition>());

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1000m, account.NetLiquidation);
                Assert.AreEqual(250m, account.TotalCashValue);
                Assert.AreEqual(250m, account.CashBalances["USD"]);
            });
        }

        [Test]
        public void AccountValueBuilderExcludesBaseCashBalanceRowsTest()
        {
            var builder =
                new InteractiveBrokersFinancialAdvisorAccountState.AccountValueBuilder(
                    "PaperA",
                    Array.Empty<string>());
            builder.Apply("AccountReady", "true", string.Empty);
            builder.Apply("NetLiquidation", "1000", "USD");
            builder.Apply("TotalCashBalance", "250", "BASE");
            builder.Apply("CashBalance", "999", "base");
            builder.Apply("$LEDGER:CashBalance", "998", "BASE");
            builder.Apply("$LEDGER-BASE:CashBalance", "997", string.Empty);
            builder.Apply("CashBalance", "250", "USD");

            var account = builder.Build(Array.Empty<BrokerageAccountPosition>());

            Assert.Multiple(() =>
            {
                Assert.AreEqual(250m, account.TotalCashValue);
                Assert.AreEqual(1, account.CashBalances.Count);
                Assert.AreEqual(250m, account.CashBalances["USD"]);
                Assert.IsFalse(account.CashBalances.ContainsKey("BASE"));
            });
        }

        [Test]
        public void AccountValueBuilderAcceptsLedgerCashAndDiscardsLedgerNonCashTest()
        {
            var builder =
                new InteractiveBrokersFinancialAdvisorAccountState.AccountValueBuilder(
                    "PaperA",
                    Array.Empty<string>());
            builder.Apply("AccountReady", "true", string.Empty);
            builder.Apply("NetLiquidation", "1000", "USD");
            builder.Apply("TotalCashValue", "250", "USD");
            builder.Apply("$LEDGER-USD:NetLiquidation", "999", string.Empty);
            builder.Apply("$LEDGER-USD:TotalCashValue", "999", string.Empty);
            builder.Apply("$LEDGER-USD:CashBalance", "250", string.Empty);

            var account = builder.Build(Array.Empty<BrokerageAccountPosition>());

            Assert.Multiple(() =>
            {
                Assert.AreEqual(1000m, account.NetLiquidation);
                Assert.AreEqual(250m, account.TotalCashValue);
                Assert.AreEqual(250m, account.CashBalances["USD"]);
            });
        }
    }
}
