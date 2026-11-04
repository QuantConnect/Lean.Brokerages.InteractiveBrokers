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
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using IBApi;
using QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Pure Financial Advisor topology, validation, hashing, and XML transformation helpers.
    /// </summary>
    internal sealed partial class InteractiveBrokersFinancialAdvisorAccountState
    {
        private const int PercentAllocationDecimalPlaces = 10;

        internal static void ValidateGroupAssignment(
            string accountId,
            string targetGroupName,
            IReadOnlyDictionary<string, BrokerageAccountGroup> selectedGroups,
            IReadOnlyCollection<string> managedAccountIds,
            string masterAccountId,
            decimal? targetAllocationValue = null)
        {
            var managed = managedAccountIds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (!managed.Contains(accountId) ||
                accountId.Equals(masterAccountId, StringComparison.OrdinalIgnoreCase) ||
                accountId.Equals(masterAccountId + "A", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Account '{accountId}' is not a managed Financial Advisor subaccount.");
            }

            if (string.IsNullOrEmpty(targetGroupName))
            {
                if (targetAllocationValue.HasValue)
                {
                    throw new InvalidOperationException(
                        "An allocation value cannot be supplied when removing an account from every Financial Advisor group.");
                }
                return;
            }

            if (!selectedGroups.TryGetValue(targetGroupName, out var targetGroup))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{targetGroupName}' is not managed by this algorithm instance.");
            }

            var alreadyInTarget = targetGroup.AccountIds.Contains(accountId, StringComparer.OrdinalIgnoreCase);
            if (IsSupportedUserSpecifiedAllocationMethod(targetGroup.AllocationMethod))
            {
                if (!alreadyInTarget && !targetAllocationValue.HasValue)
                {
                    throw new InvalidOperationException(
                        $"Assigning an account to allocation method '{targetGroup.AllocationMethod}' requires an explicit allocation value.");
                }
                if (targetAllocationValue.HasValue)
                {
                    ValidateAllocationValue(targetGroup.AllocationMethod, targetAllocationValue.Value);
                }
                else if (!targetGroup.AccountAllocationValues.ContainsKey(accountId))
                {
                    throw new InvalidOperationException(
                        $"Financial Advisor group '{targetGroup.Name}' did not expose an allocation value for account '{accountId}'.");
                }
                return;
            }

            if (!SupportsValueFreeMembership(targetGroup.AllocationMethod))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor allocation method '{targetGroup.AllocationMethod}' is not supported for account-group assignment.");
            }
            if (targetAllocationValue.HasValue)
            {
                throw new InvalidOperationException(
                    $"Allocation method '{targetGroup.AllocationMethod}' is calculated by IB and does not accept a per-account allocation value.");
            }
        }

        internal static IReadOnlyList<string> GetAccountGroupNames(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            string accountId)
        {
            return groups.Values
                .Where(group => group.AccountIds.Contains(accountId, StringComparer.OrdinalIgnoreCase))
                .Select(group => group.Name)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        internal static void ValidateResultingAssignment(
            string accountId,
            string targetGroupName,
            IReadOnlyCollection<string> resultingGroupNames)
        {
            var valid = string.IsNullOrEmpty(targetGroupName)
                ? resultingGroupNames.Count == 0
                : resultingGroupNames.Count == 1 &&
                    resultingGroupNames.Contains(targetGroupName, StringComparer.OrdinalIgnoreCase);
            if (!valid)
            {
                throw new InvalidOperationException(
                    $"Account '{accountId}' assignment was not confirmed. Expected " +
                    (string.IsNullOrEmpty(targetGroupName) ? "no groups" : $"only group '{targetGroupName}'") +
                    $", received: {string.Join(", ", resultingGroupNames)}.");
            }
        }

        private static void ValidateManagedGroupMembers(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            IReadOnlyCollection<string> managedAccountIds,
            string primaryAccountId,
            IEnumerable<string> affectedGroupNames)
        {
            var managed = managedAccountIds
                .Where(accountId =>
                    !accountId.Equals(primaryAccountId, StringComparison.OrdinalIgnoreCase) &&
                    !accountId.Equals(primaryAccountId + "A", StringComparison.OrdinalIgnoreCase))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (var groupName in affectedGroupNames
                .Where(name => !string.IsNullOrWhiteSpace(name))
                .Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if (!groups.TryGetValue(groupName, out var group))
                {
                    continue;
                }
                var invalidAccountId = group.AccountIds.FirstOrDefault(
                    accountId => !managed.Contains(accountId));
                if (invalidAccountId != null)
                {
                    throw new InvalidOperationException(
                        $"Financial Advisor group '{group.Name}' contains account " +
                        $"'{invalidAccountId}', which is not a managed subaccount. " +
                        "The group cannot be modified safely.");
                }
            }
        }

        internal static IReadOnlyList<string> ComputeUnassignedAccountIds(
            IReadOnlyDictionary<string, BrokerageAccountGroup> allGroups,
            IReadOnlyCollection<string> managedAccountIds,
            string masterAccountId)
        {
            var assigned = allGroups.Values
                .SelectMany(group => group.AccountIds)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            return managedAccountIds
                .Where(account => !assigned.Contains(account) &&
                    !account.Equals(masterAccountId, StringComparison.OrdinalIgnoreCase) &&
                    !account.Equals(masterAccountId + "A", StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(account => account, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        internal static IReadOnlyDictionary<string, BrokerageAccountDirectoryEntry> BuildAccountDirectory(
            string primaryAccountId,
            IReadOnlyCollection<string> managedAccountIds,
            IReadOnlyDictionary<string, BrokerageAccountGroup> allGroups,
            IReadOnlyDictionary<string, string> familyCodes,
            IReadOnlyDictionary<string, BrokerageAccountState> accountStates,
            IReadOnlyDictionary<string, string> accountAliases = null)
        {
            primaryAccountId ??= string.Empty;
            managedAccountIds ??= Array.Empty<string>();
            allGroups ??= new Dictionary<string, BrokerageAccountGroup>();
            familyCodes ??= new Dictionary<string, string>();
            accountStates ??= new Dictionary<string, BrokerageAccountState>();
            accountAliases ??= new Dictionary<string, string>();

            var managed = managedAccountIds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var accountIds = managed
                .Concat(allGroups.Values.SelectMany(group => group.AccountIds))
                .Concat(familyCodes.Keys)
                .Concat(accountAliases.Keys)
                .Append(primaryAccountId)
                .Where(accountId => !string.IsNullOrWhiteSpace(accountId))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(accountId => accountId, StringComparer.OrdinalIgnoreCase);

            return accountIds.ToDictionary(
                accountId => accountId,
                accountId =>
                {
                    var relationship = accountId.Equals(primaryAccountId, StringComparison.OrdinalIgnoreCase)
                        ? BrokerageAccountRelationship.Primary
                        : accountId.Equals(primaryAccountId + "A", StringComparison.OrdinalIgnoreCase)
                            ? BrokerageAccountRelationship.Aggregate
                            : managed.Contains(accountId)
                                ? BrokerageAccountRelationship.Managed
                                : BrokerageAccountRelationship.Unknown;
                    accountStates.TryGetValue(accountId, out var accountState);
                    familyCodes.TryGetValue(accountId, out var familyCode);
                    accountAliases.TryGetValue(accountId, out var accountAlias);
                    return new BrokerageAccountDirectoryEntry(
                        accountId,
                        relationship,
                        GetAccountGroupNames(allGroups, accountId),
                        accountState?.AccountType,
                        familyCode,
                        accountAlias);
                },
                StringComparer.OrdinalIgnoreCase);
        }

        private static bool SupportsValueFreeMembership(string allocationMethod)
        {
            return allocationMethod.Equals("NetLiq", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("AvailableEquity", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("Equal", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("EqualQuantity", StringComparison.OrdinalIgnoreCase);
        }

        internal static bool IsSupportedUserSpecifiedAllocationMethod(string allocationMethod)
        {
            return allocationMethod.Equals("ContractsOrShares", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("Ratio", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase);
        }

        private static void ValidateAllocationValue(string allocationMethod, decimal allocationValue)
        {
            var contractsOrShares = allocationMethod.Equals(
                "ContractsOrShares", StringComparison.OrdinalIgnoreCase);
            if (allocationValue < 0m || (!contractsOrShares && allocationValue == 0m))
            {
                throw new InvalidOperationException(
                    contractsOrShares
                        ? "Allocation method 'ContractsOrShares' requires a non-negative per-account allocation value."
                        : $"Allocation method '{allocationMethod}' requires a positive per-account allocation value.");
            }
            if (allocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase) && allocationValue > 100m)
            {
                throw new InvalidOperationException("A Percent allocation value cannot exceed 100.");
            }
        }

        internal static void ValidateGroupAllocationUpdate(
            string groupName,
            IReadOnlyDictionary<string, decimal> accountAllocationValues,
            IReadOnlyDictionary<string, BrokerageAccountGroup> selectedGroups)
        {
            if (!selectedGroups.TryGetValue(groupName, out var group))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{groupName}' is not managed by this algorithm instance.");
            }
            if (!IsSupportedUserSpecifiedAllocationMethod(group.AllocationMethod))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor allocation method '{group.AllocationMethod}' does not accept per-account allocation values.");
            }

            var allocations = NormalizeAccountAllocationValues(accountAllocationValues);
            var members = group.AccountIds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var allocationAccounts = allocations.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (members.Count == 0 || !members.SetEquals(allocationAccounts))
            {
                var missing = members.Except(allocationAccounts, StringComparer.OrdinalIgnoreCase);
                var extra = allocationAccounts.Except(members, StringComparer.OrdinalIgnoreCase);
                throw new InvalidOperationException(
                    $"Financial Advisor group '{group.Name}' allocation keys must exactly match its existing members" +
                    $" (missing: {string.Join(", ", missing)}; extra: {string.Join(", ", extra)}).");
            }

            foreach (var value in allocations.Values)
            {
                ValidateAllocationValue(group.AllocationMethod, value);
            }
            if (group.AllocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase) &&
                allocations.Values.Sum() != 100m)
            {
                throw new InvalidOperationException(
                    $"Percent group '{group.Name}' allocation values must total 100.");
            }
        }

        internal static string UpdateAccountGroupAllocationsXml(
            string xml,
            string groupName,
            IReadOnlyDictionary<string, decimal> accountAllocationValues)
        {
            if (string.IsNullOrWhiteSpace(xml))
            {
                throw new InvalidOperationException("IB returned empty FA group configuration XML.");
            }
            if (string.IsNullOrWhiteSpace(groupName))
            {
                throw new ArgumentException("A Financial Advisor group name is required.", nameof(groupName));
            }

            groupName = groupName.Trim();
            var allocations = NormalizeAccountAllocationValues(accountAllocationValues);
            var document = XDocument.Parse(xml, LoadOptions.PreserveWhitespace);
            CanonicalizeKnownGroupAllocationMethods(document);
            var matchingGroups = document.Descendants()
                .Where(element => NameEquals(element, "Group"))
                .Where(element => string.Equals(
                    GetGroupName(element),
                    groupName,
                    StringComparison.OrdinalIgnoreCase))
                .ToArray();
            if (matchingGroups.Length != 1)
            {
                throw new InvalidOperationException(matchingGroups.Length == 0
                    ? $"Financial Advisor group '{groupName}' was not present in the configuration XML."
                    : $"Financial Advisor group '{groupName}' is duplicated in the configuration XML; allocation update is ambiguous.");
            }

            var groups = ParseGroups(xml, validateAllocationConfiguration: false);
            ValidateGroupAllocationUpdate(groupName, allocations, groups);
            var groupElement = matchingGroups[0];
            var accountContainer = groupElement.Descendants().FirstOrDefault(IsAccountContainer);
            if (accountContainer == null)
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{groupName}' did not contain an account list.");
            }

            var accountElements = accountContainer.Elements()
                .Where(IsAccountElement)
                .Where(element => !string.IsNullOrWhiteSpace(GetAccountId(element)))
                .ToArray();
            var duplicateAccount = accountElements
                .GroupBy(GetAccountId, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault(group => group.Count() > 1)?.Key;
            if (duplicateAccount != null)
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{groupName}' contains duplicate account '{duplicateAccount}'.");
            }

            foreach (var accountElement in accountElements)
            {
                SetAccountAllocationValue(accountElement, allocations[GetAccountId(accountElement)]);
            }
            return document.ToString(SaveOptions.DisableFormatting);
        }

        private static Dictionary<string, decimal> NormalizeAccountAllocationValues(
            IReadOnlyDictionary<string, decimal> accountAllocationValues)
        {
            if (accountAllocationValues == null)
            {
                throw new ArgumentNullException(nameof(accountAllocationValues));
            }

            var result = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
            foreach (var allocation in accountAllocationValues)
            {
                if (string.IsNullOrWhiteSpace(allocation.Key))
                {
                    throw new ArgumentException("Allocation account identifiers must be non-empty.", nameof(accountAllocationValues));
                }
                if (!result.TryAdd(allocation.Key.Trim(), allocation.Value))
                {
                    throw new ArgumentException(
                        $"Allocation account identifier '{allocation.Key.Trim()}' is duplicated.",
                        nameof(accountAllocationValues));
                }
            }
            return result;
        }

        internal static string UpdateAccountGroupAssignmentXml(
            string xml,
            string accountId,
            string targetGroupName,
            decimal? targetAllocationValue = null)
        {
            var document = XDocument.Parse(xml, LoadOptions.PreserveWhitespace);
            foreach (var accountElement in document.Descendants()
                .Where(IsAccountContainer)
                .SelectMany(container => container.Elements().Where(IsAccountElement)))
            {
                ValidateAccountScalarFields(accountElement);
            }
            CanonicalizeKnownGroupAllocationMethods(document);
            var groupElements = document.Descendants()
                .Where(element => NameEquals(element, "Group"))
                .Select(element => (Element: element,
                    Name: GetGroupName(element),
                    AllocationMethod: GetGroupAllocationMethod(element)))
                .Where(group => !string.IsNullOrEmpty(group.Name))
                .ToArray();
            var duplicateName = groupElements.GroupBy(group => group.Name, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault(group => group.Count() > 1)?.Key;
            if (duplicateName != null)
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{duplicateName}' is duplicated in the configuration XML; assignment is ambiguous.");
            }

            if (!string.IsNullOrEmpty(targetGroupName) &&
                !groupElements.Any(group => group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{targetGroupName}' was not present in the configuration XML.");
            }

            foreach (var group in groupElements.Where(group =>
                !group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                var accountContainer = group.Element.Descendants().FirstOrDefault(IsAccountContainer);
                if (accountContainer == null)
                {
                    continue;
                }

                var accountIds = accountContainer.Elements()
                    .Where(IsAccountElement)
                    .Select(GetAccountId)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();
                if (accountIds.Length == 1 && accountIds[0].Equals(accountId, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"Account '{accountId}' is the final account in Financial Advisor group '{group.Name}'. " +
                        "IB does not accept an empty FA group; assign another account to the group before moving or removing this account.");
                }
            }

            XElement preservedTargetAccount = null;
            XElement preservedSourceAccount = null;
            foreach (var group in groupElements)
            {
                var accountContainer = group.Element.Descendants().FirstOrDefault(IsAccountContainer);
                if (accountContainer == null)
                {
                    if (group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidOperationException(
                            $"Financial Advisor group '{targetGroupName}' did not contain an account list.");
                    }
                    continue;
                }

                var accountElements = accountContainer.Elements()
                    .Where(IsAccountElement)
                    .Where(element => !string.IsNullOrWhiteSpace(GetAccountId(element)))
                    .ToArray();
                var matches = accountElements
                    .Where(element => GetAccountId(element).Equals(accountId, StringComparison.OrdinalIgnoreCase))
                    .ToArray();
                var isTarget = group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase);
                if (isTarget && matches.Length > 0)
                {
                    preservedTargetAccount = new XElement(matches[0]);
                }
                else
                {
                    foreach (var match in matches)
                    {
                        var candidate = new XElement(match);
                        if (preservedSourceAccount != null &&
                            (HasAccountSpecificMetadata(preservedSourceAccount) ||
                             HasAccountSpecificMetadata(candidate)) &&
                            !AccountElementMetadataEquals(preservedSourceAccount, candidate))
                        {
                            throw new InvalidOperationException(
                                $"Account '{accountId}' has conflicting metadata in multiple Financial Advisor groups.");
                        }
                        if (preservedSourceAccount == null ||
                            !HasAccountSpecificMetadata(preservedSourceAccount) &&
                            HasAccountSpecificMetadata(candidate))
                        {
                            preservedSourceAccount = candidate;
                        }
                    }
                }
                foreach (var match in matches)
                {
                    match.Remove();
                }

                if (!isTarget && matches.Length > 0 &&
                    group.AllocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase))
                {
                    NormalizePercentAccountElements(accountContainer, 100m, group.Name);
                }
            }

            if (!string.IsNullOrEmpty(targetGroupName))
            {
                if (preservedTargetAccount != null &&
                    preservedSourceAccount != null &&
                    (HasAccountSpecificMetadata(preservedTargetAccount) ||
                     HasAccountSpecificMetadata(preservedSourceAccount)) &&
                    !AccountElementMetadataEquals(
                        preservedTargetAccount,
                        preservedSourceAccount))
                {
                    throw new InvalidOperationException(
                        $"Account '{accountId}' has conflicting metadata in its target and source Financial Advisor groups.");
                }

                var target = groupElements.Single(group =>
                    group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase));
                var accountContainer = target.Element.Descendants().FirstOrDefault(IsAccountContainer);
                var accountElements = accountContainer.Elements().Where(IsAccountElement).ToArray();
                var preservedAccount = preservedTargetAccount ??
                    (HasAccountSpecificMetadata(preservedSourceAccount) ? preservedSourceAccount : null);
                var accountTemplate = accountElements.FirstOrDefault();
                if (preservedAccount == null && preservedSourceAccount != null &&
                    HasAccountSpecificMetadata(accountTemplate))
                {
                    accountTemplate = preservedSourceAccount;
                }
                var userSpecified = IsSupportedUserSpecifiedAllocationMethod(target.AllocationMethod);
                decimal? allocationValue = targetAllocationValue;
                if (userSpecified && !allocationValue.HasValue && preservedTargetAccount != null)
                {
                    allocationValue = GetAccountAllocationValue(preservedTargetAccount, target.Name, accountId);
                }

                if (userSpecified)
                {
                    if (!allocationValue.HasValue)
                    {
                        throw new InvalidOperationException(
                            $"Assigning account '{accountId}' to allocation method '{target.AllocationMethod}' requires an explicit allocation value.");
                    }
                    ValidateAllocationValue(target.AllocationMethod, allocationValue.Value);
                }
                else if (targetAllocationValue.HasValue)
                {
                    throw new InvalidOperationException(
                        $"Allocation method '{target.AllocationMethod}' is calculated by IB and does not accept a per-account allocation value.");
                }

                if (target.AllocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase) &&
                    (preservedTargetAccount == null || targetAllocationValue.HasValue))
                {
                    NormalizePercentAccountElements(
                        accountContainer,
                        100m - allocationValue.Value,
                        target.Name);
                }

                accountContainer.Add(CreateAccountElement(
                    preservedAccount ?? accountTemplate ?? preservedSourceAccount,
                    accountContainer.Name.Namespace,
                    accountId,
                    allocationValue,
                    preservedAccount != null));
            }

            return document.ToString(SaveOptions.DisableFormatting);
        }

        internal static string ComputeConfigurationHash(string xml)
        {
            var document = XDocument.Parse(xml, LoadOptions.None);
            if (document.Root == null)
            {
                throw new InvalidOperationException("IB returned empty FA group configuration XML.");
            }

            CanonicalizeKnownGroupAllocationMethods(document);
            var canonical = CanonicalizeConfigurationElement(document.Root);
            return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
        }

        private static string CanonicalizeConfigurationElement(XElement element)
        {
            var attributes = element.Attributes()
                .Where(attribute => !attribute.IsNamespaceDeclaration)
                .Select(attribute =>
                    CanonicalizeSequence(
                        attribute.Name.ToString(),
                        CanonicalizeConfigurationValue(
                            attribute.Name.LocalName,
                            attribute.Value)))
                .OrderBy(value => value, StringComparer.Ordinal)
                .ToArray();
            var children = element.Elements()
                .Select(CanonicalizeConfigurationElement)
                .ToArray();
            if ((NameEquals(element, "ListOfGroups") &&
                 element.Elements().All(child => NameEquals(child, "Group"))) ||
                (IsAccountContainer(element) &&
                 element.Elements().All(IsAccountElement)))
            {
                Array.Sort(children, StringComparer.Ordinal);
            }
            var directText = element.Nodes()
                .OfType<XText>()
                .Select(text => CanonicalizeConfigurationValue(element.Name.LocalName, text.Value))
                .Where(text => text.Length != 0)
                .ToArray();
            var value = children.Length == 0
                ? CanonicalizeConfigurationValue(element.Name.LocalName, element.Value)
                : directText.Length == 0
                    ? string.Empty
                    : CanonicalizeSequence(directText);
            return CanonicalizeSequence(
                element.Name.ToString(),
                CanonicalizeSequence(attributes),
                value,
                CanonicalizeSequence(children));
        }

        private static string CanonicalizeSequence(params string[] values)
        {
            return values.Length.ToString(CultureInfo.InvariantCulture) + ":" +
                string.Concat(values.Select(value =>
                {
                    value ??= string.Empty;
                    return value.Length.ToString(CultureInfo.InvariantCulture) + ":" + value;
                }));
        }

        private static string CanonicalizeConfigurationValue(string name, string value)
        {
            value = value.Trim();
            if (name.Equals("amount", StringComparison.OrdinalIgnoreCase) &&
                decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var allocationValue))
            {
                return allocationValue.ToString("0.############################", CultureInfo.InvariantCulture);
            }
            return value;
        }

        internal static IReadOnlyDictionary<string, BrokerageAccountGroup> ParseGroups(
            string xml,
            bool validateAllocationConfiguration = true)
        {
            if (string.IsNullOrWhiteSpace(xml))
            {
                throw new InvalidOperationException("IB returned empty FA group configuration XML.");
            }

            var document = XDocument.Parse(xml, LoadOptions.None);
            if (document.Root == null || !NameEquals(document.Root, "ListOfGroups"))
            {
                throw new InvalidOperationException(
                    "IB returned an unexpected Financial Advisor group configuration document.");
            }
            CanonicalizeKnownGroupAllocationMethods(document);
            var groups = new Dictionary<string, BrokerageAccountGroup>(StringComparer.OrdinalIgnoreCase);

            foreach (var element in document.Descendants().Where(element => NameEquals(element, "Group")))
            {
                var name = GetGroupName(element);
                if (string.IsNullOrWhiteSpace(name))
                {
                    throw new InvalidOperationException(
                        "IB FA group configuration contained a group with a blank name.");
                }

                var allocationMethod = GetGroupAllocationMethod(element);
                if (string.IsNullOrWhiteSpace(allocationMethod))
                {
                    throw new UnsupportedFinancialAdvisorConfigurationException(
                        $"IB FA group '{name.Trim()}' contained no allocation method. Legacy Financial " +
                        "Advisor Profiles are not supported. In TWS, enable 'Use Account Groups with " +
                        "Allocation Methods' before using unified group discovery.");
                }
                var accountContainers = element.Descendants()
                    .Where(IsAccountContainer)
                    .ToArray();
                if (accountContainers.Length != 1)
                {
                    throw new InvalidOperationException(
                        $"IB FA group '{name.Trim()}' must contain exactly one recognized account list.");
                }
                var accountContainer = accountContainers[0];
                var unsupportedAccountElement = accountContainer.Elements()
                    .FirstOrDefault(account => !IsAccountElement(account));
                if (unsupportedAccountElement != null)
                {
                    throw new InvalidOperationException(
                        $"IB FA group '{name.Trim()}' account list contained unsupported element " +
                        $"'{unsupportedAccountElement.Name.LocalName}'.");
                }
                var accountElements = accountContainer.Elements().ToArray();
                foreach (var accountElement in accountElements)
                {
                    ValidateAccountScalarFields(accountElement);
                    if (string.IsNullOrWhiteSpace(GetAccountId(accountElement)))
                    {
                        throw new InvalidOperationException(
                            $"IB FA group '{name.Trim()}' contained an account with a blank identifier.");
                    }
                }
                var accountIds = accountElements.Select(GetAccountId).ToArray();
                var duplicateAccount = accountIds
                    .GroupBy(accountId => accountId, StringComparer.OrdinalIgnoreCase)
                    .FirstOrDefault(group => group.Count() > 1)?.Key;
                if (duplicateAccount != null)
                {
                    throw new InvalidOperationException(
                        $"IB FA group '{name.Trim()}' contains duplicate account '{duplicateAccount}'.");
                }
                var accountAllocationValues = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
                foreach (var account in accountElements)
                {
                    var amount = GetOptionalAccountAllocationValue(account);
                    if (!amount.HasValue)
                    {
                        continue;
                    }

                    var accountId = GetAccountId(account);
                    if (accountAllocationValues.TryGetValue(accountId, out var existing) && existing != amount.Value)
                    {
                        throw new InvalidOperationException(
                            $"IB FA configuration contained conflicting allocation values for account '{accountId}' in group '{name.Trim()}'.");
                    }
                    accountAllocationValues[accountId] = amount.Value;
                }

                var normalizedName = name.Trim();
                if (normalizedName.Equals("All", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        "IB FA group name 'All' is reserved for all-account API requests.");
                }
                var normalizedMethod = allocationMethod.Trim();
                var group = new BrokerageAccountGroup(
                        normalizedName,
                        normalizedMethod,
                        accountIds,
                        accountAllocationValues);
                if (validateAllocationConfiguration)
                {
                    ValidateGroupAllocationConfiguration(group);
                }
                if (!groups.TryAdd(normalizedName, group))
                {
                    throw new InvalidOperationException(
                        $"IB FA configuration contained duplicate group name '{normalizedName}'.");
                }
            }

            return groups;
        }

        private static IReadOnlyDictionary<string, BrokerageAccountGroup> CanonicalizeGroups(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            IEnumerable<string> managedAccountIds)
        {
            var managedAccounts = managedAccountIds.ToArray();
            var result = new Dictionary<string, BrokerageAccountGroup>(StringComparer.OrdinalIgnoreCase);
            foreach (var group in groups.Values)
            {
                var accountIds = group.AccountIds
                    .Select(accountId => GetCanonicalManagedAccountId(managedAccounts, accountId))
                    .ToArray();
                var allocations = new Dictionary<string, decimal>(StringComparer.Ordinal);
                foreach (var allocation in group.AccountAllocationValues)
                {
                    var accountId = GetCanonicalManagedAccountId(managedAccounts, allocation.Key);
                    if (!allocations.TryAdd(accountId, allocation.Value))
                    {
                        throw new InvalidOperationException(
                            $"IB returned ambiguous account identifier casing in group '{group.Name}'.");
                    }
                }

                result.Add(
                    group.Name,
                    new BrokerageAccountGroup(
                        group.Name,
                        group.AllocationMethod,
                        accountIds,
                        allocations));
            }
            return result;
        }

        private static void ValidateAssignmentGroups(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            string accountId,
            string targetGroupName)
        {
            foreach (var group in groups.Values.Where(group =>
                group.AccountIds.Contains(accountId, StringComparer.OrdinalIgnoreCase) ||
                group.Name.Equals(targetGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                if (group.AllocationMethod.Equals(
                        "MonetaryAmount",
                        StringComparison.OrdinalIgnoreCase))
                {
                    throw new NotSupportedException(
                        $"Financial Advisor group '{group.Name}' uses the unsupported " +
                        "MonetaryAmount allocation method.");
                }
                if (!SupportsValueFreeMembership(group.AllocationMethod) &&
                    !IsSupportedUserSpecifiedAllocationMethod(group.AllocationMethod))
                {
                    throw new NotSupportedException(
                        $"Financial Advisor group '{group.Name}' uses unsupported allocation " +
                        $"method '{group.AllocationMethod}'.");
                }
                ValidateGroupAllocationConfiguration(group);
            }
        }

        private static bool TryGetGroup(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            string groupName,
            out BrokerageAccountGroup group)
        {
            if (groups.TryGetValue(groupName, out group))
            {
                return true;
            }

            group = groups.Values.FirstOrDefault(candidate =>
                candidate.Name.Equals(groupName, StringComparison.OrdinalIgnoreCase));
            return group != null;
        }

        private static void ValidateMutationScope(
            SnapshotScope scope,
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            string accountId,
            string targetGroupName)
        {
            if (scope.CompleteDiscovery)
            {
                return;
            }

            var allowedGroups = scope.GroupNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (!string.IsNullOrEmpty(targetGroupName) &&
                !allowedGroups.Contains(targetGroupName))
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{targetGroupName}' is outside the configured " +
                    "account-state scope.");
            }
            if (accountId == null)
            {
                return;
            }

            var outsideGroups = GetAccountGroupNames(groups, accountId)
                .Where(groupName => !allowedGroups.Contains(groupName))
                .ToArray();
            if (outsideGroups.Length != 0)
            {
                throw new InvalidOperationException(
                    $"Managed account '{accountId}' belongs to Financial Advisor group(s) " +
                    $"outside the configured account-state scope: {string.Join(", ", outsideGroups)}.");
            }
        }

        private static void ValidateGroupAllocationConfiguration(BrokerageAccountGroup group)
        {
            if (!IsSupportedUserSpecifiedAllocationMethod(group.AllocationMethod))
            {
                return;
            }

            foreach (var accountId in group.AccountIds)
            {
                if (!group.AccountAllocationValues.TryGetValue(accountId, out var value))
                {
                    throw new InvalidOperationException(
                        $"IB FA group '{group.Name}' uses '{group.AllocationMethod}' but account '{accountId}' has no allocation value.");
                }
                ValidateAllocationValue(group.AllocationMethod, value);
            }
            if (group.AllocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase) &&
                group.AccountAllocationValues.Count > 0 &&
                group.AccountAllocationValues.Values.Sum() != 100m)
            {
                throw new InvalidOperationException(
                    $"IB FA Percent group '{group.Name}' allocation values must total 100.");
            }
        }

        internal static IReadOnlyDictionary<string, string> ParseAliases(string xml)
        {
            if (string.IsNullOrWhiteSpace(xml))
            {
                throw new InvalidOperationException("IB returned empty Financial Advisor account-alias XML.");
            }

            var document = XDocument.Parse(xml, LoadOptions.None);
            if (document.Root == null || !NameEquals(document.Root, "ListOfAccountAliases"))
            {
                throw new InvalidOperationException(
                    "IB returned an unexpected Financial Advisor account-alias document.");
            }
            var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var element in document.Descendants().Where(element => NameEquals(element, "AccountAlias")))
            {
                var accountId = GetConsistentScalar(
                    element,
                    new[] { "account", "accountId" },
                    new[] { "account", "accountId", "acct" },
                    "account alias account identifier",
                    StringComparer.OrdinalIgnoreCase);
                if (string.IsNullOrWhiteSpace(accountId))
                {
                    throw new InvalidOperationException("IB FA account-alias configuration contained a blank account identifier.");
                }

                accountId = accountId.Trim();
                var accountAlias = (GetConsistentScalar(
                    element,
                    new[] { "alias" },
                    new[] { "alias" },
                    "account alias",
                    StringComparer.Ordinal) ?? string.Empty).Trim();
                if (aliases.TryGetValue(accountId, out var existingAlias))
                {
                    if (!string.Equals(existingAlias, accountAlias, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            $"IB FA account-alias configuration contained conflicting aliases for account '{accountId}'.");
                    }
                    continue;
                }
                aliases.Add(accountId, accountAlias);
            }

            return aliases;
        }

        internal static IReadOnlyDictionary<string, BrokerageAccountGroup> SelectGroups(
            IReadOnlyDictionary<string, BrokerageAccountGroup> availableGroups,
            IReadOnlyCollection<string> configuredGroups)
        {
            var selected = new Dictionary<string, BrokerageAccountGroup>(StringComparer.OrdinalIgnoreCase);
            foreach (var configuredName in configuredGroups)
            {
                if (!TryGetGroup(availableGroups, configuredName, out var group))
                {
                    throw new InvalidOperationException($"Configured FA group '{configuredName}' was not returned by IB.");
                }
                selected[group.Name] = group;
            }
            return selected;
        }

        internal static string ComputeMembershipHash(
            IReadOnlyDictionary<string, BrokerageAccountGroup> groups,
            IEnumerable<string> managedAccountIds = null,
            IReadOnlyDictionary<string, string> accountAliases = null,
            IReadOnlyDictionary<string, string> familyCodes = null)
        {
            var groupCanonical = CanonicalizeSequence(groups.Values
                .OrderBy(group => group.Name, StringComparer.OrdinalIgnoreCase)
                .Select(group => CanonicalizeSequence(
                    group.Name.ToUpperInvariant(),
                    group.AllocationMethod.ToUpperInvariant(),
                    CanonicalizeSequence(group.AccountIds
                        .OrderBy(id => id, StringComparer.OrdinalIgnoreCase)
                        .Select(id => id.ToUpperInvariant())
                        .ToArray())))
                .ToArray());
            var managedCanonical = CanonicalizeSequence((managedAccountIds ?? Enumerable.Empty<string>())
                .Where(account => !string.IsNullOrWhiteSpace(account))
                .Select(account => account.Trim().ToUpperInvariant())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(account => account, StringComparer.OrdinalIgnoreCase)
                .ToArray());
            var aliasesCanonical = CanonicalizeSequence((accountAliases ?? new Dictionary<string, string>())
                .Where(pair => !string.IsNullOrWhiteSpace(pair.Key))
                .OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                .Select(pair => CanonicalizeSequence(
                    pair.Key.Trim().ToUpperInvariant(),
                    pair.Value?.Trim() ?? string.Empty))
                .ToArray());
            var familyCodesCanonical = CanonicalizeSequence(
                (familyCodes ?? new Dictionary<string, string>())
                    .Where(pair => !string.IsNullOrWhiteSpace(pair.Key))
                    .OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                    .Select(pair => CanonicalizeSequence(
                        pair.Key.Trim().ToUpperInvariant(),
                        pair.Value?.Trim() ?? string.Empty))
                    .ToArray());
            var canonical = CanonicalizeSequence(
                "GROUPS",
                groupCanonical,
                "MANAGED",
                managedCanonical,
                "ALIASES",
                aliasesCanonical,
                "FAMILY_CODES",
                familyCodesCanonical);
            return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
        }

        internal static decimal NormalizeAveragePrice(Contract contract, double averageCost)
        {
            var multiplier = 1m;
            if (!string.IsNullOrWhiteSpace(contract?.Multiplier))
            {
                if (!decimal.TryParse(
                        contract.Multiplier,
                        NumberStyles.Float,
                        CultureInfo.InvariantCulture,
                        out var parsedMultiplier) ||
                    parsedMultiplier <= 0m)
                {
                    throw new InvalidOperationException(
                        $"IB returned invalid contract multiplier '{contract.Multiplier}'.");
                }
                multiplier = parsedMultiplier;
            }
            return Convert.ToDecimal(averageCost, CultureInfo.InvariantCulture) / multiplier;
        }

        internal static BrokerageAccountUnmappedPosition CreateUnmappedPosition(
            PositionMultiEventArgs row,
            string errorMessage)
        {
            var contract = row.Contract;
            var errors = new List<string>();
            if (!string.IsNullOrWhiteSpace(errorMessage))
            {
                errors.Add(errorMessage.Trim());
            }
            var strike = TryConvertBrokerageDecimal(
                contract?.Strike ?? 0d,
                "strike",
                errors);
            var averagePrice = 0m;
            try
            {
                averagePrice = NormalizeAveragePrice(contract, row.AverageCost);
            }
            catch (Exception exception)
            {
                errors.Add($"Average cost could not be normalized: {exception.Message}");
            }
            return new BrokerageAccountUnmappedPosition(
                contract?.ConId.ToString(CultureInfo.InvariantCulture),
                contract?.Symbol,
                contract?.LocalSymbol,
                contract?.SecType,
                contract?.Currency,
                contract?.Exchange,
                contract?.PrimaryExch,
                contract?.TradingClass,
                contract?.LastTradeDateOrContractMonth,
                strike,
                contract?.Right,
                contract?.Multiplier,
                row.Position,
                averagePrice,
                row.ModelCode,
                string.Join(" ", errors),
                (contract?.Strike ?? 0d).ToString("R", CultureInfo.InvariantCulture),
                row.AverageCost.ToString("R", CultureInfo.InvariantCulture));
        }

        private static decimal TryConvertBrokerageDecimal(
            double value,
            string description,
            ICollection<string> errors)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) ||
                !decimal.TryParse(
                    value.ToString("R", CultureInfo.InvariantCulture),
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var result))
            {
                errors.Add(
                    $"Brokerage {description} '{value.ToString("R", CultureInfo.InvariantCulture)}' " +
                    "could not be represented as a decimal.");
                return 0m;
            }
            return result;
        }

        private static bool NameEquals(XElement element, string name) =>
            element.Name.LocalName.Equals(name, StringComparison.OrdinalIgnoreCase);

        private static bool IsAccountContainer(XElement element) =>
            NameEquals(element, "ListOfAccts") || NameEquals(element, "Accounts") || NameEquals(element, "ListOfAccounts");

        private static bool IsAccountElement(XElement element) =>
            NameEquals(element, "String") || NameEquals(element, "Account") || NameEquals(element, "AccountId");

        private static bool IsAccountIdentifierElement(XElement element) =>
            NameEquals(element, "String") || NameEquals(element, "acct") || NameEquals(element, "AccountId");

        private static string GetAccountId(XElement element)
        {
            if (element == null)
            {
                return string.Empty;
            }
            var values = element.Attributes()
                .Where(IsAccountIdentifierAttribute)
                .Select(attribute => attribute.Value)
                .Concat(element.Elements()
                    .Where(IsAccountIdentifierElement)
                    .Select(identifier => identifier.Value))
                .ToList();
            if ((NameEquals(element, "String") || NameEquals(element, "AccountId")) &&
                !element.HasElements)
            {
                values.Add(element.Value);
            }
            if (values.Count == 0)
            {
                values.Add(string.Concat(element.Nodes().OfType<XText>().Select(text => text.Value)));
            }
            return GetConsistentValues(
                values,
                "Financial Advisor account identifier",
                StringComparer.OrdinalIgnoreCase) ?? string.Empty;
        }

        private static decimal? GetOptionalAccountAllocationValue(XElement accountElement)
        {
            var rawValues = accountElement.Attributes()
                .Where(attribute => attribute.Name.LocalName.Equals(
                    "amount",
                    StringComparison.OrdinalIgnoreCase))
                .Select(attribute => attribute.Value)
                .Concat(accountElement.Elements()
                    .Where(element => NameEquals(element, "amount"))
                    .Select(element => element.Value))
                .ToArray();
            if (rawValues.Length == 0)
            {
                return null;
            }
            var values = new List<decimal>();
            foreach (var rawValue in rawValues)
            {
                if (!decimal.TryParse(
                        rawValue.Trim(),
                        NumberStyles.Float,
                        CultureInfo.InvariantCulture,
                        out var value))
                {
                    throw new InvalidOperationException(
                        $"IB FA configuration contained invalid allocation value '{rawValue}' " +
                        $"for account '{GetAccountId(accountElement)}'.");
                }
                values.Add(value);
            }
            if (values.Distinct().Skip(1).Any())
            {
                throw new InvalidOperationException(
                    $"IB FA configuration contained conflicting allocation values for account " +
                    $"'{GetAccountId(accountElement)}'.");
            }
            return values[0];
        }

        private static decimal GetAccountAllocationValue(XElement accountElement, string groupName, string accountId)
        {
            var value = GetOptionalAccountAllocationValue(accountElement);
            if (!value.HasValue)
            {
                throw new InvalidOperationException(
                    $"Financial Advisor group '{groupName}' did not expose an allocation value for account '{accountId}'.");
            }
            return value.Value;
        }

        private static void NormalizePercentAccountElements(
            XElement accountContainer,
            decimal targetTotal,
            string groupName)
        {
            var accounts = accountContainer.Elements()
                .Where(IsAccountElement)
                .Where(account => !string.IsNullOrWhiteSpace(GetAccountId(account)))
                .ToArray();
            if (accounts.Length == 0)
            {
                if (targetTotal != 0m && targetTotal != 100m)
                {
                    throw new InvalidOperationException(
                        $"Percent group '{groupName}' cannot allocate {100m - targetTotal} percent to its only account unless that value is 100.");
                }
                return;
            }
            if (targetTotal <= 0m)
            {
                throw new InvalidOperationException(
                    $"Percent group '{groupName}' cannot retain other members when one account is assigned 100 percent.");
            }

            var current = accounts.Select(account => new
            {
                Element = account,
                AccountId = GetAccountId(account),
                Value = GetAccountAllocationValue(account, groupName, GetAccountId(account))
            })
                .OrderBy(allocation => allocation.AccountId, StringComparer.OrdinalIgnoreCase)
                .ThenBy(allocation => allocation.AccountId, StringComparer.Ordinal)
                .ToArray();
            if (current.Any(allocation => allocation.Value <= 0m))
            {
                throw new InvalidOperationException(
                    $"Percent group '{groupName}' contains a non-positive allocation value and cannot be normalized safely.");
            }
            var currentTotal = current.Sum(allocation => allocation.Value);
            if (currentTotal <= 0m)
            {
                throw new InvalidOperationException(
                    $"Percent group '{groupName}' has no positive allocation total.");
            }

            var allocated = 0m;
            for (var index = 0; index < current.Length; index++)
            {
                var value = index == current.Length - 1
                    ? targetTotal - allocated
                    : decimal.Round(
                        current[index].Value / currentTotal * targetTotal,
                        PercentAllocationDecimalPlaces,
                        MidpointRounding.AwayFromZero);
                if (value <= 0m)
                {
                    throw new InvalidOperationException(
                        $"Percent group '{groupName}' normalization produced a non-positive allocation value.");
                }
                SetAccountAllocationValue(current[index].Element, value);
                allocated += value;
            }
        }

        private static void SetAccountAllocationValue(XElement accountElement, decimal value)
        {
            ValidateAccountScalarFields(accountElement);
            var stringValue = value.ToString(CultureInfo.InvariantCulture);
            var amountAttributes = accountElement.Attributes().Where(
                attribute => attribute.Name.LocalName.Equals(
                    "amount",
                    StringComparison.OrdinalIgnoreCase)).ToArray();
            var amounts = accountElement.Elements()
                .Where(element => NameEquals(element, "amount"))
                .ToArray();
            foreach (var amountAttribute in amountAttributes)
            {
                amountAttribute.Value = stringValue;
            }
            foreach (var amount in amounts)
            {
                amount.Value = stringValue;
            }
            if (amountAttributes.Length == 0 && amounts.Length == 0)
            {
                var amount = new XElement(accountElement.Name.Namespace + "amount");
                accountElement.Add(amount);
                amount.Value = stringValue;
            }
        }

        private static XElement CreateAccountElement(
            XElement template,
            XNamespace fallbackNamespace,
            string accountId,
            decimal? allocationValue = null,
            bool preserveTemplate = false)
        {
            if (preserveTemplate)
            {
                ValidateAccountScalarFields(template);
                var preserved = new XElement(template);
                SetAccountId(preserved, accountId);
                if (allocationValue.HasValue)
                {
                    SetAccountAllocationValue(preserved, allocationValue.Value);
                }
                else
                {
                    RemoveAccountAllocationValue(preserved);
                }
                return preserved;
            }
            ValidateAccountTemplate(template);
            if (allocationValue.HasValue)
            {
                var xmlNamespace = template?.Name.Namespace ?? fallbackNamespace;
                if (template != null && NameEquals(template, "Account"))
                {
                    var createdAccount = new XElement(template.Name);
                    var allocationIdentifierAttribute = template.Attributes().FirstOrDefault(IsAccountIdentifierAttribute);
                    if (allocationIdentifierAttribute != null)
                    {
                        createdAccount.Add(new XAttribute(allocationIdentifierAttribute.Name, accountId));
                    }
                    else
                    {
                        var templateIdentifier = template.Elements().FirstOrDefault(IsAccountIdentifierElement);
                        createdAccount.Add(new XElement(templateIdentifier?.Name ?? xmlNamespace + "acct", accountId));
                    }
                    var amountAttribute = template.Attributes().FirstOrDefault(attribute =>
                        attribute.Name.LocalName.Equals("amount", StringComparison.OrdinalIgnoreCase));
                    if (amountAttribute != null)
                    {
                        createdAccount.Add(new XAttribute(
                            amountAttribute.Name,
                            allocationValue.Value.ToString(CultureInfo.InvariantCulture)));
                    }
                    else
                    {
                        var amount = template.Elements().FirstOrDefault(element => NameEquals(element, "amount"));
                        createdAccount.Add(new XElement(
                            amount?.Name ?? xmlNamespace + "amount",
                            allocationValue.Value.ToString(CultureInfo.InvariantCulture)));
                    }
                    return createdAccount;
                }
                var accountElementName = xmlNamespace + "Account";
                var identifierName = template?.Elements().FirstOrDefault(IsAccountIdentifierElement)?.Name ??
                    xmlNamespace + "acct";
                return new XElement(accountElementName,
                    new XElement(identifierName, accountId),
                    new XElement(xmlNamespace + "amount", allocationValue.Value.ToString(CultureInfo.InvariantCulture)));
            }
            if (template == null || NameEquals(template, "String"))
            {
                return new XElement((template?.Name.Namespace ?? fallbackNamespace) + "String", accountId);
            }
            if (NameEquals(template, "AccountId"))
            {
                return new XElement(template.Name, accountId);
            }

            var accountElement = new XElement(template.Name);
            var identifierAttribute = template.Attributes().FirstOrDefault(
                IsAccountIdentifierAttribute);
            if (identifierAttribute != null)
            {
                accountElement.Add(new XAttribute(identifierAttribute.Name, accountId));
                return accountElement;
            }
            var identifier = template.Elements().FirstOrDefault(IsAccountIdentifierElement);
            if (identifier != null)
            {
                accountElement.Add(new XElement(identifier.Name, accountId));
            }
            else
            {
                accountElement.Value = accountId;
            }
            return accountElement;
        }

        private static void ValidateAccountTemplate(XElement template)
        {
            if (template == null)
            {
                return;
            }

            var unsupportedAttribute = template.Attributes().FirstOrDefault(attribute =>
                !attribute.IsNamespaceDeclaration &&
                !IsAccountIdentifierAttribute(attribute) &&
                !attribute.Name.LocalName.Equals("amount", StringComparison.OrdinalIgnoreCase));
            var unsupportedElement = template.Elements().FirstOrDefault(element =>
                !IsAccountIdentifierElement(element) &&
                !NameEquals(element, "amount"));
            if (unsupportedAttribute != null || unsupportedElement != null ||
                !ValidateAccountScalarFields(template, throwOnFailure: false))
            {
                throw new InvalidOperationException(
                    $"The Financial Advisor account-list template contains unsupported metadata and cannot be copied safely.");
            }
        }

        private static bool ValidateAccountScalarFields(
            XElement accountElement,
            bool throwOnFailure = true)
        {
            var hasStructuredIdentifier = NameEquals(accountElement, "Account") &&
                (accountElement.Attributes().Any(IsAccountIdentifierAttribute) ||
                 accountElement.Elements().Any(IsAccountIdentifierElement));
            var hasDirectTextIdentifier = accountElement.Nodes()
                .OfType<XText>()
                .Any(text => !string.IsNullOrWhiteSpace(text.Value));
            if (hasStructuredIdentifier && hasDirectTextIdentifier)
            {
                if (throwOnFailure)
                {
                    throw new InvalidOperationException(
                        "The Financial Advisor account element combines a structured account identifier with direct text.");
                }
                return false;
            }
            var unsupportedField =
                (NameEquals(accountElement, "String") || NameEquals(accountElement, "AccountId")) &&
                accountElement.HasElements
                    ? accountElement
                    : accountElement.Elements()
                .Where(element => IsAccountIdentifierElement(element) || NameEquals(element, "amount"))
                .FirstOrDefault(element => element.HasElements ||
                    element.Attributes().Any(attribute => !attribute.IsNamespaceDeclaration));
            if (unsupportedField == null)
            {
                return true;
            }
            if (throwOnFailure)
            {
                throw new InvalidOperationException(
                    $"The Financial Advisor account element contains unsupported metadata inside '{unsupportedField.Name.LocalName}'.");
            }
            return false;
        }

        private static void SetAccountId(XElement accountElement, string accountId)
        {
            ValidateAccountScalarFields(accountElement);
            if (NameEquals(accountElement, "String") || NameEquals(accountElement, "AccountId"))
            {
                accountElement.Value = accountId;
                return;
            }

            var identifierAttributes = accountElement.Attributes()
                .Where(IsAccountIdentifierAttribute)
                .ToArray();
            var identifiers = accountElement.Elements()
                .Where(IsAccountIdentifierElement)
                .ToArray();
            foreach (var identifierAttribute in identifierAttributes)
            {
                identifierAttribute.Value = accountId;
            }
            foreach (var identifier in identifiers)
            {
                identifier.Value = accountId;
            }
            if (identifierAttributes.Length != 0 || identifiers.Length != 0)
            {
                return;
            }

            var directTextIdentifiers = accountElement.Nodes()
                .OfType<XText>()
                .Where(text => !string.IsNullOrWhiteSpace(text.Value))
                .ToArray();
            if (NameEquals(accountElement, "Account") && directTextIdentifiers.Length != 0)
            {
                directTextIdentifiers[0].Value = accountId;
                foreach (var duplicate in directTextIdentifiers.Skip(1))
                {
                    duplicate.Remove();
                }
                return;
            }
            throw new InvalidOperationException(
                "The Financial Advisor account element did not contain a recognized account identifier.");
        }

        private static void RemoveAccountAllocationValue(XElement accountElement)
        {
            ValidateAccountScalarFields(accountElement);
            accountElement.Attributes()
                .Where(attribute => attribute.Name.LocalName.Equals("amount", StringComparison.OrdinalIgnoreCase))
                .Remove();
            accountElement.Elements()
                .Where(element => NameEquals(element, "amount"))
                .Remove();
        }

        private static bool AccountElementMetadataEquals(XElement first, XElement second)
        {
            return GetAccountSpecificMetadata(first) == GetAccountSpecificMetadata(second);
        }

        private static bool HasAccountSpecificMetadata(XElement accountElement)
        {
            return accountElement != null && GetAccountSpecificMetadata(accountElement).Length != 0;
        }

        private static string GetAccountSpecificMetadata(XElement accountElement)
        {
            if (accountElement == null)
            {
                return string.Empty;
            }

            var attributes = accountElement.Attributes()
                .Where(attribute => !attribute.IsNamespaceDeclaration &&
                    !IsAccountIdentifierAttribute(attribute) &&
                    !attribute.Name.LocalName.Equals("amount", StringComparison.OrdinalIgnoreCase))
                .OrderBy(attribute => attribute.Name.ToString(), StringComparer.Ordinal)
                .Select(attribute => CanonicalizeSequence(
                    "ATTRIBUTE",
                    attribute.Name.ToString(),
                    attribute.Value));
            var elements = accountElement.Elements()
                .Where(element => !IsAccountIdentifierElement(element) && !NameEquals(element, "amount"))
                .OrderBy(element => element.Name.ToString(), StringComparer.Ordinal)
                .Select(element => CanonicalizeSequence(
                    "ELEMENT",
                    element.ToString(SaveOptions.DisableFormatting)));
            var metadata = attributes.Concat(elements).ToArray();
            return metadata.Length == 0
                ? string.Empty
                : CanonicalizeSequence(metadata);
        }

        private static string[] NormalizeAccountIds(IEnumerable<string> accountIds)
        {
            var requested = accountIds.ToArray();
            var result = requested
                .Where(account => !string.IsNullOrWhiteSpace(account))
                .Select(account => account.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(account => account, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            if (result.Length != requested.Length)
            {
                throw new ArgumentException("Account identifiers must be non-empty and unique.");
            }
            return result;
        }

        internal static string GetCanonicalManagedAccountId(
            IEnumerable<string> managedAccountIds,
            string accountId)
        {
            var matches = managedAccountIds
                .Where(candidate => candidate.Equals(accountId, StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            if (matches.Length > 1)
            {
                throw new InvalidOperationException(
                    $"Managed account identifier '{accountId}' is ambiguous because IB returned multiple case variants.");
            }
            return matches.FirstOrDefault() ?? accountId;
        }

        private static string GetCanonicalPrimaryAccountId(
            IEnumerable<string> managedAccountIds,
            string primaryAccountId)
        {
            if (string.IsNullOrWhiteSpace(primaryAccountId))
            {
                throw new InvalidOperationException(
                    "The configured Interactive Brokers primary account identifier is required.");
            }

            var matches = managedAccountIds
                .Where(candidate => candidate.Equals(
                    primaryAccountId,
                    StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            if (matches.Length != 1)
            {
                throw new InvalidOperationException(
                    $"The configured Interactive Brokers primary account '{primaryAccountId}' " +
                    "was not returned exactly once by managed-account discovery.");
            }
            return matches[0];
        }

        private static string GetGroupName(XElement element)
        {
            return GetConsistentScalar(
                element,
                new[] { "name" },
                new[] { "name", "GroupName" },
                "Financial Advisor group name",
                StringComparer.OrdinalIgnoreCase) ?? string.Empty;
        }

        private static string GetGroupAllocationMethod(XElement element)
        {
            return NormalizeGroupAllocationMethod(GetConsistentScalar(
                element,
                new[] { "defaultMethod" },
                new[] { "defaultMethod", "method", "AllocationMethod" },
                "Financial Advisor group allocation method",
                StringComparer.OrdinalIgnoreCase) ?? string.Empty);
        }

        private static void CanonicalizeKnownGroupAllocationMethods(XDocument document)
        {
            foreach (var group in document.Descendants().Where(element => NameEquals(element, "Group")))
            {
                foreach (var attribute in group.Attributes().Where(attribute =>
                    attribute.Name.LocalName.Equals(
                        "defaultMethod",
                        StringComparison.OrdinalIgnoreCase)))
                {
                    attribute.Value = NormalizeGroupAllocationMethod(attribute.Value);
                }
                foreach (var element in group.Elements().Where(element =>
                    NameEquals(element, "defaultMethod") ||
                    NameEquals(element, "method") ||
                    NameEquals(element, "AllocationMethod")))
                {
                    element.Value = NormalizeGroupAllocationMethod(element.Value);
                }
            }
        }

        private static string NormalizeGroupAllocationMethod(string allocationMethod)
        {
            allocationMethod = allocationMethod?.Trim() ?? string.Empty;
            if (allocationMethod.Equals("EqualQuantity", StringComparison.OrdinalIgnoreCase) ||
                allocationMethod.Equals("Equal", StringComparison.OrdinalIgnoreCase))
            {
                return "Equal";
            }
            if (allocationMethod.Equals("NetLiq", StringComparison.OrdinalIgnoreCase))
            {
                return "NetLiq";
            }
            if (allocationMethod.Equals("AvailableEquity", StringComparison.OrdinalIgnoreCase))
            {
                return "AvailableEquity";
            }
            if (allocationMethod.Equals("PctChange", StringComparison.OrdinalIgnoreCase))
            {
                return "PctChange";
            }
            if (allocationMethod.Equals("ContractsOrShares", StringComparison.OrdinalIgnoreCase))
            {
                return "ContractsOrShares";
            }
            if (allocationMethod.Equals("Ratio", StringComparison.OrdinalIgnoreCase))
            {
                return "Ratio";
            }
            if (allocationMethod.Equals("Percent", StringComparison.OrdinalIgnoreCase))
            {
                return "Percent";
            }
            return allocationMethod;
        }

        private static string GetConsistentScalar(
            XElement element,
            IReadOnlyCollection<string> attributeNames,
            IReadOnlyCollection<string> childNames,
            string description,
            StringComparer comparer)
        {
            return GetConsistentValues(
                element.Attributes()
                    .Where(attribute => attributeNames.Any(name =>
                        attribute.Name.LocalName.Equals(name, StringComparison.OrdinalIgnoreCase)))
                    .Select(attribute => attribute.Value)
                    .Concat(element.Elements()
                        .Where(child => childNames.Any(name => NameEquals(child, name)))
                        .Select(child => child.Value)),
                description,
                comparer);
        }

        private static string GetConsistentValues(
            IEnumerable<string> values,
            string description,
            StringComparer comparer)
        {
            var normalized = values
                .Select(value => value?.Trim() ?? string.Empty)
                .Distinct(comparer)
                .ToArray();
            if (normalized.Length > 1)
            {
                throw new InvalidOperationException(
                    $"IB FA configuration contained conflicting representations of the {description}.");
            }
            return normalized.FirstOrDefault();
        }

        private static bool IsAccountIdentifierAttribute(XAttribute attribute) =>
            attribute.Name.LocalName.Equals("acct", StringComparison.OrdinalIgnoreCase) ||
            attribute.Name.LocalName.Equals("account", StringComparison.OrdinalIgnoreCase) ||
            attribute.Name.LocalName.Equals("accountId", StringComparison.OrdinalIgnoreCase);

        private static IReadOnlyCollection<string> ParseManagedAccounts(string accountList)
        {
            var accounts = (accountList ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToArray();
            var ambiguousAccount = accounts
                .GroupBy(account => account, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault(group => group.Distinct(StringComparer.Ordinal).Skip(1).Any());
            if (ambiguousAccount != null)
            {
                throw new InvalidOperationException(
                    $"IB returned ambiguous managed account identifier casing for '{ambiguousAccount.Key}'.");
            }
            return accounts.Distinct(StringComparer.Ordinal).ToArray();
        }

        private static IReadOnlyDictionary<string, string> ToFamilyCodeDictionary(IEnumerable<FamilyCode> familyCodes)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var familyCode in (familyCodes ?? Enumerable.Empty<FamilyCode>())
                .Where(familyCode => !string.IsNullOrWhiteSpace(familyCode?.AccountID)))
            {
                var accountId = familyCode.AccountID.Trim();
                var value = familyCode.FamilyCodeStr?.Trim() ?? string.Empty;
                if (!result.TryGetValue(accountId, out var existing))
                {
                    result.Add(accountId, value);
                    continue;
                }
                if (!string.IsNullOrEmpty(existing) &&
                    !string.IsNullOrEmpty(value) &&
                    !string.Equals(existing, value, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"IB returned conflicting family codes for account '{accountId}'.");
                }
                if (string.IsNullOrEmpty(existing))
                {
                    result[accountId] = value;
                }
            }
            return result;
        }

        private static bool IsPrimaryOrAggregateAccount(string accountId, string primaryAccountId)
        {
            return !string.IsNullOrEmpty(primaryAccountId) &&
                (accountId.Equals(primaryAccountId, StringComparison.OrdinalIgnoreCase) ||
                    accountId.Equals(primaryAccountId + "A", StringComparison.OrdinalIgnoreCase));
        }

        internal sealed class UnsupportedFinancialAdvisorConfigurationException
            : InvalidOperationException
        {
            public UnsupportedFinancialAdvisorConfigurationException(string message)
                : base(message)
            {
            }
        }
    }
}
