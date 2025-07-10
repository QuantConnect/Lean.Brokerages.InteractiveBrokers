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

namespace QuantConnect.Brokerages.InteractiveBrokers.Client;

/// <summary>
/// Provides data for an account update event received through the Financial Advisor (FA) group API.
/// Contains account value or position information specific to a request ID, account, model code, key, value, and currency.
/// </summary>
public sealed class AccountUpdateMultiEventArgs : EventArgs
{
    /// <summary>
    /// The id of the request.
    /// </summary>
    public int RequestId { get; }

    /// <summary>
    /// The account with updates.
    /// </summary>
    public string Account { get; }

    /// <summary>
    /// The model code with updates.
    /// </summary>
    public string ModelCode { get; }

    /// <summary>
    /// The name of the parameter.
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// The value of the parameter.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// The currency of the parameter.
    /// </summary>
    public string Currency { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccountUpdateMultiEventArgs"/> class.
    /// </summary>
    /// <param name="requestId">The ID of the originating request.</param>
    /// <param name="account">The account associated with the update.</param>
    /// <param name="modelCode">The model code used for filtering the update.</param>
    /// <param name="key">The name of the updated parameter.</param>
    /// <param name="value">The value of the updated parameter.</param>
    /// <param name="currency">The currency associated with the parameter value.</param>
    public AccountUpdateMultiEventArgs(int requestId, string account, string modelCode, string key, string value, string currency)
    {
        RequestId = requestId;
        Account = account;
        ModelCode = modelCode;
        Key = key;
        Value = value;
        Currency = currency;
    }

    /// <summary>
    /// Returns a string that represents the current <see cref="AccountUpdateMultiEventArgs"/> instance.
    /// </summary>
    /// <returns>
    /// A string containing the request ID, account, model code, key, value, and currency information.
    /// </returns>
    public override string ToString()
    {
        return $"RequestId: {RequestId}, Account: {Account}, ModelCode: {ModelCode}, Key: {Key}, Value: {Value}, Currency: {Currency}";
    }
}
