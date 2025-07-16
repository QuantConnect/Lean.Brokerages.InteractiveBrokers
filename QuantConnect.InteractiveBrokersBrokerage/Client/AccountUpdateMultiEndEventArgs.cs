﻿/*
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
/// Provides data for the event that signals the end of an account update request
/// made using the Financial Advisor (FA) group API.
/// </summary>
public sealed class AccountUpdateMultiEndEventArgs : EventArgs
{
    /// <summary>
    /// The request ID.
    /// </summary>
    public int RequestId { get; }


    /// <summary>
    /// Initializes a new instance of the <see cref="AccountUpdateMultiEndEventArgs"/> class.
    /// </summary>
    /// <param name="requestId">The ID of the request that has completed.</param>
    public AccountUpdateMultiEndEventArgs(int requestId)
    {
        RequestId = requestId;
    }
}
