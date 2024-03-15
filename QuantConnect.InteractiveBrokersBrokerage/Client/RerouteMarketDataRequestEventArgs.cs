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

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Event arguments class for the <see cref="InteractiveBrokersClient.ReRouteMarketDataRequest"/>
    /// and <see cref="InteractiveBrokersClient.ReRouteMarketDataDepthRequest"/> events.
    /// </summary>
    public class RerouteMarketDataRequestEventArgs : EventArgs
    {
        /// <summary>
        /// The request ID used to track data
        /// </summary>
        public int RequestId { get; }

        /// <summary>
        /// The underlying contract ID for the new request
        /// </summary>
        public int ContractId { get; }

        /// <summary>
        /// The Underlying's primary exchange
        /// </summary>
        public string UnderlyingPrimaryExchange { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="RerouteMarketDataRequestEventArgs"/> class
        /// </summary>
        public RerouteMarketDataRequestEventArgs(int requestId, int contractId, string underlyingPrimaryExchange)
        {
            RequestId = requestId;
            ContractId = contractId;
            UnderlyingPrimaryExchange = underlyingPrimaryExchange;
        }
    }
}