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
using IBApi;

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Account-level position returned by a positions-multi request.
    /// </summary>
    internal sealed class PositionMultiEventArgs : EventArgs
    {
        /// <summary>
        /// Gets the originating request identifier.
        /// </summary>
        public int RequestId { get; }

        /// <summary>
        /// Gets the account that holds the position.
        /// </summary>
        public string Account { get; }

        /// <summary>
        /// Gets the model code used to filter the request.
        /// </summary>
        public string ModelCode { get; }

        /// <summary>
        /// Gets the Interactive Brokers contract.
        /// </summary>
        public Contract Contract { get; }

        /// <summary>
        /// Gets the exact position quantity.
        /// </summary>
        public decimal Position { get; }

        /// <summary>
        /// Gets the average cost reported by Interactive Brokers.
        /// </summary>
        public double AverageCost { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PositionMultiEventArgs"/> class.
        /// </summary>
        /// <param name="requestId">The originating request identifier.</param>
        /// <param name="account">The account that holds the position.</param>
        /// <param name="modelCode">The model code used to filter the request.</param>
        /// <param name="contract">The Interactive Brokers contract.</param>
        /// <param name="position">The exact position quantity.</param>
        /// <param name="averageCost">The average cost reported by Interactive Brokers.</param>
        public PositionMultiEventArgs(
            int requestId,
            string account,
            string modelCode,
            Contract contract,
            decimal position,
            double averageCost)
        {
            RequestId = requestId;
            Account = account;
            ModelCode = modelCode;
            Contract = contract;
            Position = position;
            AverageCost = averageCost;
        }
    }
}
