/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2026 QuantConnect Corporation.
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
    /// Handles competing live session market data errors with throttled notifications.
    /// </summary>
    public class CompetingLiveSessionMarketDataErrorHandler
    {
        /// <summary>
        /// IB error code indicating that market data is unavailable due to a competing live session.
        /// </summary>
        /// <remarks>
        /// Example IB event error:
        /// RequestId: 5 ErrorCode: 10197 - No market data during competing live session. Origin: [Id=5] Subscribe: SPY (STK SPY USD Smart ARCA  0 )
        /// </remarks>
        public const int ErrorCode = 10197;

        /// <summary>
        /// Minimum time between emitted messages.
        /// </summary>
        private static readonly TimeSpan ThrottleInterval = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Next UTC time when execution is allowed.
        /// </summary>
        private DateTime _nextAllowedExecutionUtc;

        /// <summary>
        /// Message publishing callback.
        /// </summary>
        private readonly Action<BrokerageMessageEvent> _onMessage;

        /// <summary>
        /// Initializes a new instance of the <see cref="CompetingLiveSessionMarketDataErrorHandler"/> class.
        /// </summary>
        /// <param name="onMessage">
        /// Callback invoked when an error message needs to be published.
        /// </param>
        public CompetingLiveSessionMarketDataErrorHandler(Action<BrokerageMessageEvent> onMessage)
        {
            _onMessage = onMessage;
        }

        /// <summary>
        /// Handles an error if throttling allows.
        /// </summary>
        public void Handle(DateTime utcNow, int code, string message)
        {
            if (utcNow < _nextAllowedExecutionUtc)
            {
                return;
            }

            _nextAllowedExecutionUtc = utcNow.Add(ThrottleInterval);
            _onMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, code, message));
        }
    }
}
