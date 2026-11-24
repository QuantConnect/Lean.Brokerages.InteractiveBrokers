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
using System.Threading;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Holds the brokerage state information (connection status, error conditions, etc.)
    /// </summary>
    public class InteractiveBrokersStateManager
    {
        private volatile bool _disconnected1100Fired;
        private volatile bool _previouslyInResetTime;
        // ticks rather than a DateTime? so the poll's timer thread and HandleError read and write it atomically, 0 for none
        private long _resetPeriodEndedUtcTicks;
        private readonly ManualResetEvent _connectingInProgress = new ManualResetEvent(false);

        /// <summary>
        /// True if we are currently connecting to the IB API
        /// </summary>
        public bool IsConnecting
        {
            get
            {
                return _connectingInProgress.WaitOne(0);
            }
            set
            {
                if (value)
                {
                    _connectingInProgress.Set();
                }
                else
                {
                    _connectingInProgress.Reset();
                }
            }
        }

        /// <summary>
        /// Gets/sets whether the IB client has received a Disconnect (1100) message
        /// </summary>
        public bool Disconnected1100Fired
        {
            get
            {
                return _disconnected1100Fired;
            }

            set
            {
                _disconnected1100Fired = value;
            }
        }

        /// <summary>
        /// Gets/sets whether the previous reconnection attempt was performed during the IB reset period
        /// </summary>
        public bool PreviouslyInResetTime
        {
            get
            {
                return _previouslyInResetTime;
            }

            set
            {
                _previouslyInResetTime = value;
            }
        }

        /// <summary>
        /// Gets/sets when the reconnect polling first found the IB reset period over, null while it is open or no disconnect is pending
        /// </summary>
        public DateTime? ResetPeriodEndedUtc
        {
            get
            {
                var ticks = Interlocked.Read(ref _resetPeriodEndedUtcTicks);
                return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
            }

            set
            {
                Interlocked.Exchange(ref _resetPeriodEndedUtcTicks, value?.Ticks ?? 0);
            }
        }

        /// <summary>
        /// Resets the state to the default values
        /// </summary>
        public void Reset()
        {
            _disconnected1100Fired = false;
            _previouslyInResetTime = false;
            ResetPeriodEndedUtc = null;
            _connectingInProgress.Reset();
        }
    }
}
