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

using IBApi;
using System;
using System.Threading;
using QuantConnect.Logging;
using System.Collections.Concurrent;
using Order = QuantConnect.Orders.Order;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Monitors combo orders fill events coming from IB in order to timeout waiting for every order in the group for emitting events back to Lean
    /// </summary>
    internal class ComboOrdersFillTimeoutMonitor
    {
        private Thread _thread;
        private readonly ITimeProvider _timeProvider;
        private readonly TimeSpan _comboOrderFillTimeout;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ManualResetEvent _pendingFillingEventDetailsEvent;
        private readonly ConcurrentQueue<PendingFillEvent> _pendingFillingEventDetails;

        /// <summary>
        /// Event fired when there a <see cref="PendingFillEvent"/> timed out waiting for the other order events of the combo
        /// </summary>
        public event EventHandler<PendingFillEvent> TimeoutEvent;

        /// <summary>
        /// Creates a new instance
        /// </summary>
        public ComboOrdersFillTimeoutMonitor(ITimeProvider timeProvider, TimeSpan comboOrderFillTimeout)
        {
            _timeProvider = timeProvider;
            _cancellationTokenSource = new();
            _pendingFillingEventDetails = new();
            _pendingFillingEventDetailsEvent = new(false);
            _comboOrderFillTimeout = comboOrderFillTimeout;
        }

        /// <summary>
        /// Starts the time monitor
        /// </summary>
        public void Start()
        {
            _thread = new Thread(() =>
            {
                var endOfThreadLog = () =>
                {
                    Log.Trace("ComboOrdersFillTimeoutMonitor.Start(): ended");
                };

                Log.Trace("ComboOrdersFillTimeoutMonitor.Start(): starting...");
                try
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        if (!_pendingFillingEventDetails.TryDequeue(out var details))
                        {
                            if (WaitHandle.WaitAny(new[] { _pendingFillingEventDetailsEvent, _cancellationTokenSource.Token.WaitHandle }) != 0)
                            {
                                // cancel signal
                                break;
                            }

                            _pendingFillingEventDetailsEvent.Reset();
                            continue;
                        }

                        var wait = details.ExpirationTime - _timeProvider.GetUtcNow();
                        while (!_cancellationTokenSource.IsCancellationRequested && wait > TimeSpan.Zero)
                        {
                            if (_cancellationTokenSource.Token.WaitHandle.WaitOne(wait))
                            {
                                // cancel signal
                                endOfThreadLog();
                                return;
                            }
                            wait = details.ExpirationTime - _timeProvider.GetUtcNow();
                        }

                        TimeoutEvent?.Invoke(this, details);
                    }
                }
                catch (ObjectDisposedException)
                {
                    // expected
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception e)
                {
                    Log.Error(e, "ComboOrdersFillTimeoutMonitor");
                }

                endOfThreadLog();
            })
            {
                IsBackground = true,
                Name = "ComboOrdersFillTimeoutMonitor"
            };

            _thread.Start();
        }

        /// <summary>
        /// Stops the time monitor
        /// </summary>
        public void Stop()
        {
            _thread?.StopSafely(TimeSpan.FromSeconds(10), _cancellationTokenSource);
        }

        /// <summary>
        /// Add a new pending fill
        /// </summary>
        public void AddPendingFill(Order order, IB.ExecutionDetailsEventArgs executionDetails, CommissionAndFeesReport commissionReport)
        {
            _pendingFillingEventDetails.Enqueue(new PendingFillEvent
            {
                Order = order,
                ExecutionDetails = executionDetails,
                CommissionReport = commissionReport,
                ExpirationTime = _timeProvider.GetUtcNow() + _comboOrderFillTimeout
            });

            _pendingFillingEventDetailsEvent.Set();
        }
    }

    internal class PendingFillEvent
    {
        public Order Order { get; set; }
        public IB.ExecutionDetailsEventArgs ExecutionDetails { get; set; }
        public CommissionAndFeesReport CommissionReport { get; set; }
        public DateTime ExpirationTime { get; set; }
    }
}
