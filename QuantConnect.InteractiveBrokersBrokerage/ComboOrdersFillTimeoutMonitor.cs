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
using QuantConnect.Configuration;
using QuantConnect.Logging;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;

[assembly: InternalsVisibleTo("QuantConnect.InteractiveBrokersBrokerage.Tests")]
namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Monitors combo orders fill events coming from IB in order to timeout waiting for every order in the group for emitting events back to Lean
    /// </summary>
    internal class ComboOrdersFillTimeoutMonitor
    {
        private Thread _thread;

        private readonly ConcurrentQueue<PendingFillingEventDetails> _pendingFillingEventDetails = new();

        private readonly ManualResetEvent _pendingFillingEventDetailsEvent = new(false);

        private CancellationTokenSource _cancellationTokenSource;

        public void Run(Action<Order, IB.ExecutionDetailsEventArgs, CommissionReport, bool> callback, CancellationTokenSource cancellationTokenSource)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _thread = new Thread(() =>
            {
                Log.Trace("ComboOrdersFillTimeoutMonitor.Run(): starting...");
                try
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        if (!TryGetNextPendingFill(out var details))
                        {
                            if (WaitHandle.WaitAny(new[] { _pendingFillingEventDetailsEvent, _cancellationTokenSource.Token.WaitHandle }) != 0)
                            {
                                break;
                            }

                            _pendingFillingEventDetailsEvent.Reset();
                            continue;
                        }

                        var wait = details.UtcTime + _comboOrderFillTimeout - DateTime.UtcNow;
                        if (!_cancellationTokenSource.IsCancellationRequested &&
                            wait > TimeSpan.Zero &&
                            _cancellationTokenSource.Token.WaitHandle.WaitOne(wait))
                        {
                            // cancel signal
                            break;
                        }

                        callback(details.Order, details.ExecutionDetails, details.CommissionReport, true);
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

                Log.Trace("ComboOrdersFillTimeoutMonitor.Run(): ended");
            })
            {
                IsBackground = true,
                Name = "ComboOrdersFillTimeoutMonitorThread"
            };

            _thread.Start();
        }

        public void Stop()
        {
            _thread?.StopSafely(TimeSpan.FromSeconds(10), _cancellationTokenSource);
        }

        public void AddPendingFill(PendingFillingEventDetails details)
        {
            _pendingFillingEventDetails.Enqueue(details);
            _pendingFillingEventDetailsEvent.Set();
        }

        private bool TryGetNextPendingFill(out PendingFillingEventDetails details)
        {
            if (!_pendingFillingEventDetails.TryDequeue(out details))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// The maximum time to wait for all fills in a combo order before we start emitting the fill events
        /// </summary>
        private static readonly TimeSpan _comboOrderFillTimeout = TimeSpan.FromSeconds(Config.GetInt("ib-combo-order-fill-timeout", 30));
    }

    internal class PendingFillingEventDetails
    {
        public Order Order { get; set; }
        public IB.ExecutionDetailsEventArgs ExecutionDetails { get; set; }
        public CommissionReport CommissionReport { get; set; }
        public DateTime UtcTime { get; set; }
    }
}
