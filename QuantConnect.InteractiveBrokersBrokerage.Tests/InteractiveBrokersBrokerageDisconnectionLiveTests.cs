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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Logging;
using QuantConnect.Tests.Engine.DataFeeds;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    [Explicit("Requires a configured IB Gateway. Kills it and waits for the heart beat to notice, ~15 minutes.")]
    public class InteractiveBrokersBrokerageDisconnectionLiveTests
    {
        // Reproduces the start of the outage in issue #246: the gateway process dies, so the API
        // socket is reset and no IB error 1100 is ever delivered. That leaves the heart beat as the
        // only thing that can notice, and it used to report a healthy beat whenever it was not
        // connected, so nothing was ever reported and the algorithm kept trading blind. A Disconnect
        // must reach the message handler, which is what arms its countdown to stop the algorithm.
        [Test]
        public void ReportsTheDisconnectionWhenTheGatewayDiesAndDoesNotComeBack()
        {
            Log.LogHandler = new NUnitLogHandler();

            var algorithm = new AlgorithmStub();
            var brokerage = new InteractiveBrokersBrokerage(algorithm, new OrderProvider(), algorithm.Portfolio);
            brokerage.Connect();

            Assert.IsTrue(brokerage.IsConnected, "the gateway has to be up before the test can take it down");

            if (IsWithinAWindowWhereBeingDownIsExpected(brokerage, out var reason))
            {
                Assert.Ignore($"a disconnection would be expected right now ({reason}), run this earlier in the day");
            }

            using var disconnected = new ManualResetEventSlim(false);
            brokerage.Message += (_, message) =>
            {
                if (message.Type == BrokerageMessageType.Disconnect)
                {
                    disconnected.Set();
                }
            };

            // every kill fires an exit event which refreshes the recovery grace period, so with the
            // production 30 minutes the report would only come long after the restart attempts stop.
            // Shrink it so the test completes in minutes; the unit tests cover the boundary itself.
            var gracePeriodField = typeof(InteractiveBrokersBrokerage)
                .GetField("_gatewayRecoveryGracePeriod", BindingFlags.NonPublic | BindingFlags.Static);
            var originalGracePeriod = (TimeSpan)gracePeriodField.GetValue(null);
            gracePeriodField.SetValue(null, TimeSpan.FromMinutes(3));

            // Killing it once is not enough: the exit schedules a restart a few minutes later and a
            // gateway that comes back would reconnect before the heart beat ever reported anything.
            // Holding it down is what reproduces the outage.
            var killer = new GatewayKiller(brokerage);
            try
            {
                Assert.IsTrue(disconnected.Wait(TimeSpan.FromMinutes(20)), "the lost connection was never reported");
                Assert.IsFalse(brokerage.IsConnected);
            }
            finally
            {
                gracePeriodField.SetValue(null, originalGracePeriod);
                // Disposing stops the gateway, which needs the IBAutomater lock, and IBAutomater
                // holds that lock while relaunching the gateway we keep killing. Killing is left
                // running so those relaunches fail fast, and the bound means a teardown that gets
                // stuck anyway reports instead of hanging the run.
                DisposeWithin(brokerage, TimeSpan.FromMinutes(2));
                killer.Dispose();
            }
        }

        /// <summary>
        /// Disposes within the given time, reporting rather than blocking the run forever.
        /// </summary>
        private static void DisposeWithin(IDisposable disposable, TimeSpan timeout)
        {
            var disposal = Task.Run(() =>
            {
                try
                {
                    disposable.Dispose();
                }
                catch (Exception exception)
                {
                    Log.Error(exception, "DisposeWithin()");
                }
            });

            if (!disposal.Wait(timeout))
            {
                Log.Error($"DisposeWithin(): timed out after {timeout} disposing the brokerage, " +
                    "an IB Gateway process may still be running.");
            }
        }

        /// <summary>
        /// The wall clock driven reasons the heart beat has to stay quiet, which cannot be injected.
        /// </summary>
        private static bool IsWithinAWindowWhereBeingDownIsExpected(InteractiveBrokersBrokerage brokerage, out string reason)
        {
            var automater = (QuantConnect.IBAutomater.IBAutomater)typeof(InteractiveBrokersBrokerage)
                .GetField("_ibAutomater", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(brokerage);
            var heartBeatTimeLimit = (TimeSpan)typeof(InteractiveBrokersBrokerage)
                .GetField("_heartBeatTimeLimit", BindingFlags.NonPublic | BindingFlags.Static)
                .GetValue(null);

            reason = null;
            if (automater.IsWithinScheduledServerResetTimes())
            {
                reason = "within the IB scheduled server reset times";
            }
            else if (DateTime.Now.TimeOfDay >= heartBeatTimeLimit)
            {
                reason = "close to the gateway daily restart";
            }

            return reason != null;
        }

        /// <summary>
        /// Kills the IB Gateway process, and any replacement IBAutomater starts, until disposed.
        /// Reaches for IBAutomater's own process handle so nothing else can be matched by mistake.
        /// </summary>
        private sealed class GatewayKiller : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new();
            private readonly Task _task;

            public GatewayKiller(InteractiveBrokersBrokerage brokerage)
            {
                var automater = typeof(InteractiveBrokersBrokerage)
                    .GetField("_ibAutomater", BindingFlags.NonPublic | BindingFlags.Instance)
                    .GetValue(brokerage);
                var processField = automater.GetType().GetField("_process", BindingFlags.NonPublic | BindingFlags.Instance);

                _task = Task.Run(() =>
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        try
                        {
                            if (processField.GetValue(automater) is Process process && !process.HasExited)
                            {
                                Log.Trace($"GatewayKiller: killing IBGateway process {process.Id}");
                                process.Kill();
                            }
                        }
                        catch (Exception exception)
                        {
                            // the process can exit between the check and the kill, and the handle can
                            // be swapped by a restart, neither is a problem: the next pass picks it up
                            Log.Trace($"GatewayKiller: {exception.Message}");
                        }

                        _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
                    }
                });
            }

            public void Dispose()
            {
                _cancellationTokenSource.Cancel();
                _task.Wait(TimeSpan.FromSeconds(10));
                _cancellationTokenSource.Dispose();
            }
        }
    }
}
