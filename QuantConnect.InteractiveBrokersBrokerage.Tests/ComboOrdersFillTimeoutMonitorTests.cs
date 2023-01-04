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
using System.Collections.Concurrent;
using System.Threading;

using NUnit.Framework;

using IBApi;

using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Configuration;
using QuantConnect.Orders;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;
using QuantConnect.Logging;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class ComboOrdersFillTimeoutMonitorTests
    {
        private static ComboOrdersFillTimeoutMonitor _monitor;

        private static int _callbackCallsCount;

        private static int _enqueuedItemsCount;

        private static ConcurrentBag<Tuple<DateTime, DateTime>> _monitorCallbackCallTimes;

        private static AutoResetEvent _signalStopEvent = new(false);

        [SetUp]
        public void SetUp()
        {
            Config.Set("ib-combo-order-fill-timeout", 1);

            _monitor = new ComboOrdersFillTimeoutMonitor();
            _callbackCallsCount = 0;
            _enqueuedItemsCount = 0;
            _monitorCallbackCallTimes = new ();
        }

        [TearDown]
        public void TearDown()
        {
            Config.Reset();
        }

        [TestCase(1.5)]
        [TestCase(0.5)]
        [TestCase(1)]
        [TestCase(0)]
        public void DequeuesAtTheRightTimes(double factor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _monitor.Run(MonitorCallback, cancellationTokenSource);

            EnqueueCallbackCall(factor);

            _signalStopEvent.WaitOne();
            _monitor.Stop();

            Assert.AreEqual(3, _callbackCallsCount);
            foreach (var callTimes in _monitorCallbackCallTimes)
            {
                var diff = (callTimes.Item2 - callTimes.Item1).Duration();
                Assert.LessOrEqual(diff, TimeSpan.FromMilliseconds(150));
            }
        }

        private static void MonitorCallback(Order order, IB.ExecutionDetailsEventArgs executionDetails, CommissionReport commisionReport,
            bool forceFillEmission)
        {
            _monitorCallbackCallTimes.Add(Tuple.Create(DateTime.UtcNow - _timeout, order.Time));
            if (Interlocked.Increment(ref _callbackCallsCount) == 3)
            {
                _signalStopEvent.Set();
            }
        }

        private static void EnqueueCallbackCall(double factor)
        {
            var now = DateTime.UtcNow;
            _monitor.AddPendingFill(new PendingFillingEventDetails()
            {
                Order = Order.CreateOrder(new SubmitOrderRequest(OrderType.ComboMarket, SecurityType.Option, Symbols.SPY, 1, 0, 0, now, "")),
                ExecutionDetails = new IB.ExecutionDetailsEventArgs(1, new Contract(), new Execution()),
                CommissionReport = new CommissionReport(),
                UtcTime = now
            });

            Thread.Sleep(_timeout * factor);

            if (++_enqueuedItemsCount == 3)
            {
                return;
            }

            EnqueueCallbackCall(factor);
        }

        private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(Config.GetInt("ib-combo-order-fill-timeout"));
    }
}