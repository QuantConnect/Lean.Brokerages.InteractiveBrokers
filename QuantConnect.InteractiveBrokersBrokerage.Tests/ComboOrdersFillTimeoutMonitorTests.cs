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

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class ComboOrdersFillTimeoutMonitorTests
    {
        private TimeSpan _timeout;
        private ComboOrdersFillTimeoutMonitor _monitor;
        private int _callbackCallsCount;
        private int _enqueuedItemsCount;
        private ConcurrentBag<Tuple<DateTime, DateTime>> _monitorCallbackCallTimes;
        private AutoResetEvent _signalStopEvent = new(false);
        private ITimeProvider _timeProvider;

        [SetUp]
        public void SetUp()
        {
            _timeout = TimeSpan.FromSeconds(1);
            _timeProvider = RealTimeProvider.Instance;
            _monitor = new ComboOrdersFillTimeoutMonitor(_timeProvider, _timeout);
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
            _monitor.TimeoutEvent += _monitor_TimeoutEvent;
            _monitor.Start();

            EnqueueCallbackCall(factor);

            _signalStopEvent.WaitOne();
            _monitor.Stop();
            _monitor.TimeoutEvent -= _monitor_TimeoutEvent;

            Assert.AreEqual(3, _callbackCallsCount);
            foreach (var callTimes in _monitorCallbackCallTimes)
            {
                var diff = (callTimes.Item2 - callTimes.Item1).Duration();
                Assert.LessOrEqual(diff, TimeSpan.FromMilliseconds(150));
            }
        }

        private void _monitor_TimeoutEvent(object sender, PendingFillEvent e)
        {
            _monitorCallbackCallTimes.Add(Tuple.Create(DateTime.UtcNow - _timeout, e.Order.Time));
            if (Interlocked.Increment(ref _callbackCallsCount) == 3)
            {
                _signalStopEvent.Set();
            }
        }

        private void EnqueueCallbackCall(double factor)
        {
            var now = DateTime.UtcNow;
            _monitor.AddPendingFill(
                Order.CreateOrder(new SubmitOrderRequest(OrderType.ComboMarket, SecurityType.Option, Symbols.SPY, 1, 0, 0, now, "")),
                new IB.ExecutionDetailsEventArgs(1, new Contract(), new Execution()),
                new CommissionReport()
            );

            Thread.Sleep(_timeout * factor);

            if (++_enqueuedItemsCount == 3)
            {
                return;
            }

            EnqueueCallbackCall(factor);
        }
    }
}