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
using QuantConnect.Orders;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class ComboOrdersFillTimeoutMonitorTests
    {
        private TimeSpan _timeout;
        private ComboOrdersFillTimeoutMonitor _monitor;
        private int _callbackCallsCount;
        private ConcurrentBag<Tuple<DateTime, DateTime>> _monitorCallbackCallTimes;
        private AutoResetEvent _signalStopEvent = new(false);
        private ManualTimeProvider _timeProvider;

        [SetUp]
        public void SetUp()
        {
            _timeout = TimeSpan.FromSeconds(1);
            _timeProvider = new(new DateTime(2023, 1, 6, 9, 30, 0));
            _monitor = new ComboOrdersFillTimeoutMonitor(_timeProvider, _timeout);
            _callbackCallsCount = 0;
            _monitorCallbackCallTimes = new ();
        }

        [Test]
        public void DequeuesAtTheRightTimesForSingleEvent()
        {
            _monitor.TimeoutEvent += TimeoutEventHandler;
            _monitor.Start();

            _monitor.AddPendingFill(
                Order.CreateOrder(new SubmitOrderRequest(OrderType.ComboMarket, SecurityType.Option, Symbols.SPY, 1, 0, 0, _timeProvider.GetUtcNow(), "")),
                new IB.ExecutionDetailsEventArgs(1, new Contract(), new Execution()),
                new CommissionAndFeesReport()
            );

            Thread.Sleep(_timeout * 2);
            Assert.AreEqual(0, _callbackCallsCount);

            _timeProvider.Advance(_timeout);

            Assert.IsTrue(_signalStopEvent.WaitOne(_timeout * 2));

            _monitor.Stop();
            _monitor.TimeoutEvent -= TimeoutEventHandler;

            Assert.AreEqual(1, _callbackCallsCount);
            foreach (var callTimes in _monitorCallbackCallTimes)
            {
                var diff = (callTimes.Item2 - callTimes.Item1).Duration();
                Assert.LessOrEqual(diff, TimeSpan.FromMilliseconds(150));
            }
        }

        private void TimeoutEventHandler(object sender, PendingFillEvent e)
        {
            _monitorCallbackCallTimes.Add(Tuple.Create(_timeProvider.GetUtcNow() - _timeout, e.Order.Time));
            Interlocked.Increment(ref _callbackCallsCount);
            _signalStopEvent.Set();
        }

        [Test]
        public void DequeuesAtTheRightTimesForMultipleEvents()
        {
            _monitor.TimeoutEvent += TimeoutEventHandlerMultipleFills;
            _monitor.Start();

            for (var i = 0; i < 3; i++)
            {
                _monitor.AddPendingFill(
                    Order.CreateOrder(new SubmitOrderRequest(OrderType.ComboMarket, SecurityType.Option, Symbols.SPY, 1, 0, 0, _timeProvider.GetUtcNow(), "")),
                    new IB.ExecutionDetailsEventArgs(1, new Contract(), new Execution()),
                    new CommissionAndFeesReport()
                );

                _timeProvider.Advance(_timeout / 4);
            }

            for (var i = 0; i < 3; i++)
            {
                Assert.AreEqual(i, _callbackCallsCount);
                _timeProvider.Advance(_timeout / 4);
                Assert.IsTrue(_signalStopEvent.WaitOne(_timeout * 2));
                Assert.AreEqual(i + 1, _callbackCallsCount);
            }

            _monitor.Stop();
            _monitor.TimeoutEvent -= TimeoutEventHandlerMultipleFills;

            Assert.AreEqual(3, _callbackCallsCount);
            foreach (var callTimes in _monitorCallbackCallTimes)
            {
                var diff = (callTimes.Item2 - callTimes.Item1).Duration();
                Assert.LessOrEqual(diff, TimeSpan.FromMilliseconds(150));
            }
        }

        private void TimeoutEventHandlerMultipleFills(object sender, PendingFillEvent e)
        {
            _monitorCallbackCallTimes.Add(Tuple.Create(_timeProvider.GetUtcNow() - _timeout, e.Order.Time));
            Interlocked.Increment(ref _callbackCallsCount);
            _signalStopEvent.Set();
        }
    }
}
