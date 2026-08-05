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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.TransactionHandlers;
using QuantConnect.Orders;
using QuantConnect.Tests.Engine;
using QuantConnect.Tests.Engine.DataFeeds;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersBrokerageConnectionTests
    {
        private static readonly FieldInfo HeartBeatTimeLimitField =
            typeof(InteractiveBrokersBrokerage).GetField("_heartBeatTimeLimit", BindingFlags.NonPublic | BindingFlags.Static);

        private TimeSpan _originalHeartBeatTimeLimit;

        [SetUp]
        public void SetUp()
        {
            // the heart beat is skipped after 23:00 local time because of the gateway daily restart, which would
            // make the tests below assert nothing when they happen to run in that hour. Push the limit out so the
            // wall clock stops deciding which branch is exercised.
            _originalHeartBeatTimeLimit = (TimeSpan)HeartBeatTimeLimitField.GetValue(null);
            HeartBeatTimeLimitField.SetValue(null, TimeSpan.FromDays(1));
        }

        [TearDown]
        public void TearDown()
        {
            HeartBeatTimeLimitField.SetValue(null, _originalHeartBeatTimeLimit);
        }

        // The heart beat is the only check that runs unconditionally, so it is what has to notice a connection
        // that never came back. It used to report a healthy beat whenever it was not connected, which meant the
        // detector was switched off by the very condition it exists to detect: a gateway restart that failed to
        // reconnect kept the algorithm running for three days, silently rejecting every order, because no
        // disconnect was ever reported to the brokerage message handler.
        [Test]
        public void HeartBeatReportsFailureWhenTheConnectionIsLostWithoutAnExpectedReason()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            // no client at all, so IsConnected is false: this is the state the deployment sat in for three days
            var automater = CreateInertAutomater();
            SetPrivateField(brokerage, "_ibAutomater", automater);

            Assert.IsFalse(InvokeIsConnected(brokerage), "the fixture must start from a disconnected state");

            // IsWithinScheduledServerResetTimes() reads the wall clock and is the one input that cannot be
            // injected. Skipping is better than asserting the opposite branch here: it keeps the test from
            // silently passing without exercising the case it exists for.
            if (automater.IsWithinScheduledServerResetTimes())
            {
                Assert.Ignore("within the IB scheduled server reset times, a disconnection would be expected right now");
            }

            Assert.IsFalse(InvokeHeartBeat(brokerage),
                "a lost connection that nothing accounts for must be reported as a failed beat");
        }

        // The counterpart of the test above and the reason it cannot simply alarm on every dropped connection:
        // the gateway restarts every night at 23:45 and the socket is torn down on purpose each time. Those
        // windows reconnect within about a minute and must not be reported, otherwise every deployment would be
        // killed nightly.
        [TestCase("_isDisposeCalled", TestName = "HeartBeatStaysQuietWhileDisposing")]
        [TestCase("IsConnecting", TestName = "HeartBeatStaysQuietWhileConnecting")]
        [TestCase("BeginGatewayRestart", TestName = "HeartBeatStaysQuietWhileTheGatewayRestartIsRunning")]
        public void HeartBeatDoesNotReportAnExpectedDisconnection(string expectedReasonField)
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateField(brokerage, "_ibAutomater", CreateInertAutomater());

            if (expectedReasonField == "IsConnecting")
            {
                // a connection attempt holds IsConnected false for as long as it runs, which is minutes when
                // it needs a 2FA confirmation
                GetStateManager(brokerage).IsConnecting = true;
            }
            else if (expectedReasonField == "BeginGatewayRestart")
            {
                BeginGatewayRestart(brokerage);
            }
            else
            {
                SetPrivateField(brokerage, expectedReasonField, true);
            }

            Assert.IsFalse(InvokeIsConnected(brokerage));
            Assert.IsTrue(InvokeHeartBeat(brokerage), "an expected disconnection must not be reported as a failed beat");
        }

        // A restart is accounted for until it finishes, not until its wait is over: the minutes a cold start
        // spends waiting for the weekly 2FA confirmation used to be reported as a lost connection.
        [Test]
        public void HeartBeatStaysQuietUntilTheGatewayRestartFinishes()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var automater = CreateInertAutomater();
            SetPrivateField(brokerage, "_ibAutomater", automater);

            if (automater.IsWithinScheduledServerResetTimes())
            {
                Assert.Ignore("within the IB scheduled server reset times, a disconnection would be expected right now");
            }

            BeginGatewayRestart(brokerage);
            Assert.IsTrue(InvokeHeartBeat(brokerage), "a restart that has not finished accounts for the disconnection");

            EndGatewayRestart(brokerage);
            Assert.IsFalse(InvokeHeartBeat(brokerage),
                "once the restart is finished a lost connection is a real loss again");
        }

        // The weekly restart stops the gateway, which makes the exit handler start its own while the weekly one
        // is still running. A flag would be cleared by whichever finished first, leaving the other unaccounted.
        [Test]
        public void OverlappingGatewayRestartsAreAccountedForUntilTheLastOneFinishes()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var automater = CreateInertAutomater();
            SetPrivateField(brokerage, "_ibAutomater", automater);

            if (automater.IsWithinScheduledServerResetTimes())
            {
                Assert.Ignore("within the IB scheduled server reset times, a disconnection would be expected right now");
            }

            BeginGatewayRestart(brokerage);
            BeginGatewayRestart(brokerage);

            EndGatewayRestart(brokerage);
            Assert.IsTrue(InvokeHeartBeat(brokerage), "the restart still running must keep accounting for the disconnection");

            EndGatewayRestart(brokerage);
            Assert.IsFalse(InvokeHeartBeat(brokerage), "with both finished the lost connection is a real loss again");
        }

        // A reconnection ends every restart in flight at once, so the ones still running would push the count
        // negative and the next restart would be one short of accounting for itself.
        [Test]
        public void AGatewayRestartAfterAReconnectionIsStillAccountedFor()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var automater = CreateInertAutomater();
            SetPrivateField(brokerage, "_ibAutomater", automater);

            BeginGatewayRestart(brokerage);
            InvokeOnReconnected(brokerage, "back");
            // the restart that was in flight finishes after the reconnection already ended it
            EndGatewayRestart(brokerage);

            BeginGatewayRestart(brokerage);

            Assert.IsFalse(InvokeIsConnected(brokerage));
            Assert.IsTrue(InvokeHeartBeat(brokerage), "the new restart must account for its own disconnection");
        }

        // DefaultBrokerageMessageHandler disposes its pending countdown and starts a new one every time it
        // receives a disconnect message, so repeating it while still down would keep pushing the algorithm
        // shutdown into the future and never stop anything. It has to be raised once per disconnection.
        [Test]
        public void DisconnectIsReportedOncePerDisconnection()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, message) => messages.Add(message);

            InvokeOnDisconnected(brokerage, "first");
            InvokeOnDisconnected(brokerage, "second");
            InvokeOnDisconnected(brokerage, "third");

            Assert.AreEqual(1, messages.Count(x => x.Type == BrokerageMessageType.Disconnect));
            Assert.AreEqual("first", messages.Single(x => x.Type == BrokerageMessageType.Disconnect).Message);
        }

        // ...and once we are back the next disconnection is a new episode that has to be reported again,
        // otherwise the safety net only ever works once per deployment.
        [Test]
        public void ReconnectingReArmsTheDisconnectReport()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, message) => messages.Add(message);

            InvokeOnDisconnected(brokerage, "lost");
            InvokeOnReconnected(brokerage, "back");
            InvokeOnDisconnected(brokerage, "lost again");

            Assert.AreEqual(2, messages.Count(x => x.Type == BrokerageMessageType.Disconnect));
            Assert.AreEqual(1, messages.Count(x => x.Type == BrokerageMessageType.Reconnect));
            CollectionAssert.AreEqual(
                new[] { BrokerageMessageType.Disconnect, BrokerageMessageType.Reconnect, BrokerageMessageType.Disconnect },
                messages.Select(x => x.Type));
        }

        // ...and a reset is not a reconnection: it runs on every gateway exit, so re-arming there would emit a
        // disconnect per restart cycle and each one restarts the countdown the report exists to trigger.
        [Test]
        public void ResettingTheStateDoesNotReArmTheDisconnectReport()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, message) => messages.Add(message);

            InvokeOnDisconnected(brokerage, "lost");
            GetStateManager(brokerage).Reset();
            InvokeOnDisconnected(brokerage, "still lost");

            Assert.AreEqual(1, messages.Count(x => x.Type == BrokerageMessageType.Disconnect));
        }

        // A running gateway is not a working one. The restart that is due after a gateway exit used to skip on
        // "IBAutomater is already running", which answers whether the process is alive when the question is
        // whether the brokerage is connected: the gateway left up but unauthenticated by a failed weekly restart
        // answered yes, the restart was skipped, and the deployment sat disconnected for three days.
        [TestCase(false, false, false, "Start", TestName = "AGatewayThatIsNotRunningIsStarted")]
        [TestCase(false, true, false, "Start", TestName = "AGatewayThatIsNotRunningIsStartedEvenIfWeAreConnected")]
        [TestCase(true, true, false, "Skip", TestName = "ARunningAndConnectedGatewayIsLeftAlone")]
        [TestCase(true, false, false, "Replace", TestName = "ARunningGatewayThatTakesNoConnectionIsReplaced")]
        [TestCase(true, false, true, "Skip", TestName = "AGatewayIsOnlyReplacedOncePerOutage")]
        public void TheRestartActionFollowsTheConnectionAndNotTheProcess(bool isRunning, bool isConnected,
            bool alreadyReplaced, string expectedAction)
        {
            var action = typeof(InteractiveBrokersBrokerage)
                .GetMethod("GetGatewayRestartAction", BindingFlags.NonPublic | BindingFlags.Static)
                .Invoke(null, new object[] { isRunning, isConnected, alreadyReplaced });

            Assert.AreEqual(expectedAction, action.ToString());
        }

        // The transaction handler invalidates the order itself when PlaceOrder fails, but only with a generic
        // "Brokerage failed to place orders: [20]" that never says why. It appends the exception message to it,
        // so throwing is what carries the actual cause without reporting success for a rejected order.
        [Test]
        public void PlaceOrderWhenDisconnectedThrowsWithTheReason()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            var orderEvents = new List<OrderEvent>();
            brokerage.Message += (_, message) => messages.Add(message);
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            Assert.IsFalse(brokerage.IsConnected);

            var order = new MarketOrder(Symbols.SPY, -19454, DateTime.UtcNow);
            var exception = Assert.Throws<InvalidOperationException>(() => brokerage.PlaceOrder(order));

            Assert.AreEqual("Orders cannot be submitted when disconnected.", exception.Message);
            // the transaction handler raises the single invalid order event, we must not raise a duplicate
            CollectionAssert.IsEmpty(orderEvents);
            // the brokerage level warning is still raised, it is what support greps for by code
            Assert.AreEqual(1, messages.Count(x => x.Code == "PlaceOrderWhenDisconnected"));
        }

        // ...and the throw above is only worth anything if the transaction handler carries the reason out to the
        // ticket, which is what the algorithm actually reads. Drives the real handler to prove the round trip.
        [Test]
        public void PlaceOrderWhenDisconnectedReportsTheReasonOnTheTicket()
        {
            var algorithm = new AlgorithmStub();
            var equity = algorithm.AddEquity("SPY");
            equity.SetMarketPrice(new Tick { Value = 100m });
            algorithm.SetFinishedWarmingUp();

            // the parameterless constructor skips the subscription validation, PlaceOrder only needs IsConnected
            using var brokerage = new InteractiveBrokersBrokerage();
            Assert.IsFalse(brokerage.IsConnected);

            var transactionHandler = new SynchronousTransactionHandler();
            transactionHandler.Initialize(algorithm, brokerage, new TestResultHandler());
            algorithm.Transactions.SetOrderProcessor(transactionHandler);

            try
            {
                var request = new SubmitOrderRequest(OrderType.Market, equity.Symbol.SecurityType, equity.Symbol, 1m, 0, 0,
                    DateTime.UtcNow, string.Empty);
                algorithm.Transactions.SetOrderId(request);

                var ticket = transactionHandler.Process(request);
                // the queue is drained by the engine loop, which is not running here
                transactionHandler.HandleOrderRequest(request);

                Assert.AreEqual(OrderStatus.Invalid, ticket.Status);
                Assert.IsTrue(ticket.SubmitRequest.Response.IsError, "a rejected order must not report a successful response");
                StringAssert.Contains("Orders cannot be submitted when disconnected.", ticket.SubmitRequest.Response.ErrorMessage);
            }
            finally
            {
                transactionHandler.Exit();
            }
        }

        /// <summary>
        /// No worker threads, so the request is only ever processed by the explicit HandleOrderRequest call above.
        /// </summary>
        private class SynchronousTransactionHandler : BrokerageTransactionHandler
        {
            protected override bool SynchronousProcessing => true;
        }

        /// <summary>
        /// An IBAutomater that is only ever asked about the scheduled reset times, it is never started so no
        /// gateway process is involved.
        /// </summary>
        private static QuantConnect.IBAutomater.IBAutomater CreateInertAutomater()
        {
            return new QuantConnect.IBAutomater.IBAutomater(
                ibDirectory: string.Empty,
                ibVersion: string.Empty,
                userName: string.Empty,
                password: string.Empty,
                tradingMode: "paper",
                portNumber: 4002,
                exportIbGatewayLogs: false);
        }

        private static bool InvokeHeartBeat(InteractiveBrokersBrokerage brokerage)
        {
            // a tiny wait keeps the test fast, the value only drives the internal wait handles
            return (bool)InvokePrivate(brokerage, "HeartBeat", new object[] { 1 });
        }

        private static void InvokeOnDisconnected(InteractiveBrokersBrokerage brokerage, string message)
        {
            InvokePrivate(brokerage, "OnDisconnected", new object[] { message });
        }

        private static void InvokeOnReconnected(InteractiveBrokersBrokerage brokerage, string message)
        {
            InvokePrivate(brokerage, "OnReconnected", new object[] { message });
        }

        private static void BeginGatewayRestart(InteractiveBrokersBrokerage brokerage)
        {
            InvokePrivate(brokerage, "BeginGatewayRestart", Array.Empty<object>());
        }

        private static void EndGatewayRestart(InteractiveBrokersBrokerage brokerage)
        {
            InvokePrivate(brokerage, "EndGatewayRestart", Array.Empty<object>());
        }

        private static bool InvokeIsConnected(InteractiveBrokersBrokerage brokerage)
        {
            return brokerage.IsConnected;
        }

        private static InteractiveBrokersStateManager GetStateManager(InteractiveBrokersBrokerage brokerage)
        {
            return (InteractiveBrokersStateManager)typeof(InteractiveBrokersBrokerage)
                .GetField("_stateManager", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(brokerage);
        }

        private static object InvokePrivate(object instance, string name, object[] arguments)
        {
            return instance.GetType()
                .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .Invoke(instance, arguments);
        }

        private static void SetPrivateField(object instance, string name, object value)
        {
            instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(instance, value);
        }
    }
}
