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
            // the heart beat is skipped after 23:00 local time because of the gateway daily restart:
            // push the limit out so the wall clock does not decide which branch is exercised
            _originalHeartBeatTimeLimit = (TimeSpan)HeartBeatTimeLimitField.GetValue(null);
            HeartBeatTimeLimitField.SetValue(null, TimeSpan.FromDays(1));
        }

        [TearDown]
        public void TearDown()
        {
            HeartBeatTimeLimitField.SetValue(null, _originalHeartBeatTimeLimit);
        }

        // The heart beat used to report a healthy beat whenever it was not connected, which kept a
        // gateway restart that never came back unnoticed for days: being disconnected is a failed
        // beat unless a recent gateway exit accounts for it.
        [Test]
        public void HeartBeatReportsFailureWhenTheConnectionIsLostWithoutARecentGatewayExit()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var automater = CreateInertAutomater();
            SetPrivateField(brokerage, "_ibAutomater", automater);

            Assert.IsFalse(brokerage.IsConnected, "the fixture must start from a disconnected state");

            // the wall clock cannot be injected: skipping keeps the test from silently passing
            // without exercising the case it exists for
            if (automater.IsWithinScheduledServerResetTimes())
            {
                Assert.Ignore("within the IB scheduled server reset times, a disconnection would be expected right now");
            }

            Assert.IsFalse(InvokeHeartBeat(brokerage),
                "a lost connection that no restart accounts for must be reported as a failed beat");
        }

        // ...and within the grace period of a gateway exit the disconnection is the restart at work:
        // the exit driven restart waits minutes before starting and the login can wait minutes more
        // for its 2FA confirmation, none of which must be reported nightly.
        [Test]
        public void HeartBeatStaysQuietWithinTheGatewayRecoveryGracePeriod()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateField(brokerage, "_ibAutomater", CreateInertAutomater());
            SetPrivateField(brokerage, "_lastIBAutomaterExitTime", DateTime.UtcNow);

            Assert.IsFalse(brokerage.IsConnected);
            Assert.IsTrue(InvokeHeartBeat(brokerage), "a recent gateway exit accounts for the disconnection");
        }

        [Test]
        public void HeartBeatStaysQuietWhileDisposing()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateField(brokerage, "_ibAutomater", CreateInertAutomater());
            SetPrivateField(brokerage, "_isDisposeCalled", true);

            Assert.IsTrue(InvokeHeartBeat(brokerage), "disposing accounts for the disconnection");
        }

        // a connection attempt holds IsConnected false for as long as it runs, which is minutes when
        // it needs a 2FA confirmation
        [Test]
        public void HeartBeatStaysQuietWhileConnecting()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            SetPrivateField(brokerage, "_ibAutomater", CreateInertAutomater());
            GetStateManager(brokerage).IsConnecting = true;

            Assert.IsTrue(InvokeHeartBeat(brokerage), "a connection attempt in progress accounts for the disconnection");
        }

        // DefaultBrokerageMessageHandler disposes its pending countdown and starts a new one on every
        // disconnect message, so repeating it while still down would defer the shutdown forever.
        [Test]
        public void DisconnectIsReportedOncePerDisconnection()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, message) => messages.Add(message);

            InvokeOnDisconnected(brokerage, "first");
            InvokeOnDisconnected(brokerage, "second");

            Assert.AreEqual(1, messages.Count(x => x.Type == BrokerageMessageType.Disconnect));
            Assert.AreEqual("first", messages.Single(x => x.Type == BrokerageMessageType.Disconnect).Message);
        }

        // ...and once we are back the next disconnection is a new episode that has to be reported
        // again, otherwise the safety net only ever works once per deployment.
        [Test]
        public void ReconnectingReArmsTheDisconnectReport()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, message) => messages.Add(message);

            InvokeOnDisconnected(brokerage, "lost");
            InvokeOnReconnected(brokerage, "back");
            InvokeOnDisconnected(brokerage, "lost again");

            CollectionAssert.AreEqual(
                new[] { BrokerageMessageType.Disconnect, BrokerageMessageType.Reconnect, BrokerageMessageType.Disconnect },
                messages.Select(x => x.Type));
        }

        // The weekly restart exists to get the 2FA confirmation requested at the time the user
        // picked, so it skips on having logged in today. Skipping on having exited today is what
        // broke: on the two Sundays reported the token expiry exit at 23:45 made the 23:54 check
        // skip and the deployment traded on a session nobody renewed.
        [Test]
        public void TheWeeklyRestartRunsUntilTheGatewayHasLoggedInToday()
        {
            // the 23:54 check of the Sunday in the report, five minutes before the configured 23:59
            var utcNow = new DateTime(2026, 8, 2, 23, 54, 0, DateTimeKind.Utc);

            Assert.IsTrue(ShouldRunWeeklyRestart(default, utcNow), "a gateway that never logged in must restart");
            // the deployment logged in on the Friday, two days before: this is the case that was being skipped
            Assert.IsTrue(ShouldRunWeeklyRestart(new DateTime(2026, 7, 31, 20, 51, 0, DateTimeKind.Utc), utcNow),
                "the last login was on Friday, the weekly confirmation is due");
            // and once it has logged in today there is nothing left for the weekly restart to ask for
            Assert.IsFalse(ShouldRunWeeklyRestart(new DateTime(2026, 8, 2, 23, 46, 0, DateTimeKind.Utc), utcNow));
        }

        /// <summary>
        /// An IBAutomater that is only ever asked about the scheduled reset times, never started.
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

        private static bool ShouldRunWeeklyRestart(DateTime lastLoginTimeUtc, DateTime utcNow)
        {
            return (bool)typeof(InteractiveBrokersBrokerage)
                .GetMethod("ShouldRunWeeklyRestart", BindingFlags.NonPublic | BindingFlags.Static)
                .Invoke(null, new object[] { lastLoginTimeUtc, utcNow });
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
