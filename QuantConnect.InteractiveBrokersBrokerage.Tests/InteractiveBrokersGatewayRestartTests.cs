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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.IBAutomater;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersGatewayRestartTests
    {
        private string _gatewayDirectory;
        private InteractiveBrokersBrokerage _brokerage;
        private TestIBAutomater _automater;
        private List<BrokerageMessageEvent> _messages;
        private List<string> _automaterOutput;
        private TimeSpan _gracePeriod;

        [SetUp]
        public void SetUp()
        {
            // the reset period tests expect the gateway restart right after the window closes
            _gracePeriod = InteractiveBrokersBrokerage.PostResetReconnectGracePeriod;
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.Zero;

            // a gateway installation without its bundled Java: any start attempt fails with JavaNotFound before launching anything
            _gatewayDirectory = Path.Combine(Path.GetTempPath(), $"ib-gateway-restart-tests-{Guid.NewGuid():N}");
            // IBAutomater looks for the gateway under {dir}/ibgateway/{version} on Windows and at {dir} itself on Linux
            var versionDirectory = OperatingSystem.IsWindows() ? Path.Combine(_gatewayDirectory, "ibgateway", "1034") : _gatewayDirectory;
            var executable = OperatingSystem.IsWindows() ? "ibgateway.exe" : "ibgateway";
            Directory.CreateDirectory(versionDirectory);
            File.WriteAllText(Path.Combine(versionDirectory, executable), string.Empty);
            File.WriteAllText(Path.Combine(versionDirectory, "ibgateway.vmoptions"), string.Empty);

            _automater = new TestIBAutomater(_gatewayDirectory);
            _automaterOutput = new List<string>();
            _automater.OutputDataReceived += (_, e) => _automaterOutput.Add(e.Data);

            _brokerage = new InteractiveBrokersBrokerage();
            SetField(_brokerage, "_ibAutomater", _automater);
            // the deployment already connected once, as when the nightly or weekly gateway restarts happen
            SetField(_brokerage, "_pastFirstConnection", true);
            _automater.Exited += (sender, e) => InvokeMethod(_brokerage, "OnIbAutomaterExited", sender, e);
            _automater.OutputDataReceived += (sender, e) => InvokeMethod(_brokerage, "OnIbAutomaterOutputDataReceived", sender, e);

            _messages = new List<BrokerageMessageEvent>();
            _brokerage.Message += (_, message) => _messages.Add(message);
        }

        [TearDown]
        public void TearDown()
        {
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = _gracePeriod;
            // ends the reconnect polling the tests started
            StateManager.Reset();
            _brokerage.Dispose();
            Directory.Delete(_gatewayDirectory, true);
        }

        private InteractiveBrokersStateManager StateManager => GetField<InteractiveBrokersStateManager>(_brokerage, "_stateManager");

        [Test]
        public void FailedAutoRestartRaisesTheActualError()
        {
            // the gateway exits for its nightly auto-restart and the relaunch fails
            SetField(_automater, "_isRestartInProgress", true);
            InvokeMethod(_automater, "OnProcessExited", null, EventArgs.Empty);

            Assert.AreEqual(ErrorCode.JavaNotFound, _automater.GetLastStartResult().ErrorCode);
            Assert.IsTrue(_messages.Any(m => m.Type == BrokerageMessageType.Error && m.Code == nameof(ErrorCode.JavaNotFound)));

            // the exit of the failed relaunch is not handled as another auto-restart
            InvokeMethod(_automater, "OnProcessExited", null, EventArgs.Empty);
            Assert.AreEqual(1, _automaterOutput.Count(x => x == "Waiting for IBGateway auto-restart"));
        }

        [Test]
        public void GatewayStartRetriesAfterInitializationTimeout()
        {
            // awaiting the user after a timed out login, the weekly restart finds the gateway not running
            SetField(_automater, "_lastStartResult", new StartResult(ErrorCode.InitializationTimeout));
            InvokeMethod(_brokerage, "StartGatewayAndConnect");

            // the start was attempted again instead of returning the stored timeout, and its failure is raised once
            Assert.IsTrue(_automaterOutput.Any(x => x.StartsWith("Loading IBGateway")));
            var error = _messages.Single(m => m.Type == BrokerageMessageType.Error);
            Assert.AreEqual(nameof(ErrorCode.JavaNotFound), error.Code);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void RefusedAutoRestartLoginIsRaisedAsDisconnect(bool isWithinResetTimes)
        {
            // the gateway relaunched for its nightly auto-restart and IB refused the re-login of the saved session
            _automater.IsWithinResetTimes = isWithinResetTimes;
            SetField(_automater, "_isRestartInProgress", true);
            InvokeMethod(_automater, "OnProcessOutputDataReceived", RefusedLogin);
            InvokeMethod(_automater, "OnProcessOutputDataReceived", RefusedLogin);

            // handled like an expired auto-restart token: the gateway exit retries with a full login instead of reporting an error
            Assert.IsFalse(GetField<bool>(_automater, "_isRestartInProgress"));
            Assert.IsFalse(_automater.GetLastStartResult().HasError);
            Assert.AreEqual(1, _messages.Count(m => m.Type == BrokerageMessageType.Disconnect));
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Error || m.Type == BrokerageMessageType.ActionRequired));
        }

        [TestCase(true, false)]
        [TestCase(false, true)]
        public void RefusedLoginWithoutAutoRestartIsUnchanged(bool isWithinResetTimes, bool expectedLoginFailed)
        {
            _automater.IsWithinResetTimes = isWithinResetTimes;
            InvokeMethod(_automater, "OnProcessOutputDataReceived", RefusedLogin);

            Assert.AreEqual(expectedLoginFailed, _automater.GetLastStartResult().ErrorCode == ErrorCode.LoginFailed);
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));
        }

        [Test]
        public void DisconnectOutlivingTheResetPeriodRestartsTheGateway()
        {
            _automater.IsWithinResetTimes = true;
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));

            // the reset period is over and IB never restored the connection
            _automater.IsWithinResetTimes = false;
            InvokeMethod(_brokerage, "TryWaitForReconnect");

            Assert.AreEqual(1, _messages.Count(m => m.Type == BrokerageMessageType.Disconnect));
            Assert.IsTrue(_automater.Stopped.Wait(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(1, _automater.StopCount);
        }

        [Test]
        public void DisconnectRestoredWithinTheResetPeriodDoesNotRestartTheGateway()
        {
            _automater.IsWithinResetTimes = true;
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1102, ConnectivityRestored));

            // the poll scheduled within the reset period finds the connection restored
            _automater.IsWithinResetTimes = false;
            InvokeMethod(_brokerage, "TryWaitForReconnect");
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));

            // a later disconnect is raised as such, the gateway is not restarted
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));

            Assert.AreEqual(1, _messages.Count(m => m.Type == BrokerageMessageType.Disconnect));
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(2)));
        }

        [Test]
        public void DisconnectOutlivingTheResetPeriodWaitsForTheGracePeriod()
        {
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.FromHours(1);
            _automater.IsWithinResetTimes = true;
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));

            // the reset period is over but the connection may still come back, as when IB overruns its published window
            _automater.IsWithinResetTimes = false;
            InvokeMethod(_brokerage, "TryWaitForReconnect");

            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));
            Assert.AreEqual(0, _automater.StopCount);
            Assert.IsTrue(StateManager.PreviouslyInResetTime);
            Assert.IsNotNull(StateManager.ResetPeriodEndedUtc);

            // the connection is restored within the grace period
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1102, ConnectivityRestored));

            Assert.IsFalse(StateManager.Disconnected1100Fired);
            Assert.IsFalse(StateManager.PreviouslyInResetTime);
            Assert.IsNull(StateManager.ResetPeriodEndedUtc);

            // the incident is over: the next poll does nothing even with the grace period expired
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.Zero;
            InvokeMethod(_brokerage, "TryWaitForReconnect");

            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(2)));
            Assert.AreEqual(0, _automater.StopCount);
        }

        [Test]
        public void ANewDisconnectStartsWithoutResetPeriodHistory()
        {
            // as left by a reconnect poll that ran while a 1102 restored the connection
            StateManager.PreviouslyInResetTime = true;
            Assert.IsFalse(StateManager.Disconnected1100Fired);

            _automater.IsWithinResetTimes = false;
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));

            var disconnect = _messages.Single(m => m.Type == BrokerageMessageType.Disconnect);
            StringAssert.Contains("Connection with Interactive Brokers lost", disconnect.Message);
            Assert.IsFalse(StateManager.PreviouslyInResetTime);
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(2)));
            Assert.AreEqual(0, _automater.StopCount);
        }

        [Test]
        public void ConnectionRestoredDuringAPostResetPollIsNotRaisedAsADisconnect()
        {
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.FromHours(1);
            _automater.IsWithinResetTimes = true;
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));

            // the window closed and the first post-window poll opened the grace period
            _automater.IsWithinResetTimes = false;
            InvokeMethod(_brokerage, "TryWaitForReconnect");
            Assert.IsNotNull(StateManager.ResetPeriodEndedUtc);

            // IB restores the connection on the reader thread after the next poll passed its first check
            _automater.OnResetTimesQueried = () =>
            {
                _automater.OnResetTimesQueried = null;
                _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1102, ConnectivityRestored));
            };
            InvokeMethod(_brokerage, "TryWaitForReconnect");

            // nothing was raised during the window, so the base class drops the 1102's Reconnect and a Disconnect here would stick
            Assert.IsEmpty(_messages);
            Assert.IsFalse(StateManager.Disconnected1100Fired);
            Assert.IsFalse(StateManager.PreviouslyInResetTime);
            Assert.IsNull(StateManager.ResetPeriodEndedUtc);
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(2)));
            Assert.AreEqual(0, _automater.StopCount);
        }

        private const string RefusedLogin = "Login failed: Passwords are case sensitive. Please ensure that the Caps Lock key is not activated.";
        private const string ConnectivityLost = "Connectivity between IBKR and Trader Workstation has been lost.";
        private const string ConnectivityRestored = "Connectivity between IBKR and Trader Workstation has been restored - data maintained.";

        private class TestIBAutomater : IBAutomater.IBAutomater
        {
            public bool IsWithinResetTimes { get; set; }
            // lets a test interleave IB messages with a reconnect poll that already started
            public Action OnResetTimesQueried { get; set; }
            public int StopCount;
            public ManualResetEventSlim Stopped { get; } = new();

            public TestIBAutomater(string gatewayDirectory)
                : base(gatewayDirectory, "1034", "user", "password", "paper", 4002, false)
            {
            }

            public override bool IsWithinScheduledServerResetTimes()
            {
                OnResetTimesQueried?.Invoke();
                return IsWithinResetTimes;
            }

            public override void Stop()
            {
                Interlocked.Increment(ref StopCount);
                Stopped.Set();
                base.Stop();
            }
        }

        private static T GetField<T>(object instance, string name)
        {
            return (T)GetMember(instance, type => type.GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)).GetValue(instance);
        }

        private static void SetField(object instance, string name, object value)
        {
            GetMember(instance, type => type.GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)).SetValue(instance, value);
        }

        private static void InvokeMethod(object instance, string name, params object[] parameters)
        {
            GetMember(instance, type => type.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)).Invoke(instance, parameters);
        }

        private static T GetMember<T>(object instance, Func<Type, T> getMember)
            where T : MemberInfo
        {
            // the private members of the IBAutomater are declared by the base type of the test one
            var type = instance.GetType();
            return getMember(type) ?? getMember(type.BaseType);
        }
    }
}
