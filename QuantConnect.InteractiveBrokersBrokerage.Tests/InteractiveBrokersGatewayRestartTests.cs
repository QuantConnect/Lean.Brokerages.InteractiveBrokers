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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.IBAutomater;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine.DataFeeds;
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
        private TimeSpan _retryInterval;

        [SetUp]
        public void SetUp()
        {
            // the reset period tests expect the gateway restart right after the window closes
            _gracePeriod = InteractiveBrokersBrokerage.PostResetReconnectGracePeriod;
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.Zero;
            _retryInterval = InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval;

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
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = _retryInterval;
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

        [TestCase(true)]
        [TestCase(false)]
        public void FailedReconnectWithinTheResetPeriodRaisesNothing(bool afterAutoRestart)
        {
            // the gateway is up and logged in but IB doesn't serve the account while it resets
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart);

            // not even a disconnect: with an open market Lean would stop the algorithm 15 minutes after it
            Assert.IsEmpty(_messages);
        }

        [Test]
        public void OpenMarketAlgorithmIsNotStoppedWhileRetryingWithinTheResetPeriod()
        {
            var algorithm = new AlgorithmStub();
            algorithm.AddSecurities(forex: new List<string> { "EURUSD" });
            var utcNow = DateTime.UtcNow;
            algorithm.SetDateTime(utcNow);
            var exchange = algorithm.Securities.Values.Single().Exchange;
            exchange.SetMarketHours([new MarketHoursSegment(MarketHoursState.Market, TimeSpan.Zero, TimeSpan.FromDays(1))], utcNow.ConvertFromUtc(exchange.TimeZone).DayOfWeek);
            // Lean's handler with its 15 minutes to reconnect cut to 2 seconds, it checks once a minute
            var handler = new DefaultBrokerageMessageHandler(algorithm, new LiveNodePacket(), null, TimeSpan.FromSeconds(2));
            _brokerage.Message += (_, message) => handler.HandleMessage(message);

            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(500);
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart: false);

            Thread.Sleep(TimeSpan.FromSeconds(70));
            Assert.IsNull(algorithm.RunTimeError);
        }

        [TestCase(true, "IBAutomaterAutoRestartError")]
        [TestCase(false, "IBAutomaterRestartError")]
        public void FailedReconnectOutsideTheResetPeriodIsRaisedAsError(bool afterAutoRestart, string expectedCode)
        {
            _automater.IsWithinResetTimes = false;
            Reconnect(afterAutoRestart);

            Assert.AreEqual(expectedCode, _messages.Single(m => m.Type == BrokerageMessageType.Error).Code);
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Disconnect));
        }

        [Test]
        public void ReconnectStillFailingAfterTheResetPeriodRestartsTheGateway()
        {
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(50);
            _automater.IsWithinResetTimes = true;
            // both reconnect paths fail before the first retry, they share its sequence
            Reconnect(afterAutoRestart: true);
            Reconnect(afterAutoRestart: false);

            // the retries keep failing while IB resets
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(1)));
            Assert.IsEmpty(_messages);

            _automater.IsWithinResetTimes = false;

            // a new gateway session is the last resort, its exit handler logs in and reconnects
            Assert.IsTrue(_automater.Stopped.Wait(TimeSpan.FromSeconds(10)));
            Thread.Sleep(500);
            Assert.AreEqual(1, _automater.StopCount);
            // only now is the disconnect raised
            Assert.AreEqual(BrokerageMessageType.Disconnect, _messages.Single().Type);
        }

        [Test]
        public void ReconnectRetriesContinueDuringTheGracePeriod()
        {
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(50);
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.FromHours(1);
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart: true);

            // the reset period is over but IB may still be coming back, as when it overruns its published window
            _automater.IsWithinResetTimes = false;
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(1)));
            Assert.IsEmpty(_messages);

            // the retries were still running: the next one finds the grace period over
            InteractiveBrokersBrokerage.PostResetReconnectGracePeriod = TimeSpan.Zero;
            Assert.IsTrue(_automater.Stopped.Wait(TimeSpan.FromSeconds(10)));
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Error));
        }

        [Test]
        public void ReconnectRetryStartsWithoutTheDisconnectOfAPreviousAttempt()
        {
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(50);
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart: true);

            // a failed attempt's connection reported that the gateway lost IB
            _brokerage.HandleError(this, new IB.ErrorEventArgs(-1, 0, 1100, ConnectivityLost));
            Assert.IsTrue(StateManager.Disconnected1100Fired);

            // no 1102 follows if IB returns while no connection is open: left set, a retry that connects would end as not connected
            Assert.IsTrue(SpinWait.SpinUntil(() => !StateManager.Disconnected1100Fired, TimeSpan.FromSeconds(10)));
            Assert.IsFalse(_messages.Any(m => m.Type == BrokerageMessageType.Error));
        }

        [Test]
        public void ReconnectRetryWaitsForAReconnectInProgress()
        {
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(200);
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart: true);

            // as while a gateway restart or start is inside Connect()
            SetField(_brokerage, "_isReconnecting", 1);
            _automater.IsWithinResetTimes = false;

            // a retry that connected now would fail outside the reset period and stop the gateway
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(1)));
            Assert.IsEmpty(_messages);

            // that reconnect is over and the gateway still isn't connected: the retries go on
            SetField(_brokerage, "_isReconnecting", 0);
            Assert.IsTrue(_automater.Stopped.Wait(TimeSpan.FromSeconds(10)));
        }

        [TestCase(true)]
        [TestCase(false)]
        public void ReconnectStepsAsideForAReconnectInProgress(bool afterAutoRestart)
        {
            // as while a retry is inside Connect()
            SetField(_brokerage, "_isReconnecting", 1);
            _automater.IsWithinResetTimes = false;
            Reconnect(afterAutoRestart);

            // a second Connect() would fail and stop the algorithm with an error
            Assert.IsEmpty(_messages);
            Assert.AreEqual(1, GetField<int>(_brokerage, "_isReconnecting"));
        }

        [Test]
        public void ConnectionEstablishedMeanwhileEndsTheRetries()
        {
            using var gateway = new HandshakeOnlyGateway();
            InteractiveBrokersBrokerage.ResetPeriodReconnectRetryInterval = TimeSpan.FromMilliseconds(500);
            _automater.IsWithinResetTimes = true;
            Reconnect(afterAutoRestart: true);

            // another path connects before the first retry
            var client = new IB.InteractiveBrokersClient(new EReaderMonitorSignal());
            client.ClientSocket.eConnect("127.0.0.1", gateway.Port, 0);
            SetField(_brokerage, "_client", client);
            StateManager.IsConnecting = false;
            Assert.IsTrue(_brokerage.IsConnected);

            // the retries end there: nothing reports the healthy connection as lost and the gateway is left alone
            _automater.IsWithinResetTimes = false;
            Assert.IsFalse(_automater.Stopped.Wait(TimeSpan.FromSeconds(2)));
            Assert.IsEmpty(_messages);
            Assert.IsTrue(_brokerage.IsConnected);
        }

        /// <summary>
        /// Reconnects through the gateway's auto-restart or through a gateway start, failing in both: this brokerage has no gateway to connect to
        /// </summary>
        private void Reconnect(bool afterAutoRestart)
        {
            if (afterAutoRestart)
            {
                InvokeMethod(_brokerage, "OnIbAutomaterRestarted", null, EventArgs.Empty);
                return;
            }

            // a gateway already running, so the start succeeds and the connection is attempted
            SetField(_automater, "_process", Process.GetCurrentProcess());
            try
            {
                InvokeMethod(_brokerage, "StartGatewayAndConnect");
            }
            finally
            {
                // not ours to stop
                SetField(_automater, "_process", null);
            }
        }

        /// <summary>
        /// Answers the IB API handshake and keeps the connection open, enough for a client to be connected
        /// </summary>
        private sealed class HandshakeOnlyGateway : IDisposable
        {
            private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
            private TcpClient _client;

            public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            public HandshakeOnlyGateway()
            {
                _listener.Start();
                _listener.AcceptTcpClientAsync().ContinueWith(accepted =>
                {
                    _client = accepted.Result;
                    var serverVersion = Encoding.ASCII.GetBytes("199\020261107 23:45:31 EST\0");
                    var writer = new BinaryWriter(_client.GetStream());
                    writer.Write(BinaryPrimitives.ReverseEndianness(serverVersion.Length));
                    writer.Write(serverVersion);
                }, TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            public void Dispose()
            {
                _client?.Dispose();
                _listener.Stop();
            }
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
