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
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.IBAutomater;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersGatewayRestartTests
    {
        private string _gatewayDirectory;
        private InteractiveBrokersBrokerage _brokerage;
        private IBAutomater.IBAutomater _automater;
        private List<BrokerageMessageEvent> _messages;
        private List<string> _automaterOutput;

        [SetUp]
        public void SetUp()
        {
            // a gateway installation without its bundled Java: any start attempt fails with JavaNotFound before launching anything
            _gatewayDirectory = Path.Combine(Path.GetTempPath(), $"ib-gateway-restart-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(_gatewayDirectory);
            File.WriteAllText(Path.Combine(_gatewayDirectory, "ibgateway"), string.Empty);
            File.WriteAllText(Path.Combine(_gatewayDirectory, "ibgateway.vmoptions"), string.Empty);

            _automater = new IBAutomater.IBAutomater(_gatewayDirectory, "1034", "user", "password", "paper", 4002, false);
            _automaterOutput = new List<string>();
            _automater.OutputDataReceived += (_, e) => _automaterOutput.Add(e.Data);

            _brokerage = new InteractiveBrokersBrokerage();
            SetField(_brokerage, "_ibAutomater", _automater);
            // the deployment already connected once, as when the nightly or weekly gateway restarts happen
            SetField(_brokerage, "_pastFirstConnection", true);
            _automater.Exited += (sender, e) => InvokeMethod(_brokerage, "OnIbAutomaterExited", sender, e);

            _messages = new List<BrokerageMessageEvent>();
            _brokerage.Message += (_, message) => _messages.Add(message);
        }

        [TearDown]
        public void TearDown()
        {
            _brokerage.Dispose();
            Directory.Delete(_gatewayDirectory, true);
        }

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

        private static void SetField(object instance, string name, object value)
        {
            instance.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance).SetValue(instance, value);
        }

        private static void InvokeMethod(object instance, string name, params object[] parameters)
        {
            instance.GetType().GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance).Invoke(instance, parameters);
        }
    }
}
