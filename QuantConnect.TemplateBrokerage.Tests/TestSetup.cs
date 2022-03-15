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
using System.IO;
using NUnit.Framework;
using System.Collections;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.TemplateBrokerage.Tests
{
    [TestFixture]
    public class TestSetup
    {
        [Test, TestCaseSource(nameof(TestParameters))]
        public void TestSetupCase()
        {
        }

        public static void ReloadConfiguration()
        {
            // nunit 3 sets the current folder to a temp folder we need it to be the test bin output folder
            var dir = TestContext.CurrentContext.TestDirectory;
            Environment.CurrentDirectory = dir;
            Directory.SetCurrentDirectory(dir);
            // reload config from current path
            Config.Reset();

            var environment = Environment.GetEnvironmentVariables();
            foreach (DictionaryEntry entry in environment)
            {
                var envKey = entry.Key.ToString();
                var value = entry.Value.ToString();

                if (envKey.StartsWith("QC_"))
                {
                    var key = envKey.Substring(3).Replace("_", "-").ToLower();

                    Log.Trace($"TestSetup(): Updating config setting '{key}' from environment var '{envKey}'");
                    Config.Set(key, value);
                }
            }

            // resets the version among other things
            Globals.Reset();
        }

        private static void SetUp()
        {
            Log.LogHandler = new CompositeLogHandler();
            Log.Trace("TestSetup(): starting...");
            ReloadConfiguration();
        }

        private static TestCaseData[] TestParameters
        {
            get
            {
                SetUp();
                return new [] { new TestCaseData() };
            }
        }
    }
}