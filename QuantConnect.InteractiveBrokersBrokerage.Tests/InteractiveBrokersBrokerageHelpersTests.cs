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

using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine;
using QuantConnect.Tests.Engine.BrokerageTransactionHandlerTests;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersBrokerageHelpersTests
    {
        [TestCaseSource(nameof(StartDatesToComputeRestartDelay))]
        public void CalculatesTheWeeklyRestartDelay(DateTime from, DateTime sunday)
        {
            var timeOfDay = new TimeSpan(9, 30, 0);
            var time = InteractiveBrokersBrokerage.ComputeNextWeeklyRestartTimeUtc(timeOfDay, from);

            var expectedTime = sunday.Date.Add(timeOfDay);

            Assert.AreEqual(expectedTime, time);
        }

        // (start date, next Sunday)
        private static TestCaseData[] StartDatesToComputeRestartDelay => new[]
        {
            // Start on Monday
            new TestCaseData(
                new DateTime(2022, 8, 29, 12, 30, 45), // Monday
                new DateTime(2022, 9, 4)),   // Next Sunday
            // Sunday
            new TestCaseData(
                new DateTime(2022, 12, 4, 12, 30, 25),  // Sunday
                new DateTime(2022, 12, 4))   // Next Sunday
        };
    }
}