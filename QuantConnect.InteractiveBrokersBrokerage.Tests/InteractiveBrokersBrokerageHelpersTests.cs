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

using NUnit.Framework;

using QuantConnect.Brokerages.InteractiveBrokers;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersBrokerageHelpersTests
    {
        [Test]

        public void GetsNextSunday()
        {
            var baseDate = new DateTime(2022, 12, 5); // Monday
            Assert.AreEqual(DayOfWeek.Monday, baseDate.DayOfWeek);
            var expectedNextSunday = new DateTime(2022, 12, 11); // Sunday
            Assert.AreEqual(DayOfWeek.Sunday, expectedNextSunday.DayOfWeek);

            for (var i = 0; i < 7; i++)
            {
                var date = baseDate.AddDays(i);
                var nextSunday = InteractiveBrokersBrokerage.GetNextSundayFromDate(date);

                Assert.AreEqual(expectedNextSunday, nextSunday);
            }
        }


        [TestCaseSource(nameof(StartDatesToComputeRestartDelay))]
        public void CalculatesTheWeeklyRestartDelay(DateTime currentDate, DateTime expectedSunday)
        {
            var restartTimeOfDay = new TimeSpan(9, 30, 0);
            var time = InteractiveBrokersBrokerage.ComputeNextWeeklyRestartTimeUtc(restartTimeOfDay, currentDate);

            var expectedTime = expectedSunday.Date.Add(restartTimeOfDay);

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
                new DateTime(2022, 12, 4))   // Same Sunday
        };
    }
}