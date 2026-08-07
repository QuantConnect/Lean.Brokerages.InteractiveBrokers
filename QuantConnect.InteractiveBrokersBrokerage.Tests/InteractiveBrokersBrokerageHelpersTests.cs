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
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

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

        // IBGateway confirms the first order of a deployment on a Financial Advisor account with a warning
        // dialog and silently drops every other order that reaches it while that dialog is unanswered, so
        // only one order may be in flight until the first one is answered
        [Test]
        public void OnlyTheFirstFinancialAdvisorOrderIsLetThroughUntilItIsAnswered()
        {
            using var brokerage = CreateBrokerage(financialAdvisor: true);

            Assert.IsTrue(WaitForFinancialAdvisorFirstOrder(brokerage), "the first order should claim the gate");

            var released = 0;
            var followers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                Assert.IsFalse(WaitForFinancialAdvisorFirstOrder(brokerage), "only one order may claim the gate");
                Interlocked.Increment(ref released);
            })).ToArray();

            // the followers must stay held back while the first order is unanswered
            Assert.IsFalse(Task.WaitAll(followers, TimeSpan.FromMilliseconds(500)));
            Assert.AreEqual(0, released);

            GetFirstOrderAnsweredEvent(brokerage).Set();

            Assert.IsTrue(Task.WaitAll(followers, TimeSpan.FromSeconds(5)), "the batch should be released");
            Assert.AreEqual(4, released);
        }

        [Test]
        public void FinancialAdvisorOrdersAreNotHeldBackOnceTheFirstOneIsAnswered()
        {
            using var brokerage = CreateBrokerage(financialAdvisor: true);
            GetFirstOrderAnsweredEvent(brokerage).Set();

            // no order claims the gate anymore, so none of them waits
            Assert.IsFalse(WaitForFinancialAdvisorFirstOrder(brokerage));
            Assert.IsFalse(WaitForFinancialAdvisorFirstOrder(brokerage));
        }

        [Test]
        public void NonFinancialAdvisorOrdersAreNeverHeldBack()
        {
            // a non advisor account opens the gate on initialization, no order ever waits
            using var brokerage = CreateBrokerage(financialAdvisor: false);

            Assert.IsFalse(WaitForFinancialAdvisorFirstOrder(brokerage));
            Assert.IsFalse(WaitForFinancialAdvisorFirstOrder(brokerage));
        }

        private static InteractiveBrokersBrokerage CreateBrokerage(bool financialAdvisor)
        {
            var brokerage = new InteractiveBrokersBrokerage();
            if (!financialAdvisor)
            {
                // Initialize() opens the gate for non advisor accounts, it is not run for a bare instance
                GetFirstOrderAnsweredEvent(brokerage).Set();
            }
            return brokerage;
        }

        private static bool WaitForFinancialAdvisorFirstOrder(InteractiveBrokersBrokerage brokerage)
        {
            return (bool)typeof(InteractiveBrokersBrokerage)
                .GetMethod("WaitForFinancialAdvisorFirstOrder", BindingFlags.NonPublic | BindingFlags.Instance)
                .Invoke(brokerage, null);
        }

        private static ManualResetEventSlim GetFirstOrderAnsweredEvent(InteractiveBrokersBrokerage brokerage)
        {
            return (ManualResetEventSlim)typeof(InteractiveBrokersBrokerage)
                .GetField("_financialAdvisorFirstOrderAnswered", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(brokerage);
        }
    }
}