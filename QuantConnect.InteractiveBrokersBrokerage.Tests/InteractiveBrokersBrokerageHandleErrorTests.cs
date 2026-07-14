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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    [TestFixture]
    public class InteractiveBrokersBrokerageHandleErrorTests
    {
        // error 300 ("Can't find EId with tickerId:N") is IB rejecting a cancelMktData for a ticker
        // it has no active subscription for. It must only be suppressed when it targets a market-data
        // ticker we have already unsubscribed (the async cancelMktData races the removal, common during
        // teardown); a 300 for a ticker we still believe is active must still surface to the user.
        [TestCase(false, 0, TestName = "HandleError_Code300_ForUnsubscribedTicker_IsIgnored")]
        [TestCase(true, 1, TestName = "HandleError_Code300_ForSubscribedTicker_IsSurfaced")]
        public void HandleErrorSuppressesCode300OnlyForUnsubscribedTickers(bool stillSubscribed, int expectedMessageCount)
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            const int tickerId = 12;

            // seed the request information for the ticker as a market-data subscription request
            var requestInformationType = typeof(InteractiveBrokersBrokerage).GetNestedType("RequestInformation", BindingFlags.NonPublic);
            var requestType = typeof(InteractiveBrokersBrokerage).GetNestedType("RequestType", BindingFlags.NonPublic);
            var requestInformation = Activator.CreateInstance(requestInformationType);
            requestInformationType.GetProperty("RequestId").SetValue(requestInformation, tickerId);
            requestInformationType.GetProperty("RequestType").SetValue(requestInformation, Enum.Parse(requestType, "Subscription"));
            requestInformationType.GetProperty("Message").SetValue(requestInformation, $"[Id={tickerId}] Subscribe: HSI (IND HSI HKD hkfe   0 )");
            ((IDictionary)GetPrivateFieldValue(brokerage, "_requestInformation"))[tickerId] = requestInformation;

            if (stillSubscribed)
            {
                // ticker still tracked: the 300 is unexpected and must be surfaced
                var subscriptionEntryType = typeof(InteractiveBrokersBrokerage).GetNestedType("SubscriptionEntry", BindingFlags.NonPublic);
                var subscriptionEntry = Activator.CreateInstance(subscriptionEntryType);
                ((IDictionary)GetPrivateFieldValue(brokerage, "_subscribedTickers"))[tickerId] = subscriptionEntry;
            }

            var messages = new List<BrokerageMessageEvent>();
            void OnMessage(object _, BrokerageMessageEvent message) => messages.Add(message);
            brokerage.Message += OnMessage;

            var errorEvent = new IB.ErrorEventArgs(
                id: tickerId,
                time: 0,
                code: 300,
                message: "Can't find EId with tickerId:12");
            brokerage.HandleError(this, errorEvent);

            brokerage.Message -= OnMessage;

            Assert.AreEqual(expectedMessageCount, messages.Count(m => m.Code == "300"));
        }

        // error 460 ("No trading permissions") references an order request, so it must invalidate the
        // order and release the thread waiting for the order response, otherwise the wait times out
        // five minutes later and stops the algorithm with 'Timeout waiting for brokerage response'.
        // See https://github.com/QuantConnect/Lean.Brokerages.InteractiveBrokers/issues/93
        [Test]
        public void HandleErrorCode460InvalidatesOrderAndReleasesPendingResponse()
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            const int ibOrderId = 2;

            var orderProvider = new OrderProvider();
            var order = new MarketOrder(Symbols.SPY, 23580, DateTime.UtcNow);
            orderProvider.Add(order);
            order.BrokerId.Add("2");
            typeof(InteractiveBrokersBrokerage)
                .GetField("_orderProvider", BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(brokerage, orderProvider);

            using var pendingResponseEvent = new ManualResetEventSlim(false);
            var pendingOrderResponses = (IDictionary)GetPrivateFieldValue(brokerage, "_pendingOrderResponse");
            pendingOrderResponses[ibOrderId] = pendingResponseEvent;

            List<BrokerageMessageEvent> messages = [];
            List<OrderEvent> orderEvents = [];
            brokerage.Message += (_, message) => messages.Add(message);
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            brokerage.HandleError(this, new IB.ErrorEventArgs(
                id: ibOrderId,
                time: 0,
                code: 460,
                message: "No trading permissions."));

            Assert.IsTrue(pendingResponseEvent.IsSet);
            Assert.IsFalse(pendingOrderResponses.Contains(ibOrderId));
            Assert.AreEqual(1, orderEvents.Count(e => e.OrderId == order.Id && e.Status == OrderStatus.Invalid));
            Assert.AreEqual(1, messages.Count(m => m.Code == "460" && m.Type == BrokerageMessageType.Warning));
        }

        private static object GetPrivateFieldValue(object instance, string name)
        {
            return instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(instance);
        }
    }
}
