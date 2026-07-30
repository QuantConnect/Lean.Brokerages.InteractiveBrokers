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
using QuantConnect.Tests.Engine.DataFeeds;
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

        // errors rejecting an order request must invalidate the order and release the thread waiting
        // for the order response, otherwise the wait times out five minutes later and stops the
        // algorithm with 'Timeout waiting for brokerage response' instead of the actual reason.
        // - 200 ("No security definition", e.g. a delisted symbol), see https://github.com/QuantConnect/Lean.Brokerages.InteractiveBrokers/issues/25
        // - 460 ("No trading permissions"), see https://github.com/QuantConnect/Lean.Brokerages.InteractiveBrokers/issues/93
        [TestCase(200, "No security definition has been found for the request")]
        [TestCase(460, "No trading permissions.")]
        public void HandleErrorOrderRejectionInvalidatesOrderAndReleasesPendingResponse(int errorCode, string errorMessage)
        {
            using var brokerage = new InteractiveBrokersBrokerage();
            const int ibOrderId = 2;

            var orderProvider = new OrderProvider();
            var order = new MarketOrder(Symbols.SPY, 23580, DateTime.UtcNow);
            orderProvider.Add(order);
            order.BrokerId.Add("2");
            SetPrivateFieldValue(brokerage, "_orderProvider", orderProvider);

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
                code: errorCode,
                message: errorMessage));

            Assert.IsTrue(pendingResponseEvent.IsSet);
            Assert.IsFalse(pendingOrderResponses.Contains(ibOrderId));
            Assert.AreEqual(1, orderEvents.Count(e => e.OrderId == order.Id && e.Status == OrderStatus.Invalid));
            // with no algorithm attached the rejections are surfaced with Error severity, note that a 200 is
            // downgraded to a Warning when the algorithm has 'IgnoreUnknownAssetHoldings' enabled (the default)
            Assert.AreEqual(1, messages.Count(m => m.Code == errorCode.ToStringInvariant() && m.Type == BrokerageMessageType.Error));
        }

        // the 200 message is deduped per symbol when 'IgnoreUnknownAssetHoldings' is enabled (the default), for
        // instance the market data subscription of a delisted symbol is rejected with a 200 before any order for
        // it is placed. That dedup must not skip the order invalidation: it left the pending order response
        // unreleased, so an algorithm holding a symbol IB cannot resolve was still stopped by the response
        // timeout with 'Timeout waiting for brokerage response' instead of getting the rejection on the order.
        [Test]
        public void HandleErrorInvalidatesEveryOrderRejectedWithNoSecurityDefinition()
        {
            const string errorMessage = "No security definition has been found for the request";

            using var brokerage = new InteractiveBrokersBrokerage();
            var algorithm = new AlgorithmStub();
            Assert.IsTrue(algorithm.Settings.IgnoreUnknownAssetHoldings, "the message dedup under test requires this setting enabled");
            SetPrivateFieldValue(brokerage, "_algorithm", algorithm);

            var orderProvider = new OrderProvider();
            SetPrivateFieldValue(brokerage, "_orderProvider", orderProvider);

            List<BrokerageMessageEvent> messages = [];
            List<OrderEvent> orderEvents = [];
            brokerage.Message += (_, message) => messages.Add(message);
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            // the market data subscription for the symbol is rejected first, registering it as an unsupported asset
            const int tickerId = 1;
            SeedRequestInformation(brokerage, tickerId, "Subscription", Symbols.SPY);
            brokerage.HandleError(this, new IB.ErrorEventArgs(id: tickerId, time: 0, code: 200, message: errorMessage));

            Assert.IsEmpty(orderEvents, "the market data request rejection is not an order rejection");

            // now every order for that same symbol gets rejected with the very same error
            var pendingOrderResponses = (IDictionary)GetPrivateFieldValue(brokerage, "_pendingOrderResponse");
            for (var ibOrderId = 2; ibOrderId <= 3; ibOrderId++)
            {
                var order = new MarketOrder(Symbols.SPY, 1, DateTime.UtcNow);
                order.BrokerId.Add(ibOrderId.ToStringInvariant());
                orderProvider.Add(order);

                using var pendingResponseEvent = new ManualResetEventSlim(false);
                pendingOrderResponses[ibOrderId] = pendingResponseEvent;
                SeedRequestInformation(brokerage, ibOrderId, "PlaceOrder", Symbols.SPY);

                orderEvents.Clear();
                brokerage.HandleError(this, new IB.ErrorEventArgs(id: ibOrderId, time: 0, code: 200, message: errorMessage));

                Assert.IsTrue(pendingResponseEvent.IsSet, $"the pending response of order {ibOrderId} was not released");
                Assert.IsFalse(pendingOrderResponses.Contains(ibOrderId));
                Assert.AreEqual(1, orderEvents.Count(e => e.Status == OrderStatus.Invalid), $"order {ibOrderId} was not invalidated");
                StringAssert.StartsWith($"200 - {errorMessage}", orderEvents.Single().Message);
            }

            // the user facing message is still emitted a single time for the symbol
            Assert.AreEqual(1, messages.Count(m => m.Code == "200"));
            Assert.AreEqual(BrokerageMessageType.Warning, messages.Single(m => m.Code == "200").Type);
        }

        // an error answering a request that is not an order cannot be an order rejection, so it must not
        // invalidate anything: that covers error 200 for contract details or market data requests, and the
        // market data and history codes that are in InvalidatingCodes too (10002, 10006-10012, 10014, ...).
        // An error for a request we are not tracking (e.g. an order reconstructed from IB's open orders, whose
        // id was assigned in a previous session) is still invalidated, as it was before.
        [TestCase(200, "Subscription", false, TestName = "HandleError_Code200_OnNonOrderRequest_DoesNotInvalidate")]
        [TestCase(201, "Subscription", false, TestName = "HandleError_Code201_OnNonOrderRequest_DoesNotInvalidate")]
        [TestCase(10008, "History", false, TestName = "HandleError_Code10008_OnHistoryRequest_DoesNotInvalidate")]
        [TestCase(200, "PlaceOrder", true, TestName = "HandleError_Code200_OnOrderRequest_Invalidates")]
        [TestCase(201, "CancelOrder", true, TestName = "HandleError_Code201_OnCancelRequest_Invalidates")]
        [TestCase(201, null, true, TestName = "HandleError_Code201_WithoutRequestInformation_Invalidates")]
        public void HandleErrorInvalidatesOnlyErrorsAnsweringOrderRequests(int errorCode, string requestType, bool expectedInvalidation)
        {
            const int requestId = 7;

            using var brokerage = new InteractiveBrokersBrokerage();

            var orderProvider = new OrderProvider();
            var order = new MarketOrder(Symbols.SPY, 1, DateTime.UtcNow);
            order.BrokerId.Add(requestId.ToStringInvariant());
            orderProvider.Add(order);
            SetPrivateFieldValue(brokerage, "_orderProvider", orderProvider);

            using var pendingResponseEvent = new ManualResetEventSlim(false);
            var pendingOrderResponses = (IDictionary)GetPrivateFieldValue(brokerage, "_pendingOrderResponse");
            pendingOrderResponses[requestId] = pendingResponseEvent;
            if (requestType != null)
            {
                SeedRequestInformation(brokerage, requestId, requestType, Symbols.SPY);
            }

            List<OrderEvent> orderEvents = [];
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            brokerage.HandleError(this, new IB.ErrorEventArgs(id: requestId, time: 0, code: errorCode, message: "some rejection"));

            Assert.AreEqual(expectedInvalidation, pendingResponseEvent.IsSet);
            Assert.AreEqual(expectedInvalidation ? 1 : 0, orderEvents.Count(e => e.Status == OrderStatus.Invalid));
        }

        // with 'IgnoreUnknownAssetHoldings' disabled the 200 is not downgraded to a warning, so on top of
        // invalidating the order the rejection is surfaced with Error severity, which stops the algorithm
        [Test]
        public void HandleErrorSurfacesNoSecurityDefinitionAsErrorWhenNotIgnoringUnknownAssetHoldings()
        {
            const int ibOrderId = 2;

            using var brokerage = new InteractiveBrokersBrokerage();
            var algorithm = new AlgorithmStub();
            algorithm.Settings.IgnoreUnknownAssetHoldings = false;
            SetPrivateFieldValue(brokerage, "_algorithm", algorithm);

            var orderProvider = new OrderProvider();
            var order = new MarketOrder(Symbols.SPY, 1, DateTime.UtcNow);
            order.BrokerId.Add(ibOrderId.ToStringInvariant());
            orderProvider.Add(order);
            SetPrivateFieldValue(brokerage, "_orderProvider", orderProvider);

            using var pendingResponseEvent = new ManualResetEventSlim(false);
            var pendingOrderResponses = (IDictionary)GetPrivateFieldValue(brokerage, "_pendingOrderResponse");
            pendingOrderResponses[ibOrderId] = pendingResponseEvent;
            SeedRequestInformation(brokerage, ibOrderId, "PlaceOrder", Symbols.SPY);

            List<BrokerageMessageEvent> messages = [];
            List<OrderEvent> orderEvents = [];
            brokerage.Message += (_, message) => messages.Add(message);
            brokerage.OrdersStatusChanged += (_, events) => orderEvents.AddRange(events);

            brokerage.HandleError(this, new IB.ErrorEventArgs(
                id: ibOrderId,
                time: 0,
                code: 200,
                message: "No security definition has been found for the request"));

            Assert.IsTrue(pendingResponseEvent.IsSet);
            Assert.AreEqual(1, orderEvents.Count(e => e.Status == OrderStatus.Invalid));
            Assert.AreEqual(BrokerageMessageType.Error, messages.Single(m => m.Code == "200").Type);
        }

        private static void SeedRequestInformation(InteractiveBrokersBrokerage brokerage, int requestId, string requestType, Symbol symbol)
        {
            var requestInformationType = typeof(InteractiveBrokersBrokerage).GetNestedType("RequestInformation", BindingFlags.NonPublic);
            var requestTypeType = typeof(InteractiveBrokersBrokerage).GetNestedType("RequestType", BindingFlags.NonPublic);
            var requestInformation = Activator.CreateInstance(requestInformationType);
            requestInformationType.GetProperty("RequestId").SetValue(requestInformation, requestId);
            requestInformationType.GetProperty("RequestType").SetValue(requestInformation, Enum.Parse(requestTypeType, requestType));
            requestInformationType.GetProperty("AssociatedSymbol").SetValue(requestInformation, symbol);
            requestInformationType.GetProperty("Message").SetValue(requestInformation, $"[Id={requestId}] {requestType}: {symbol.Value}");
            ((IDictionary)GetPrivateFieldValue(brokerage, "_requestInformation"))[requestId] = requestInformation;
        }

        private static void SetPrivateFieldValue(object instance, string name, object value)
        {
            instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(instance, value);
        }

        private static object GetPrivateFieldValue(object instance, string name)
        {
            return instance.GetType()
                .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(instance);
        }
    }
}
