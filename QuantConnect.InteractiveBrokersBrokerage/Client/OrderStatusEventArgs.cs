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

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Event arguments class for the <see cref="InteractiveBrokersClient.OrderStatus"/> event
    /// </summary>
    public sealed class OrderStatusEventArgs : EventArgs
    {
        /// <summary>
        /// The order Id that was specified previously in the call to placeOrder()
        /// </summary>
        public int OrderId { get; }

        /// <summary>
        /// The order status.
        /// </summary>
        public string Status { get; }

        /// <summary>
        /// Specifies the number of shares that have been executed.
        /// </summary>
        public int Filled { get; }

        /// <summary>
        /// Specifies the number of shares still outstanding.
        /// </summary>
        public int Remaining { get; }

        /// <summary>
        /// The average price of the shares that have been executed.
        /// This parameter is valid only if the filled parameter value is greater than zero.
        /// Otherwise, the price parameter will be zero.
        /// </summary>
        public double AverageFillPrice { get; }

        /// <summary>
        /// The TWS id used to identify orders. Remains the same over TWS sessions.
        /// </summary>
        public long PermId { get; }

        /// <summary>
        /// The order ID of the parent order, used for bracket and auto trailing stop orders.
        /// </summary>
        public int ParentId { get; }

        /// <summary>
        /// The last price of the shares that have been executed.
        /// This parameter is valid only if the filled parameter value is greater than zero.
        /// Otherwise, the price parameter will be zero.
        /// </summary>
        public double LastFillPrice { get; }

        /// <summary>
        /// The ID of the client (or TWS) that placed the order.
        /// Note that TWS orders have a fixed clientId and orderId of 0 that distinguishes them from API orders.
        /// </summary>
        public int ClientId { get; }

        /// <summary>
        /// This field is used to identify an order held when TWS is trying to locate shares for a short sell.
        /// The value used to indicate this is 'locate'.
        /// </summary>
        public string WhyHeld { get; }

        /// <summary>
        /// If an order has been capped, this indicates the current capped price.
        /// Requires TWS 967+ and API v973.04+. Python API specifically requires API v973.06+.
        /// </summary>
        public double MktCapPrice { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="OrderStatusEventArgs"/> class
        /// </summary>
        public OrderStatusEventArgs(int orderId, string status, int filled, int remaining, double averageFillPrice, long permId, int parentId, double lastFillPrice, int clientId, string whyHeld, double mktCapPrice)
        {
            OrderId = orderId;
            Status = status;
            Filled = filled;
            Remaining = remaining;
            AverageFillPrice = averageFillPrice;
            PermId = permId;
            ParentId = parentId;
            LastFillPrice = lastFillPrice;
            ClientId = clientId;
            WhyHeld = whyHeld;
            MktCapPrice = mktCapPrice;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"OrderId: {OrderId.ToStringInvariant()}, " +
                   $"Status: {Status}, " +
                   $"Filled: {Filled.ToStringInvariant()}, " +
                   $"Remaining: {Remaining.ToStringInvariant()}, " +
                   $"AverageFillPrice: {AverageFillPrice.ToStringInvariant()}, " +
                   $"PermId: {PermId.ToStringInvariant()}, " +
                   $"ParentId: {ParentId.ToStringInvariant()}, " +
                   $"LastFillPrice: {LastFillPrice.ToStringInvariant()}, " +
                   $"ClientId: {ClientId.ToStringInvariant()}, " +
                   $"WhyHeld: {WhyHeld}," +
                   $"MktCapPrice: {MktCapPrice}";
        }
    }
}