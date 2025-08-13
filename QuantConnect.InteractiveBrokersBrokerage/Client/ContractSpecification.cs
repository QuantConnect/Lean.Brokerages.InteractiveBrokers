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

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Describes a tradable instrument's trading class and minimum tick size,
    /// as provided by an Interactive Brokers <c>reqContractDetails(...)</c> request.
    /// </summary>
    public class ContractSpecification
    {
        /// <summary>
        /// Gets the IB trading class of the instrument.
        /// For example, for Apple stock options this value may be <c>AAPL</c>.
        /// </summary>
        public string TradingClass { get; }

        /// <summary>
        /// Gets the minimum price increment (tick size) for the instrument.
        /// For example, <c>0.01</c> means prices move in 1-cent increments.
        /// </summary>
        public decimal MinTick { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ContractSpecification"/> class.
        /// </summary>
        /// <param name="tradingClass">
        /// The IB trading class of the instrument, identifying its group or symbol root.
        /// </param>
        /// <param name="minTick">
        /// The smallest allowed price increment for this instrument.
        /// </param>
        public ContractSpecification(string tradingClass, decimal minTick)
        {
            TradingClass = tradingClass;
            MinTick = minTick;
        }
    }
}
