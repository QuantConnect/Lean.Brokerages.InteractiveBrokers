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

using IBApi;
using NodaTime;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.IBAutomater;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Securities.FutureOption;
using QuantConnect.Securities.Index;
using QuantConnect.Securities.IndexOption;
using QuantConnect.Securities.Option;
using QuantConnect.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Api;
using RestSharp;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using Bar = QuantConnect.Data.Market.Bar;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Securities.Forex;
using QuantConnect.Lean.Engine.Results;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("QuantConnect.Tests.Brokerages.InteractiveBrokers")]

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// The Interactive Brokers brokerage
    /// </summary>
    [BrokerageFactory(typeof(InteractiveBrokersBrokerageFactory))]
    public sealed class InteractiveBrokersBrokerage : Brokerage, IDataQueueHandler, IDataQueueUniverseProvider
    {
        /// <summary>
        /// The name of the brokerage.
        /// </summary>
        private const string BrokerageName = "Interactive Brokers Brokerage";

        /// <summary>
        /// During market open there can be some extra delay and resource constraint so let's be generous
        /// </summary>
        private static readonly TimeSpan _responseTimeout = TimeSpan.FromSeconds(Config.GetInt("ib-response-timeout", 60 * 5));

        /// <summary>
        /// Some orders might not send a submission event back to us, in which case we create a submission event ourselves
        /// </summary>
        /// <remarks>I've seen combo limit order take up to 5 seconds to be trigger a submission event</remarks>
        private static readonly TimeSpan _noSubmissionOrdersResponseTimeout = TimeSpan.FromSeconds(Config.GetInt("ib-no-submission-orders-response-timeout", 10));
        private static bool _submissionOrdersWarningSent;
        private bool _sentFAOrderPropertiesWarning;

        private readonly HashSet<OrderType> _noSubmissionOrderTypes = new(new[] {
            OrderType.MarketOnOpen,
            OrderType.ComboLegLimit,
            // combo market & limit do not send submission event when trading only options
            OrderType.ComboMarket,
            OrderType.ComboLimit
        });

        /// <summary>
        /// The maximum time to wait for all fills in a combo order before we start emitting the fill events
        /// </summary>
        private static readonly TimeSpan _comboOrderFillTimeout = TimeSpan.FromSeconds(Config.GetInt("ib-combo-order-fill-timeout", 30));

        /// <summary>
        /// The default gateway version to use
        /// </summary>
        public static string DefaultVersion { get; } = "1034";

        private IBAutomater.IBAutomater _ibAutomater;

        // Existing orders created in TWS can *only* be cancelled/modified when connected with ClientId = 0
        private const int ClientId = 0;

        // daily restart is at 23:45 local host time
        private static TimeSpan _heartBeatTimeLimit = new(23, 0, 0);

        // next valid order id (or request id, or ticker id) for this client
        private int _nextValidId;

        private readonly object _nextValidIdLocker = new object();

        private CancellationTokenSource _gatewayRestartTokenSource;

        private int _port;
        private string _account;
        private string _host;
        private IAlgorithm _algorithm;
        private bool _loadExistingHoldings;
        private IOrderProvider _orderProvider;
        private IMapFileProvider _mapFileProvider;
        private IDataAggregator _aggregator;
        private IB.InteractiveBrokersClient _client;

        private string _agentDescription;
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        private Thread _messageProcessingThread;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private ManualResetEvent _currentTimeEvent = new ManualResetEvent(false);
        private Thread _heartBeatThread;

        // Notifies the thread reading information from Gateway/TWS whenever there are messages ready to be consumed
        private readonly EReaderSignal _signal = new EReaderMonitorSignal();

        private readonly ManualResetEvent _connectEvent = new ManualResetEvent(false);
        private readonly ManualResetEvent _waitForNextValidId = new ManualResetEvent(false);
        private readonly ManualResetEvent _accountHoldingsResetEvent = new ManualResetEvent(false);
        private Exception _accountHoldingsLastException;

        /// <summary>
        /// Stores skipped orders whose FA group does not match the configured filter.
        /// Key is the order ID; value is the FA group associated with the order.
        /// </summary>
        private readonly ConcurrentDictionary<int, string> _skippedOrdersByFaGroup = new();

        // tracks pending brokerage order responses. In some cases we've seen orders been placed and they never get through to IB
        private readonly ConcurrentDictionary<int, ManualResetEventSlim> _pendingOrderResponse = new();

        // tracks the pending orders in the group before emitting the fill events
        private readonly Dictionary<int, List<PendingFillEvent>> _pendingGroupOrdersForFilling = new();

        private readonly ComboOrdersFillTimeoutMonitor _comboOrdersFillTimeoutMonitor = new(RealTimeProvider.Instance, _comboOrderFillTimeout);

        private readonly ConcurrentDictionary<int, StopLimitOrder> _preSubmittedStopLimitOrders = new();

        /// <summary>
        /// Provides a thread-safe service for caching and managing original orders when they are part of a group.
        /// </summary>
        private GroupOrderCacheManager _groupOrderCacheManager = new();

        // tracks requested order updates, so we can flag Submitted order events as updates
        private readonly ConcurrentDictionary<int, int> _orderUpdates = new ConcurrentDictionary<int, int>();

        // tracks executions before commission reports, map: execId -> execution
        private readonly ConcurrentDictionary<string, IB.ExecutionDetailsEventArgs> _orderExecutions = new ConcurrentDictionary<string, IB.ExecutionDetailsEventArgs>();

        // tracks commission reports before executions, map: execId -> commission report
        private readonly ConcurrentDictionary<string, CommissionAndFeesReport> _commissionReports = new ConcurrentDictionary<string, CommissionAndFeesReport>();

        // holds account properties, cash balances and holdings for the account
        private readonly InteractiveBrokersAccountData _accountData = new InteractiveBrokersAccountData();

        // holds brokerage state information (connection status, error conditions, etc.)
        private readonly InteractiveBrokersStateManager _stateManager = new InteractiveBrokersStateManager();

        private readonly object _sync = new object();

        private readonly ConcurrentDictionary<string, ContractDetails> _contractDetails = new ConcurrentDictionary<string, ContractDetails>();

        private InteractiveBrokersSymbolMapper _symbolMapper;
        private readonly HashSet<string> _invalidContracts = new();

        // IB TWS may adjust the contract details to define the combo orders more accurately, so we keep track of them in order to be able
        // to update these orders without getting error 105 ("Order being modified does not match original order").
        // https://groups.io/g/twsapi/topic/5333246?p=%2C%2C%2C20%2C0%2C0%2C0%3A%3A%2C%2C%2C0%2C0%2C0%2C5333246
        private ConcurrentDictionary<long, Contract> _comboOrdersContracts = new();

        // Prioritized list of exchanges used to find right futures contract
        private static readonly Dictionary<string, string> _futuresExchanges = new Dictionary<string, string>
        {
            { Market.CME, "CME" },
            { Market.NYMEX, "NYMEX" },
            { Market.COMEX, "COMEX" },
            { Market.CBOT, "CBOT" },
            { Market.ICE, "NYBOT" },
            { Market.CFE, "CFE" },
            { Market.NYSELIFFE, "NYSELIFFE" },
            { Market.EUREX, "EUREX" }
        };

        private static readonly SymbolPropertiesDatabase _symbolPropertiesDatabase = SymbolPropertiesDatabase.FromDataFolder();

        /// <summary>
        /// Provides primary exchange data based on LEAN map files.
        /// </summary>
        private MapFilePrimaryExchangeProvider _exchangeProvider;

        // exchange time zones by symbol
        private readonly Dictionary<Symbol, SecurityExchangeHours> _symbolExchangeHours = [];

        // IB requests made through the IB-API must be limited to a maximum of 50 messages/3 second
        private readonly RateGate _messagingRateLimiter = new RateGate(50, TimeSpan.FromSeconds(3));

        // See https://interactivebrokers.github.io/tws-api/historical_limitations.html
        // Making more than 60 requests within any ten minute period will cause a pacing violation for Small Bars (30 secs or less)
        private readonly RateGate _historyHighResolutionRateLimiter = new(58, TimeSpan.FromMinutes(10));
        // The maximum number of simultaneous open historical data requests from the API is 50, we limit the count further so we can server them as best as possible
        private readonly SemaphoreSlim _concurrentHistoryRequests = new(20);

        // additional IB request information, will be matched with errors in the handler, for better error reporting
        private readonly ConcurrentDictionary<int, RequestInformation> _requestInformation = new();

        /// <summary>
        /// Provides cached access to Interactive Brokers contract specifications,
        /// including trading class and minimum tick size, to reduce duplicate <c>reqContractDetails</c> requests.
        /// </summary>
        internal IB.ContractSpecificationService _contractSpecificationService;

        // when unsubscribing symbols immediately after subscribing IB returns an error (Can't find EId with tickerId:nnn),
        // so we track subscription times to ensure symbols are not unsubscribed before a minimum time span has elapsed
        private readonly Dictionary<int, DateTime> _subscriptionTimes = new Dictionary<int, DateTime>();

        private readonly TimeSpan _minimumTimespanBeforeUnsubscribe = TimeSpan.FromSeconds(5);

        private readonly bool _enableDelayedStreamingData = Config.GetBool("ib-enable-delayed-streaming-data");

        // The UTC time at which IBAutomater should be restarted and 2FA confirmation should be requested on Sundays (IB's weekly restart)
        private TimeSpan _weeklyRestartUtcTime;
        private DateTime _lastIBAutomaterExitTime;
        private readonly object _lastIBAutomaterExitTimeLock = new object();

        private volatile bool _isDisposeCalled;
        private bool _isInitialized;
        private bool _pastFirstConnection;

        private bool _historyHighResolutionRateLimitWarning;
        private bool _historySecondResolutionWarning;
        private bool _historyDelistedAssetWarning;
        private bool _historyExpiredAssetWarning;
        private bool _historyOpenInterestWarning;
        private bool _historyCfdTradeWarning;
        private bool _historyInvalidPeriodWarning;

        /// <summary>
        /// Tracks whether a warning about safe MarketOnOpen execution has already been sent.
        /// </summary>
        /// <remarks>Ensures the warning is raised only once per application run to avoid spamming messages.</remarks>
        private bool _hasWarnedSafeMooExecution;

        // Symbols that IB doesn't support ("No security definition has been found for the request")
        // We keep track of them to avoid flooding the logs with the same error/warning
        private readonly HashSet<string> _unsupportedAssets = new();

        /// <summary>
        /// Represents the allocation group managed by financial advisors.
        /// </summary>
        /// <remarks>
        /// The specific Advisor Account Group name that has already been created in TWS Global Configuration.
        /// </remarks>
        private static string _financialAdvisorsGroupFilter;

        /// <summary>
        /// Represents the next local market open time after which the first 'lastPrice' tick for the NDX index should be skipped.
        /// This is used to ensure only the initial tick after market open is ignored each trading day.
        /// </summary>
        internal static DateTime _nextNdxMarketOpenSkipTime = default;

        /// <summary>
        /// Stores the exchange hours for the NDX security, used to determine market open/close times and related calculations.
        /// </summary>
        private static SecurityExchangeHours _ndxSecurityExchangeHours;

        /// <summary>
        /// Returns true if we're currently connected to the broker
        /// </summary>
        public override bool IsConnected => _client != null && _client.Connected && !_stateManager.Disconnected1100Fired && !_stateManager.IsConnecting;

        /// <summary>
        /// Returns true if the connected user is a financial advisor
        /// </summary>
        public bool IsFinancialAdvisor => IsMasterAccount(_account);

        /// <summary>
        /// Returns true if the account is a financial advisor master account
        /// </summary>
        /// <param name="account">The account code</param>
        /// <returns>True if the account is a master account</returns>
        public static bool IsMasterAccount(string account)
        {
            return account.Contains("F");
        }

        /// <summary>
        /// Creates a new InteractiveBrokersBrokerage using values from configuration
        /// </summary>
        public InteractiveBrokersBrokerage() : base(BrokerageName)
        {
        }

        /// <summary>
        /// Creates a new InteractiveBrokersBrokerage using values from configuration:
        ///     ib-account (required)
        ///     ib-host (optional, defaults to LOCALHOST)
        ///     ib-port (optional, defaults to 4001)
        ///     ib-agent-description (optional, defaults to Individual)
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        /// <param name="orderProvider">An instance of IOrderProvider used to fetch Order objects by brokerage ID</param>
        /// <param name="securityProvider">The security provider used to give access to algorithm securities</param>
        public InteractiveBrokersBrokerage(IAlgorithm algorithm, IOrderProvider orderProvider, ISecurityProvider securityProvider)
            : this(algorithm, orderProvider, securityProvider, Config.Get("ib-account"))
        {
        }

        /// <summary>
        /// Creates a new InteractiveBrokersBrokerage for the specified account
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        /// <param name="orderProvider">An instance of IOrderProvider used to fetch Order objects by brokerage ID</param>
        /// <param name="securityProvider">The security provider used to give access to algorithm securities</param>
        /// <param name="account">The account used to connect to IB</param>
        public InteractiveBrokersBrokerage(IAlgorithm algorithm, IOrderProvider orderProvider, ISecurityProvider securityProvider, string account)
            : this(
                algorithm,
                orderProvider,
                securityProvider,
                account,
                Config.Get("ib-host", "LOCALHOST"),
                Config.GetInt("ib-port", 4001),
                Config.Get("ib-tws-dir"),
                Config.Get("ib-version", DefaultVersion),
                Config.Get("ib-user-name"),
                Config.Get("ib-password"),
                Config.Get("ib-trading-mode"),
                Config.GetValue("ib-agent-description", IB.AgentDescription.Individual),
                financialAdvisorsGroupFilter: Config.Get("ib-financial-advisors-group-filter")
                )
        {
        }

        /// <summary>
        /// Creates a new InteractiveBrokersBrokerage from the specified values
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        /// <param name="orderProvider">An instance of IOrderProvider used to fetch Order objects by brokerage ID</param>
        /// <param name="securityProvider">The security provider used to give access to algorithm securities</param>
        /// <param name="account">The Interactive Brokers account name</param>
        /// <param name="host">host name or IP address of the machine where TWS is running. Leave blank to connect to the local host.</param>
        /// <param name="port">must match the port specified in TWS on the Configure&gt;API&gt;Socket Port field.</param>
        /// <param name="ibDirectory">The IB Gateway root directory</param>
        /// <param name="ibVersion">The IB Gateway version</param>
        /// <param name="userName">The login user name</param>
        /// <param name="password">The login password</param>
        /// <param name="tradingMode">The trading mode: 'live' or 'paper'</param>
        /// <param name="agentDescription">Used for Rule 80A describes the type of trader.</param>
        /// <param name="loadExistingHoldings">False will ignore existing security holdings from being loaded.</param>
        /// <param name="weeklyRestartUtcTime">The UTC time at which IBAutomater should be restarted and 2FA confirmation should be requested on Sundays (IB's weekly restart)</param>
        /// <param name="financialAdvisorsGroupFilter">The name of the financial advisors group filter associated with this client.</param>
        public InteractiveBrokersBrokerage(
            IAlgorithm algorithm,
            IOrderProvider orderProvider,
            ISecurityProvider securityProvider,
            string account,
            string host,
            int port,
            string ibDirectory,
            string ibVersion,
            string userName,
            string password,
            string tradingMode,
            string agentDescription = IB.AgentDescription.Individual,
            bool loadExistingHoldings = true,
            TimeSpan? weeklyRestartUtcTime = null,
            string financialAdvisorsGroupFilter = default)
            : base(BrokerageName)
        {
            Initialize(
                algorithm,
                orderProvider,
                securityProvider,
                account,
                host,
                port,
                ibDirectory,
                ibVersion,
                userName,
                password,
                tradingMode,
                agentDescription,
                loadExistingHoldings,
                weeklyRestartUtcTime,
                financialAdvisorsGroupFilter);
        }

        /// <summary>
        /// Provides public access to the underlying IBClient instance
        /// </summary>
        public IB.InteractiveBrokersClient Client => _client;

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            try
            {
                if (!IsConnected)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.PlaceOrder(): Symbol: {order.Symbol.Value} Quantity: {order.Quantity}. Id: {order.Id}");
                    OnMessage(
                        new BrokerageMessageEvent(
                            BrokerageMessageType.Warning,
                            "PlaceOrderWhenDisconnected",
                            "Orders cannot be submitted when disconnected."));
                    return false;
                }

                IBPlaceOrder(order, true);
                return true;
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.PlaceOrder(): " + err);
                return false;
            }
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            try
            {
                Log.Trace($"InteractiveBrokersBrokerage.UpdateOrder(): Symbol: {order.Symbol.Value} Quantity: {order.Quantity} Status: {order.Status} Id: {order.Id}");

                if (!IsConnected)
                {
                    OnMessage(
                        new BrokerageMessageEvent(
                            BrokerageMessageType.Warning,
                            "UpdateOrderWhenDisconnected",
                            "Orders cannot be updated when disconnected."));
                    return false;
                }

                _orderUpdates[order.Id] = order.Id;
                IBPlaceOrder(order, false);
            }
            catch (Exception err)
            {
                int id;
                _orderUpdates.TryRemove(order.Id, out id);
                Log.Error("InteractiveBrokersBrokerage.UpdateOrder(): " + err);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            try
            {
                Log.Trace("InteractiveBrokersBrokerage.CancelOrder(): Symbol: " + order.Symbol.Value + " Quantity: " + order.Quantity);

                if (!IsConnected)
                {
                    OnMessage(
                        new BrokerageMessageEvent(
                            BrokerageMessageType.Warning,
                            "CancelOrderWhenDisconnected",
                            "Orders cannot be cancelled when disconnected."));
                    return false;
                }

                // this could be better
                foreach (var id in order.BrokerId)
                {
                    var orderId = Parse.Int(id);

                    _requestInformation[orderId] = new RequestInformation
                    {
                        RequestId = orderId,
                        RequestType = RequestType.CancelOrder,
                        AssociatedSymbol = order.Symbol,
                        Message = $"[Id={orderId}] CancelOrder: " + order
                    };

                    CheckRateLimiting();

                    var eventSlim = new ManualResetEventSlim(false);
                    _pendingOrderResponse[orderId] = eventSlim;

                    _client.ClientSocket.cancelOrder(orderId, new OrderCancel());

                    if (!eventSlim.Wait(_responseTimeout))
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "NoBrokerageResponse", $"Timeout waiting for brokerage response for brokerage order id {orderId} lean id {order.Id}"));
                    }
                    else
                    {
                        eventSlim.DisposeSafely();
                    }
                }

                // canceled order events fired upon confirmation, see HandleError
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.CancelOrder(): OrderID: " + order.Id + " - " + err);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Gets all open orders on the account
        /// </summary>
        /// <returns>The open orders returned from IB</returns>
        public override List<Order> GetOpenOrders()
        {
            // If client 0 invokes reqOpenOrders, it will cause currently open orders placed from TWS manually to be 'bound',
            // i.e. assigned an order ID so that they can be modified or cancelled by the API client 0.
            GetOpenOrdersInternal(false);

            // return all open orders (including those placed from TWS, which will have a negative order id)
            lock (_nextValidIdLocker)
            {
                return GetOpenOrdersInternal(true);
            }
        }

        private List<Order> GetOpenOrdersInternal(bool all)
        {
            var orders = new List<(IBApi.Order Order, Contract Contract, OrderState OrderState)>();

            var manualResetEvent = new ManualResetEvent(false);

            Exception exception = null;
            var lastOrderId = 0;

            // define our handlers
            EventHandler<IB.OpenOrderEventArgs> clientOnOpenOrder = (sender, args) =>
            {
                try
                {
                    if (args.OrderId > lastOrderId)
                    {
                        lastOrderId = args.OrderId;
                    }

                    if (TrySkipOrderByFaGroup(args.OrderId, args.Order.FaGroup))
                    {
                        return;
                    }

                    // keep the IB order, contract objects and order state returned from RequestOpenOrders
                    orders.Add((args.Order, args.Contract, args.OrderState));
                }
                catch (Exception e)
                {
                    exception = e;
                }
            };
            EventHandler clientOnOpenOrderEnd = (sender, args) =>
            {
                // this signals the end of our RequestOpenOrders call
                manualResetEvent.Set();
            };

            _client.OpenOrder += clientOnOpenOrder;
            _client.OpenOrderEnd += clientOnOpenOrderEnd;

            CheckRateLimiting();

            if (all)
            {
                _client.ClientSocket.reqAllOpenOrders();
            }
            else
            {
                _client.ClientSocket.reqOpenOrders();
            }

            // wait for our end signal
            var timedOut = !manualResetEvent.WaitOne(15000);

            // remove our handlers
            _client.OpenOrder -= clientOnOpenOrder;
            _client.OpenOrderEnd -= clientOnOpenOrderEnd;

            if (exception != null)
            {
                throw new Exception("InteractiveBrokersBrokerage.GetOpenOrders(): ", exception);
            }

            if (timedOut)
            {
                throw new TimeoutException("InteractiveBrokersBrokerage.GetOpenOrders(): Operation took longer than 15 seconds.");
            }

            if (all)
            {
                // https://interactivebrokers.github.io/tws-api/order_submission.html
                // if the function reqAllOpenOrders is used by a client, subsequent orders placed by that client
                // must have order IDs greater than the order IDs of all orders returned because of that function call.

                if (lastOrderId >= _nextValidId)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.GetOpenOrders(): Updating nextValidId from {_nextValidId} to {lastOrderId + 1}");
                    _nextValidId = lastOrderId + 1;
                }
            }

            // convert results to Lean Orders outside the eventhandler to avoid nesting requests, as conversion may request
            // contract details
            return orders.Select(orderContract => ConvertOrders(orderContract.Order, orderContract.Contract, orderContract.OrderState)).SelectMany(orders => orders).ToList();
        }

        private Contract GetOpenOrderContract(int orderId)
        {
            Contract contract = null;
            var manualResetEvent = new ManualResetEvent(false);

            // define our handlers
            EventHandler<IB.OpenOrderEventArgs> clientOnOpenOrder = (sender, args) =>
            {
                if (args.OrderId == orderId)
                {
                    contract = args.Contract;
                    manualResetEvent.Set();
                }
            };
            EventHandler clientOnOpenOrderEnd = (sender, args) =>
            {
                // this signals the end of our RequestOpenOrders call
                manualResetEvent.Set();
            };

            _client.OpenOrder += clientOnOpenOrder;
            _client.OpenOrderEnd += clientOnOpenOrderEnd;

            CheckRateLimiting();

            _client.ClientSocket.reqOpenOrders();

            // wait for our end signal
            var timedOut = !manualResetEvent.WaitOne(15000);

            // remove our handlers
            _client.OpenOrder -= clientOnOpenOrder;
            _client.OpenOrderEnd -= clientOnOpenOrderEnd;

            if (timedOut)
            {
                throw new TimeoutException("InteractiveBrokersBrokerage.GetOpenOrders(): Operation took longer than 15 seconds.");
            }

            return contract;
        }

        /// <summary>
        /// Gets all holdings for the account
        /// </summary>
        /// <returns>The current holdings from the account</returns>
        public override List<Holding> GetAccountHoldings()
        {
            if (!IsConnected)
            {
                Log.Trace("InteractiveBrokersBrokerage.GetAccountHoldings(): not connected, connecting now");
                Connect();
            }

            var utcNow = DateTime.UtcNow;
            var holdings = new List<Holding>();

            foreach (var kvp in _accountData.AccountHoldings.Values)
            {
                var holding = ObjectActivator.Clone(kvp.Holding);

                if (holding.Quantity != 0)
                {
                    if (OptionSymbol.IsOptionContractExpired(holding.Symbol, utcNow))
                    {
                        OnMessage(
                            new BrokerageMessageEvent(
                                BrokerageMessageType.Warning,
                                "ExpiredOptionHolding",
                                $"The option holding for [{holding.Symbol.Value}] is expired and will be excluded from the account holdings."));

                        continue;
                    }

                    holdings.Add(holding);
                }
            }

            // Prevent holdings calculation every time we receive portfolio updates from IB
            _loadExistingHoldings = false;

            return holdings;
        }

        /// <summary>
        /// Gets the current cash balance for each currency held in the brokerage account
        /// </summary>
        /// <returns>The current cash balance for each currency available for trading</returns>
        public override List<CashAmount> GetCashBalance()
        {
            if (!IsConnected)
            {
                if (_ibAutomater.IsWithinScheduledServerResetTimes())
                {
                    // Occasionally the disconnection due to the IB reset period might last
                    // much longer than expected during weekends (even up to the cash sync time).
                    // In this case we do not try to reconnect (since this would fail anyway)
                    // but we return the existing balances instead.
                    Log.Trace("InteractiveBrokersBrokerage.GetCashBalance(): not connected within reset times, returning existing balances");
                }
                else
                {
                    Log.Trace("InteractiveBrokersBrokerage.GetCashBalance(): not connected, connecting now");
                    Connect();
                }
            }

            var balances = _accountData.CashBalances.Select(x => new CashAmount(x.Value, x.Key)).ToList();

            if (balances.Count == 0)
            {
                Log.Trace($"InteractiveBrokersBrokerage.GetCashBalance(): no balances found, IsConnected: {IsConnected}, _disconnected1100Fired: {_stateManager.Disconnected1100Fired}");
            }

            return balances;
        }

        /// <summary>
        /// Gets the execution details matching the filter
        /// </summary>
        /// <returns>A list of executions matching the filter</returns>
        public List<IB.ExecutionDetailsEventArgs> GetExecutions(string symbol, string type, string exchange, DateTime? timeSince, string side)
        {
            var filter = new ExecutionFilter
            {
                AcctCode = _account,
                ClientId = ClientId,
                Exchange = exchange,
                SecType = type ?? IB.SecurityType.Undefined,
                Symbol = symbol,
                Time = (timeSince ?? DateTime.MinValue).ToStringInvariant("yyyyMMdd HH:mm:ss"),
                Side = side ?? IB.ActionSide.Undefined
            };

            var details = new List<IB.ExecutionDetailsEventArgs>();

            var manualResetEvent = new ManualResetEvent(false);

            var requestId = GetNextId();

            _requestInformation[requestId] = new RequestInformation
            {
                RequestId = requestId,
                RequestType = RequestType.Executions,
                Message = $"[Id={requestId}] GetExecutions: " + symbol
            };

            // define our event handlers
            EventHandler<IB.RequestEndEventArgs> clientOnExecutionDataEnd = (sender, args) =>
            {
                if (args.RequestId == requestId) manualResetEvent.Set();
            };
            EventHandler<IB.ExecutionDetailsEventArgs> clientOnExecDetails = (sender, args) =>
            {
                if (args.RequestId == requestId) details.Add(args);
            };

            _client.ExecutionDetails += clientOnExecDetails;
            _client.ExecutionDetailsEnd += clientOnExecutionDataEnd;

            CheckRateLimiting();

            // no need to be fancy with request id since that's all this client does is 1 request
            _client.ClientSocket.reqExecutions(requestId, filter);

            if (!manualResetEvent.WaitOne(5000))
            {
                throw new TimeoutException("InteractiveBrokersBrokerage.GetExecutions(): Operation took longer than 5 seconds.");
            }

            // remove our event handlers
            _client.ExecutionDetails -= clientOnExecDetails;
            _client.ExecutionDetailsEnd -= clientOnExecutionDataEnd;

            return details;
        }

        /// <summary>
        /// Connects the client to the IB gateway
        /// </summary>
        public override void Connect()
        {
            if (IsConnected || _isDisposeCalled)
            {
                return;
            }

            Log.Trace("InteractiveBrokersBrokerage.Connect(): not connected, start connecting now...");

            var lastAutomaterStartResult = _ibAutomater.GetLastStartResult();
            if (lastAutomaterStartResult.HasError)
            {
                lastAutomaterStartResult = _ibAutomater.Start(false);
                CheckIbAutomaterError(lastAutomaterStartResult);
                // There was an error but we did not throw, must be another 2FA timeout, we can't continue
                if (lastAutomaterStartResult.HasError)
                {
                    // we couldn't start IBAutomater, so we cannot connect
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "IBAutomaterWarning", $"Unable to restart IBAutomater: {lastAutomaterStartResult.ErrorMessage}"));
                    return;
                }
            }

            _stateManager.IsConnecting = true;

            var attempt = 1;
            const int maxAttempts = 7;

            var subscribedSymbolsCount = _subscriptionManager.GetSubscribedSymbols().Count();
            if (subscribedSymbolsCount > 0)
            {
                Log.Trace($"InteractiveBrokersBrokerage.Connect(): Data subscription count {subscribedSymbolsCount}, restoring data subscriptions is required");
            }

            // While not disposed instead of while(true). This could be happening in a different thread than the dispose call, so let's be safe.
            while (!_isDisposeCalled)
            {
                try
                {
                    Log.Trace("InteractiveBrokersBrokerage.Connect(): Attempting to connect ({0}/{1}) ...", attempt, maxAttempts);

                    // we're going to receive fresh values for all account data, so we clear all
                    _accountData.Clear();

                    // if message processing thread is still running, wait until it terminates
                    Disconnect();

                    // At initial startup or after a gateway restart, we need to wait for the gateway to be ready for a connect request.
                    // Attempting to connect to the socket too early will get a SocketException: Connection refused.
                    if (_cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(2500)))
                    {
                        break;
                    }

                    _waitForNextValidId.Reset();
                    _connectEvent.Reset();

                    // we're going to try and connect several times, if successful break
                    Log.Trace("InteractiveBrokersBrokerage.Connect(): calling _client.ClientSocket.eConnect()");
                    _client.ClientSocket.eConnect(_host, _port, ClientId);

                    if (!_connectEvent.WaitOne(TimeSpan.FromSeconds(15)))
                    {
                        Log.Error("InteractiveBrokersBrokerage.Connect(): timeout waiting for connect callback");
                    }

                    // create the message processing thread
                    var reader = new EReader(_client.ClientSocket, _signal);
                    reader.Start();

                    _messageProcessingThread = new Thread(() =>
                    {
                        Log.Trace("InteractiveBrokersBrokerage.Connect(): IB message processing thread started: #" + Thread.CurrentThread.ManagedThreadId);

                        while (_client.ClientSocket.IsConnected())
                        {
                            try
                            {
                                _signal.waitForSignal();
                                reader.processMsgs();
                            }
                            catch (Exception error)
                            {
                                // error in message processing thread, log error and disconnect
                                Log.Error("InteractiveBrokersBrokerage.Connect(): Error in message processing thread #" + Thread.CurrentThread.ManagedThreadId + ": " + error);
                            }
                        }

                        Log.Trace("InteractiveBrokersBrokerage.Connect(): IB message processing thread ended: #" + Thread.CurrentThread.ManagedThreadId);
                    })
                    { IsBackground = true };

                    _messageProcessingThread.Start();

                    // pause for a moment to receive next valid ID message from gateway
                    if (!_waitForNextValidId.WaitOne(15000))
                    {
                        // no response, disconnect and retry
                        Disconnect();

                        // max out at 5 attempts to connect ~1 minute
                        if (attempt++ < maxAttempts)
                        {
                            Thread.Sleep(1000);
                            continue;
                        }

                        throw new TimeoutException("InteractiveBrokersBrokerage.Connect(): Operation took longer than 15 seconds.");
                    }

                    Log.Trace("InteractiveBrokersBrokerage.Connect(): IB next valid id received.");

                    if (!_client.Connected) throw new Exception("InteractiveBrokersBrokerage.Connect(): Connection returned but was not in connected state.");

                    // request account information for logging purposes
                    var group = string.IsNullOrEmpty(_financialAdvisorsGroupFilter) ? "All" : _financialAdvisorsGroupFilter;
                    _client.ClientSocket.reqAccountSummary(GetNextId(), group, "AccountType");
                    _client.ClientSocket.reqManagedAccts();
                    _client.ClientSocket.reqFamilyCodes();

                    if (IsFinancialAdvisor)
                    {
                        if (!DownloadFinancialAdvisorAccount())
                        {
                            Log.Trace("InteractiveBrokersBrokerage.Connect(): DownloadFinancialAdvisorAccount failed.");

                            Disconnect();

                            if (_accountHoldingsLastException != null)
                            {
                                // if an exception was thrown during account download, do not retry but exit immediately
                                attempt = maxAttempts;
                                throw new Exception(_accountHoldingsLastException.Message, _accountHoldingsLastException);
                            }

                            if (attempt++ < maxAttempts)
                            {
                                Thread.Sleep(1000);
                                continue;
                            }

                            throw new TimeoutException("InteractiveBrokersBrokerage.Connect(): DownloadFinancialAdvisorAccount failed.");
                        }
                    }
                    else
                    {
                        if (!DownloadAccount())
                        {
                            Log.Trace($"InteractiveBrokersBrokerage.Connect(): DownloadAccount failed, attempt {attempt}");

                            Disconnect();

                            if (_accountHoldingsLastException != null)
                            {
                                // if an exception was thrown during account download, do not retry but exit immediately
                                attempt = maxAttempts;
                                throw new Exception(_accountHoldingsLastException.Message, _accountHoldingsLastException);
                            }

                            if (attempt++ < maxAttempts)
                            {
                                Thread.Sleep(1000);
                                continue;
                            }

                            throw new TimeoutException("InteractiveBrokersBrokerage.Connect(): DownloadAccount failed.");
                        }
                    }

                    // enable logging at Warning level
                    _client.ClientSocket.setServerLogLevel(3);

                    break;
                }
                catch (Exception err)
                {
                    // max out at 5 attempts to connect ~1 minute
                    if (attempt++ < maxAttempts)
                    {
                        Thread.Sleep(15000);
                        continue;
                    }
                    _stateManager.IsConnecting = false;

                    // we couldn't connect after several attempts, log the error and throw an exception
                    Log.Error(err);

                    throw;
                }
            }
            _stateManager.IsConnecting = false;

            // if we reached here we should be connected, check just in case
            if (IsConnected)
            {
                _pastFirstConnection = true;
                Log.Trace("InteractiveBrokersBrokerage.Connect(): Restoring data subscriptions...");
                RestoreDataSubscriptions();

                // we need to tell the DefaultBrokerageMessageHandler we are connected else he will kill us
                OnMessage(BrokerageMessageEvent.Reconnected("Connect() finished successfully"));
            }
            else
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "ConnectionState", "Unexpected, not connected state. Unable to connect to Interactive Brokers. Terminating algorithm."));
            }
        }

        private bool HeartBeat(int waitTimeMs)
        {
            if (_cancellationTokenSource.Token.WaitHandle.WaitOne(Time.GetSecondUnevenWait(waitTimeMs)))
            {
                // cancel signal
                return true;
            }

            if (!_isDisposeCalled &&
                !_ibAutomater.IsWithinScheduledServerResetTimes() &&
                IsConnected &&
                // do not run heart beat if we are close to daily restarts
                DateTime.Now.TimeOfDay < _heartBeatTimeLimit &&
                // do not run heart beat if we are restarting
                !IsRestartInProgress())
            {
                _currentTimeEvent.Reset();
                // request current time to the server
                _client.ClientSocket.reqCurrentTime();
                var result = _currentTimeEvent.WaitOne(Time.GetSecondUnevenWait(waitTimeMs), _cancellationTokenSource.Token);
                if (!result)
                {
                    Log.Error("InteractiveBrokersBrokerage.HeartBeat(): failed!", overrideMessageFloodProtection: true);
                }
                return result;
            }
            // expected
            return true;
        }

        private void RunHeartBeatThread()
        {
            _heartBeatThread = new Thread(() =>
            {
                Log.Trace("InteractiveBrokersBrokerage.RunHeartBeatThread(): starting...");
                var waitTimeMs = 1000 * 60 * 2;
                try
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        if (!HeartBeat(waitTimeMs))
                        {
                            // just in case we were unlucky, we are reconnecting or similar let's retry with a longer wait
                            if (!HeartBeat(waitTimeMs * 3))
                            {
                                // we emit the disconnected event so that if the re connection below fails it will kill the algorithm
                                OnMessage(BrokerageMessageEvent.Disconnected("Connection with Interactive Brokers lost. Heart beat failed."));
                                try
                                {
                                    Disconnect();
                                }
                                catch (Exception)
                                {
                                }
                                Connect();
                            }
                            else
                            {
                                Log.Trace("InteractiveBrokersBrokerage.HeartBeat(): succeeded!");
                            }
                        }
                    }
                }
                catch (ObjectDisposedException)
                {
                    // expected
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception e)
                {
                    Log.Error(e, "HeartBeat");
                }

                Log.Trace("InteractiveBrokersBrokerage.RunHeartBeatThread(): ended");
            })
            { IsBackground = true, Name = "IbHeartBeat" };

            _heartBeatThread.Start();
        }

        /// <summary>
        /// Downloads account and position data for the specified financial advisor group.
        /// This is typically called after a successful connection to ensure all relevant data is available before proceeding.
        /// </summary>
        /// <returns><c>true</c> if both account and position data were received within the timeout period; otherwise, <c>false</c>.</returns>
        private bool DownloadFinancialAdvisorAccount()
        {
            if (string.IsNullOrEmpty(_financialAdvisorsGroupFilter))
            {
                // Only one account can be subscribed at a time.
                // With Financial Advisory (FA) account structures there is an alternative way of
                // specifying the account code such that information is returned for 'All' sub accounts.
                // This is done by appending the letter 'A' to the end of the account number
                // https://interactivebrokers.github.io/tws-api/account_updates.html#gsc.tab=0

                // subscribe to the FA account
                return DownloadAccount();
            }

            using var accountDataReceived = new AutoResetEvent(false);
            using var positionDataReceived = new AutoResetEvent(false);

            void OnAccountUpdateMultiEnd(object _, IB.AccountUpdateMultiEndEventArgs args)
            {
                Log.Trace($"InteractiveBrokersBrokerage.DownloadFinancialAdvisorAccount(): Completed account data download for {args}");
                accountDataReceived.Set();
            }

            void OnPositionMultiEnd(object _, EventArgs args)
            {
                Log.Trace("InteractiveBrokersBrokerage.DownloadFinancialAdvisorAccount(): Completed position data download for FA account.");
                positionDataReceived.Set();
            }

            _client.AccountUpdateMultiEnd += OnAccountUpdateMultiEnd;
            _client.PositionMultiEnd += OnPositionMultiEnd;

            try
            {
                _client.RequestAccountUpdatesMulti(GetNextId(), _financialAdvisorsGroupFilter);

                if (!accountDataReceived.WaitOne(TimeSpan.FromSeconds(10)))
                {
                    Log.Error($"InteractiveBrokersBrokerage.DownloadFinancialAdvisorAccount(): Timed out while waiting for account updates for FA group: {_financialAdvisorsGroupFilter}");
                    return false;
                }

                _client.RequestPositionsMulti(GetNextId(), _financialAdvisorsGroupFilter);

                // Wait for position update to finish
                if (!positionDataReceived.WaitOne(TimeSpan.FromSeconds(20)))
                {
                    Log.Error($"InteractiveBrokersBrokerage.DownloadFinancialAdvisorAccount(): Timed out while waiting for position updates for FA group: {_financialAdvisorsGroupFilter}");
                    return false;
                }

                return true;
            }
            finally
            {
                _client.AccountUpdateMultiEnd -= OnAccountUpdateMultiEnd;
                _client.PositionMultiEnd -= OnPositionMultiEnd;
            }
        }

        private string GetAccountName()
        {
            return IsFinancialAdvisor ? $"{_account}A" : _account;
        }

        /// <summary>
        /// Downloads the account information and subscribes to account updates.
        /// This method is called upon successful connection.
        /// </summary>
        private bool DownloadAccount()
        {
            var account = GetAccountName();
            Log.Trace($"InteractiveBrokersBrokerage.DownloadAccount(): Downloading account data for {account}");

            _accountHoldingsLastException = null;
            _accountHoldingsResetEvent.Reset();

            // define our event handler, this acts as stop to make sure when we leave Connect we have downloaded the full account
            EventHandler<IB.AccountDownloadEndEventArgs> clientOnAccountDownloadEnd = (sender, args) =>
            {
                Log.Trace("InteractiveBrokersBrokerage.DownloadAccount(): Finished account download for " + args.Account);
                _accountHoldingsResetEvent.Set();
            };
            _client.AccountDownloadEnd += clientOnAccountDownloadEnd;

            // we'll wait to get our first account update, we need to be absolutely sure we
            // have downloaded the entire account before leaving this function
            using var firstAccountUpdateReceived = new ManualResetEvent(false);
            using var accountIsNotReady = new ManualResetEvent(false);
            EventHandler<IB.UpdateAccountValueEventArgs> clientOnUpdateAccountValue = (sender, args) =>
            {
                if (args != null && !string.IsNullOrEmpty(args.Key)
                    && string.Equals(args.Key, "accountReady", StringComparison.InvariantCultureIgnoreCase)
                    && bool.TryParse(args.Value, out var isReady))
                {
                    if (!isReady)
                    {
                        accountIsNotReady.Set();
                    }
                }
                firstAccountUpdateReceived.Set();
            };

            _client.UpdateAccountValue += clientOnUpdateAccountValue;

            // first we won't subscribe, wait for this to finish, below we'll subscribe for continuous updates
            _client.ClientSocket.reqAccountUpdates(true, account);

            // wait to see the first account value update
            firstAccountUpdateReceived.WaitOne(2500);

            // take pause to ensure the account is downloaded before continuing, this was added because running in
            // linux there appears to be different behavior where the account download end fires immediately.
            Thread.Sleep(2500);

            var result = _accountHoldingsResetEvent.WaitOne(TimeSpan.FromSeconds(20));

            // remove our event handlers
            _client.AccountDownloadEnd -= clientOnAccountDownloadEnd;
            _client.UpdateAccountValue -= clientOnUpdateAccountValue;

            if (!result)
            {
                Log.Trace("InteractiveBrokersBrokerage.DownloadAccount(): Operation took longer than 20 seconds.");
            }

            if (accountIsNotReady.WaitOne(TimeSpan.Zero))
            {
                result = false;
                Log.Error("InteractiveBrokersBrokerage.DownloadAccount(): Account is not ready! Means that the IB server is in the process of resetting. Wait 30min and retry...");
                _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromMinutes(30));
            }

            return result && _accountHoldingsLastException == null;
        }

        /// <summary>
        /// Disconnects the client from the IB gateway
        /// </summary>
        public override void Disconnect()
        {
            Log.Trace("InteractiveBrokersBrokerage.Disconnect(): Starting");
            try
            {
                if (_client != null && _client.ClientSocket != null && _client.Connected)
                {
                    if (string.IsNullOrEmpty(_financialAdvisorsGroupFilter))
                    {
                        // unsubscribe from account updates
                        _client.ClientSocket.reqAccountUpdates(subscribe: false, GetAccountName());
                    }
                    else
                    {
                        _client.CancelAccountUpdatesMulti(GetNextId());
                        _client.CancelPositionsMulti(GetNextId());
                    }
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception);
            }
            _client?.ClientSocket.eDisconnect();

            if (_messageProcessingThread != null)
            {
                _signal.issueSignal();
                if (!_messageProcessingThread.Join(TimeSpan.FromSeconds(30)))
                {
                    Log.Error("InteractiveBrokersBrokerage.Disconnect(): timeout waiting for message processing thread to end");
                }
                _messageProcessingThread = null;
            }
            Log.Trace("InteractiveBrokersBrokerage.Disconnect(): ended");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            if (_isDisposeCalled)
            {
                return;
            }

            Log.Trace("InteractiveBrokersBrokerage.Dispose(): Disposing of IB resources.");

            _isDisposeCalled = true;

            _heartBeatThread.StopSafely(TimeSpan.FromSeconds(10), _cancellationTokenSource);
            _comboOrdersFillTimeoutMonitor.Stop();

            if (_client != null)
            {
                Disconnect();
                _client.Dispose();
            }

            _aggregator.DisposeSafely();
            _ibAutomater?.Stop();
            _ibAutomater.DisposeSafely();

            _messagingRateLimiter.DisposeSafely();
            _concurrentHistoryRequests.DisposeSafely();
            _historyHighResolutionRateLimiter.DisposeSafely();
        }

        /// <summary>
        /// Initialize the instance of this class
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        /// <param name="orderProvider">An instance of IOrderProvider used to fetch Order objects by brokerage ID</param>
        /// <param name="securityProvider">The security provider used to give access to algorithm securities</param>
        /// <param name="account">The Interactive Brokers account name</param>
        /// <param name="host">host name or IP address of the machine where TWS is running. Leave blank to connect to the local host.</param>
        /// <param name="port">must match the port specified in TWS on the Configure&gt;API&gt;Socket Port field.</param>
        /// <param name="ibDirectory">The IB Gateway root directory</param>
        /// <param name="ibVersion">The IB Gateway version</param>
        /// <param name="userName">The login user name</param>
        /// <param name="password">The login password</param>
        /// <param name="tradingMode">The trading mode: 'live' or 'paper'</param>
        /// <param name="agentDescription">Used for Rule 80A describes the type of trader.</param>
        /// <param name="loadExistingHoldings">False will ignore existing security holdings from being loaded.</param>
        /// <param name="weeklyRestartUtcTime">The UTC time at which IBAutomater should be restarted and 2FA confirmation should be requested on Sundays (IB's weekly restart)</param>
        /// <param name="financialAdvisorsGroupFilter">The name of the financial advisors group associated with this client.</param>
        private void Initialize(
            IAlgorithm algorithm,
            IOrderProvider orderProvider,
            ISecurityProvider securityProvider,
            string account,
            string host,
            int port,
            string ibDirectory,
            string ibVersion,
            string userName,
            string password,
            string tradingMode,
            string agentDescription = IB.AgentDescription.Individual,
            bool loadExistingHoldings = true,
            TimeSpan? weeklyRestartUtcTime = null,
            string financialAdvisorsGroupFilter = default)
        {
            if (_isInitialized)
            {
                return;
            }

            ValidateSubscription();

            _isInitialized = true;
            _loadExistingHoldings = loadExistingHoldings;
            _algorithm = algorithm;
            _orderProvider = orderProvider;
            ConcurrencyEnabled = true;

            if (!string.IsNullOrEmpty(financialAdvisorsGroupFilter))
            {
                Log.Trace($"InteractiveBrokersBrokerage.InteractiveBrokersBrokerage(): Using Financial Advisor group filter: '{financialAdvisorsGroupFilter}'");
                _financialAdvisorsGroupFilter = financialAdvisorsGroupFilter;
            }

            _mapFileProvider = Composer.Instance.GetPart<IMapFileProvider>();
            if (_mapFileProvider == null)
            {
                // toolbox downloader case
                var mapFileProviderName = Config.Get("map-file-provider", "QuantConnect.Data.Auxiliary.LocalDiskMapFileProvider");
                Log.Trace($"InteractiveBrokersBrokerage.InteractiveBrokersBrokerage(): found no map file provider instance, creating {mapFileProviderName}");
                _mapFileProvider = Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(mapFileProviderName);
                _mapFileProvider.Initialize(Composer.Instance.GetExportedValueByTypeName<IDataProvider>(Config.Get("data-provider", "DefaultDataProvider")));
            }
            _aggregator = Composer.Instance.GetPart<IDataAggregator>();
            if (_aggregator == null)
            {
                // toolbox downloader case
                var aggregatorName = Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager");
                Log.Trace($"InteractiveBrokersBrokerage.InteractiveBrokersBrokerage(): found no data aggregator instance, creating {aggregatorName}");
                _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(aggregatorName);
            }
            _account = account;
            _host = host;
            _port = port;

            _agentDescription = agentDescription;

            _symbolMapper = new InteractiveBrokersSymbolMapper(_mapFileProvider);
            _contractSpecificationService = new(GetContractDetails);
            _exchangeProvider = new MapFilePrimaryExchangeProvider(_mapFileProvider);

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (s, t) => Subscribe(s);
            _subscriptionManager.UnsubscribeImpl += (s, t) => Unsubscribe(s);

            Log.Trace("InteractiveBrokersBrokerage.InteractiveBrokersBrokerage(): Starting IB Automater...");

            // start IB Gateway
            var exportIbGatewayLogs = true; // Config.GetBool("ib-export-ibgateway-logs");
            _ibAutomater = new IBAutomater.IBAutomater(ibDirectory, ibVersion, userName, password, tradingMode, port, exportIbGatewayLogs);
            _ibAutomater.OutputDataReceived += OnIbAutomaterOutputDataReceived;
            _ibAutomater.ErrorDataReceived += OnIbAutomaterErrorDataReceived;
            _ibAutomater.Exited += OnIbAutomaterExited;
            _ibAutomater.Restarted += OnIbAutomaterRestarted;

            try
            {
                CheckIbAutomaterError(_ibAutomater.Start(false));
            }
            catch
            {
                // we are going the kill the deployment, let's clean up the automater
                _ibAutomater.DisposeSafely();
                throw;
            }

            // default the weekly restart to one hour before FX market open (GetNextWeekendReconnectionTimeUtc)
            _weeklyRestartUtcTime = weeklyRestartUtcTime ?? _defaultWeeklyRestartUtcTime;
            // schedule the weekly IB Gateway restart
            StartGatewayWeeklyRestartTask();

            Log.Trace($"InteractiveBrokersBrokerage.InteractiveBrokersBrokerage(): Host: {host}, Port: {port}, Account: {account}, AgentDescription: {agentDescription}");

            _client = new IB.InteractiveBrokersClient(_signal);

            // running as a data provider only
            if (_algorithm != null)
            {
                // set up event handlers
                _client.UpdatePortfolio += HandlePortfolioUpdates;
                _client.OrderStatus += HandleOrderStatusUpdates;
                _client.OpenOrder += HandleOpenOrder;
                _client.OpenOrderEnd += HandleOpenOrderEnd;
                _client.ExecutionDetails += HandleExecutionDetails;
                _client.CommissionReport += HandleCommissionReport;
            }
            _client.UpdateAccountValue += HandleUpdateAccountValue;
            _client.AccountSummary += HandleAccountSummary;
            _client.ManagedAccounts += HandleManagedAccounts;
            _client.FamilyCodes += HandleFamilyCodes;
            _client.Error += HandleError;
            _client.TickPrice += HandleTickPrice;
            _client.TickSize += HandleTickSize;
            _client.CurrentTimeUtc += HandleBrokerTime;
            _client.ReRouteMarketDataRequest += HandleMarketDataReRoute;
            if (!string.IsNullOrEmpty(financialAdvisorsGroupFilter))
            {
                _client.AccountUpdateMulti += HandleUpdateAccountValue;
            }

            // we need to wait until we receive the next valid id from the server
            _client.NextValidId += (sender, e) =>
            {
                lock (_nextValidIdLocker)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.HandleNextValidID(): updating nextValidId from {_nextValidId} to {e.OrderId}");

                    _nextValidId = e.OrderId;
                    _waitForNextValidId.Set();
                }
            };

            _client.ConnectAck += (sender, e) =>
            {
                Log.Trace($"InteractiveBrokersBrokerage.HandleConnectAck(): API client connected [Server Version: {_client.ClientSocket.ServerVersion}].");
                _connectEvent.Set();
            };

            _client.ConnectionClosed += (sender, e) =>
            {
                Log.Trace($"InteractiveBrokersBrokerage.HandleConnectionClosed(): API client disconnected [Server Version: {_client.ClientSocket.ServerVersion}].");
                _connectEvent.Set();
            };

            // initialize our heart beat thread
            RunHeartBeatThread();

            // initialize the combo orders fill timeout monitor thread
            _comboOrdersFillTimeoutMonitor.TimeoutEvent += (object sender, PendingFillEvent e) =>
            {
                EmitOrderFill(e.Order, e.ExecutionDetails, e.CommissionReport, true);
            };
            _comboOrdersFillTimeoutMonitor.Start();
        }

        /// <summary>
        /// Places the order with InteractiveBrokers
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <param name="needsNewId">Set to true to generate a new order ID, false to leave it alone</param>
        /// <param name="exchange">The exchange to send the order to, defaults to "Smart" to use IB's smart routing</param>
        private void IBPlaceOrder(Order order, bool needsNewId, string exchange = null)
        {
            if (!_groupOrderCacheManager.TryGetGroupCachedOrders(order, out var orders))
            {
                return;
            }

            // MOO/MOC require directed option orders.
            // We resolve non-equity markets in the `CreateContract` method.
            if (exchange == null &&
                order.Symbol.SecurityType == SecurityType.Option &&
                (order.Type == OrderType.MarketOnOpen || order.Type == OrderType.MarketOnClose))
            {
                exchange = Market.CBOE.ToUpperInvariant();
            }

            // If a combo order is being updated, let's use the existing contract to overcome IB's bug.
            // See https://github.com/QuantConnect/Lean.Brokerages.InteractiveBrokers/issues/66 and
            // https://groups.io/g/twsapi/topic/5333246?p=%2C%2C%2C20%2C0%2C0%2C0%3A%3A%2C%2C%2C0%2C0%2C0%2C5333246
            Contract contract;
            if (needsNewId || order.GroupOrderManager == null)
            {
                contract = CreateContract(orders[0].Symbol, false, orders, exchange);
            }
            else
            {
                if (!_comboOrdersContracts.TryGetValue(order.GroupOrderManager.Id, out contract))
                {
                    // We need the contract created by IB in order to update combo orders, so lets fetch it
                    contract = GetOpenOrderContract(int.Parse(order.BrokerId[0]));
                    // No need to cache the contract here, it will be cached by our default OpenOrder handler
                }
            }

            if (TryAvoidMarketOnOpenBoundaryRejection(order.Symbol, order.Type, GetRealTimeTickTime(order.Symbol), out var delay))
            {
                _cancellationTokenSource.Token.WaitHandle.WaitOne(delay);
            }

            CheckRateLimiting();

            int ibOrderId;
            ManualResetEventSlim orderSubmittedEvent = null;

            // Let's lock here so that getting request id and placing the order is atomic.
            // If there are multiple threads placing orders at the same time, two threads could
            // get ids but the one with the higher id could place the order first, making the other
            // order request to fail, since IB will assume the previous request ID was already used.
            lock (_nextValidIdLocker)
            {
                if (needsNewId)
                {
                    // the order ids are generated for us by the SecurityTransactionManaer
                    var id = GetNextId();
                    foreach (var newOrder in orders)
                    {
                        newOrder.BrokerId.Add(id.ToStringInvariant());
                    }
                    ibOrderId = id;
                }
                else if (order.BrokerId.Any())
                {
                    // this is *not* perfect code
                    ibOrderId = Parse.Int(order.BrokerId[0]);
                }
                else
                {
                    throw new ArgumentException("Expected order with populated BrokerId for updating orders.");
                }

                Log.Trace($"InteractiveBrokersBrokerage.PlaceOrder(): Symbol: {order.Symbol.Value} Quantity: {order.Quantity}. Id: {order.Id}. BrokerId: {ibOrderId}");

                _requestInformation[ibOrderId] = new RequestInformation
                {
                    RequestId = ibOrderId,
                    RequestType = RequestType.PlaceOrder,
                    AssociatedSymbol = order.Symbol,
                    Message = $"[Id={ibOrderId}] IBPlaceOrder: {order.Symbol.Value} ({GetContractDescription(contract)} )"
                };

                if (order.Type == OrderType.OptionExercise)
                {
                    // IB API requires exerciseQuantity to be positive
                    _client.ClientSocket.exerciseOptions(ibOrderId, contract, 1, decimal.ToInt32(order.AbsoluteQuantity), _account, 0,
                        string.Empty, string.Empty, false);
                }
                else
                {
                    _pendingOrderResponse[ibOrderId] = orderSubmittedEvent = new ManualResetEventSlim(false);
                    var ibOrder = ConvertOrder(orders, contract, ibOrderId);
                    _client.ClientSocket.placeOrder(ibOrder.OrderId, contract, ibOrder);
                }
            }

            if (order.Type != OrderType.OptionExercise)
            {
                var noSubmissionOrderTypes = _noSubmissionOrderTypes.Contains(order.Type);
                if (!orderSubmittedEvent.Wait(noSubmissionOrderTypes ? _noSubmissionOrdersResponseTimeout : _responseTimeout))
                {
                    if (noSubmissionOrderTypes)
                    {
                        if (!_submissionOrdersWarningSent)
                        {
                            _submissionOrdersWarningSent = true;
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning,
                                "OrderSubmissionWarning",
                                "Interactive Brokers does not send a submission event for some order types, in these cases if no error is detected Lean will generate the submission event."));
                        }

                        if (_pendingOrderResponse.TryRemove(ibOrderId, out var _))
                        {
                            orderSubmittedEvent.DisposeSafely();

                            var orderEvents = orders.Where(order => order != null).Select(order => new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = OrderStatus.Submitted,
                                Message = "Lean Generated Interactive Brokers Order Event"
                            }).ToList();
                            OnOrderEvents(orderEvents);
                        }
                    }
                    else
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "NoBrokerageResponse", $"Timeout waiting for brokerage response for brokerage order id {ibOrderId} lean id {order.Id}"));
                    }
                }
                else
                {
                    orderSubmittedEvent.DisposeSafely();
                }
            }
        }

        /// <summary>
        /// Checks whether a <see cref="OrderType.MarketOnOpen"/> order falls into IB's 
        /// unsafe submission window (exactly at market close, e.g., 16:00:00 ET),
        /// which would otherwise be rejected with "Exchange is closed".
        /// </summary>
        /// <param name="symbol">The trading symbol for which the order is placed.</param>
        /// <param name="orderType">The order type being submitted (only MOO is affected).</param>
        /// <param name="nowExchangeTimeZone">Current exchange-local time.</param>
        /// <param name="delay">The required delay before safe submission if within the unsafe boundary.
        /// </param>
        /// <returns><c>true</c> if submission falls into the unsafe window and a delay is required; otherwise <c>false</c>.
        /// </returns>
        /// <remarks>
        /// IB rejects MarketOnOpen orders submitted at the exact close boundary
        /// (<c>16:00:00 ET</c>). This method applies a small delay (default 1s)
        /// to ensure safe submission.
        /// </remarks>
        internal bool TryAvoidMarketOnOpenBoundaryRejection(Symbol symbol, OrderType orderType, in DateTime nowExchangeTimeZone, out TimeSpan delay)
        {
            delay = TimeSpan.Zero;
            if (orderType != OrderType.MarketOnOpen || symbol.SecurityType != SecurityType.Equity || symbol.ID.Market != Market.USA)
            {
                return false;
            }

            var fiveSeconds = TimeSpan.FromSeconds(5);

            // IB rejects MOO orders submitted exactly at this boundary.
            var marketOnOpenOrderSafeSubmissionStartTime = GetSecurityExchangeHours(symbol).GetLastDailyMarketClose(nowExchangeTimeZone.Add(-fiveSeconds), false);

            // adds a buffer to avoid IB rejecting orders with error '201 - Order rejected - reason: Exchange is closed.'
            var marketOnOpenOrderSafeSubmissionEndTime = marketOnOpenOrderSafeSubmissionStartTime.Add(fiveSeconds);

            if (nowExchangeTimeZone >= marketOnOpenOrderSafeSubmissionStartTime && nowExchangeTimeZone < marketOnOpenOrderSafeSubmissionEndTime)
            {
                if (!_hasWarnedSafeMooExecution)
                {
                    _hasWarnedSafeMooExecution = true;
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "SafeMooExecution",
                        "Delayed MarketOnOpen order submission to avoid IB rejection: '201 - Order rejected - reason: Exchange is closed.'"));
                }

                delay = marketOnOpenOrderSafeSubmissionEndTime - nowExchangeTimeZone;
                return true;
            }

            return false;
        }

        private static string GetUniqueKey(Contract contract)
        {
            var leanSecurityType = ConvertSecurityType(contract);
            if (leanSecurityType == SecurityType.Equity ||
                leanSecurityType == SecurityType.Forex ||
                leanSecurityType == SecurityType.Cfd ||
                leanSecurityType == SecurityType.Index)
            {
                return contract.ToString().ToUpperInvariant();
            }

            // for IB trading class can be different depending on the contract flavor, e.g. index options SPX & SPXW
            return $"{contract.ToString().ToUpperInvariant()} {contract.LastTradeDateOrContractMonth.ToStringInvariant()} {contract.Strike.ToStringInvariant()} {contract.Right} {contract.TradingClass}";
        }

        /// <summary>
        /// Get Contract Description
        /// </summary>
        /// <param name="contract">Contract to retrieve description of</param>
        /// <returns>string description</returns>
        public static string GetContractDescription(Contract contract)
        {
            return $"{contract} {contract.PrimaryExch ?? string.Empty} {contract.LastTradeDateOrContractMonth.ToStringInvariant()} {contract.Strike.ToStringInvariant()} {contract.Right}";
        }

        /// <summary>
        /// Retrieves the primary exchange for a given symbol.
        /// </summary>
        /// <param name="contract">The IB <see cref="Contract"/> object containing contract details.</param>
        /// <param name="symbol">The Lean <see cref="Symbol"/> object representing the security.</param>
        /// <returns>
        /// The name of the primary exchange as a string.
        /// If the market is USA and Lean provides an exchange, that value is returned.
        /// Otherwise, falls back to the IB contract's <c>PrimaryExch</c> field.
        /// Returns <c>null</c> if no exchange information is available.
        /// </returns>
        internal string GetPrimaryExchange(Contract contract, Symbol symbol)
        {
            if (symbol.ID.Market.Equals(Market.USA, StringComparison.InvariantCultureIgnoreCase))
            {
                var leanExchange = _exchangeProvider.GetPrimaryExchange(symbol.ID)?.Name;
                if (!string.IsNullOrEmpty(leanExchange))
                {
                    return leanExchange;
                }
            }

            return GetContractDetails(contract, symbol.Value)?.Contract.PrimaryExch;
        }

        /// <summary>
        /// Will return and cache the IB contract details for the requested contract
        /// </summary>
        /// <param name="contract">The target contract</param>
        /// <param name="ticker">The associated Lean ticker. Just used for logging, can be provided empty</param>
        internal ContractDetails GetContractDetails(Contract contract, string ticker, bool failIfNotFound = true)
        {
            if (contract.SecType != null && _contractDetails.TryGetValue(GetUniqueKey(contract), out var details))
            {
                return details;
            }
            return GetContractDetailsImpl(contract, ticker, failIfNotFound);
        }

        /// <summary>
        /// Will return and cache the IB contract details for the requested contract
        /// </summary>
        /// <param name="contract">The target contract</param>
        /// <param name="ticker">The associated Lean ticker. Just used for logging, can be provided empty</param>
        /// <param name="failIfNotFound">
        /// If <c>true</c>, the request is treated as a required lookup and will be logged as a <see cref="RequestType.ContractDetails"/> request.
        /// If <c>false</c>, the request is treated as a soft lookup (<see cref="RequestType.SoftContractDetails"/>) that does not cause a failure if no details are found.
        /// </param>
        private ContractDetails GetContractDetailsImpl(Contract contract, string ticker, bool failIfNotFound = true)
        {
            const int timeout = 60; // sec

            var requestId = GetNextId();

            var contractDetailsList = new List<ContractDetails>();

            Log.Trace($"InteractiveBrokersBrokerage.GetContractDetails(): {ticker} ({contract})");

            _requestInformation[requestId] = new RequestInformation
            {
                RequestId = requestId,
                RequestType = failIfNotFound ? RequestType.ContractDetails : RequestType.SoftContractDetails,
                Message = $"[Id={requestId}] GetContractDetails: {ticker} ({contract})"
            };

            var manualResetEvent = new ManualResetEvent(false);

            // define our event handlers
            EventHandler<IB.ContractDetailsEventArgs> clientOnContractDetails = (sender, args) =>
            {
                // ignore other requests
                if (args.RequestId != requestId)
                {
                    return;
                }

                var details = args.ContractDetails;
                contractDetailsList.Add(details);

                var uniqueKey = GetUniqueKey(details.Contract);
                _contractDetails.TryAdd(uniqueKey, details);

                Log.Trace($"InteractiveBrokersBrokerage.GetContractDetails(): clientOnContractDetails event: {uniqueKey}");
            };

            EventHandler<IB.RequestEndEventArgs> clientOnContractDetailsEnd = (sender, args) =>
            {
                if (args.RequestId == requestId)
                {
                    manualResetEvent.Set();
                }
            };

            EventHandler<IB.ErrorEventArgs> clientOnError = (sender, args) =>
            {
                if (args.Id == requestId)
                {
                    manualResetEvent.Set();
                }
            };

            _client.ContractDetails += clientOnContractDetails;
            _client.ContractDetailsEnd += clientOnContractDetailsEnd;
            _client.Error += clientOnError;

            CheckRateLimiting();

            // make the request for data
            _client.ClientSocket.reqContractDetails(requestId, contract);

            if (!manualResetEvent.WaitOne(timeout * 1000))
            {
                Log.Error("InteractiveBrokersBrokerage.GetContractDetails(): failed to receive response from IB within {0} seconds", timeout);
            }

            // be sure to remove our event handlers
            _client.Error -= clientOnError;
            _client.ContractDetailsEnd -= clientOnContractDetailsEnd;
            _client.ContractDetails -= clientOnContractDetails;

            Log.Trace($"InteractiveBrokersBrokerage.GetContractDetails(): contracts found: {contractDetailsList.Count}");

            return contractDetailsList.FirstOrDefault();
        }

        /// <summary>
        /// Helper method to normalize a provided price to the Lean expected unit
        /// </summary>
        /// <param name="price">Price to be normalized</param>
        /// <param name="symbol">Symbol from which we need to get the PriceMagnifier attribute to normalize the price</param>
        /// <returns>The price normalized to LEAN expected unit</returns>
        public static decimal NormalizePriceToLean(double price, Symbol symbol)
        {
            return NormalizePriceToLean(Convert.ToDecimal(price), symbol);
        }

        /// <summary>
        /// Helper method to normalize a provided price to the Lean expected unit
        /// </summary>
        /// <param name="price">Price to be normalized</param>
        /// <param name="symbol">Symbol from which we need to get the PriceMagnifier attribute to normalize the price</param>
        /// <returns>The price normalized to LEAN expected unit</returns>
        public static decimal NormalizePriceToLean(decimal price, Symbol symbol)
        {
            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, Currencies.USD);
            return Convert.ToDecimal(price) / symbolProperties.PriceMagnifier;
        }

        /// <summary>
        /// Helper method to normalize a provided price to the brokerage expected unit, for example cents,
        /// applying rounding to minimum tick size
        /// </summary>
        /// <param name="price">Price to be normalized</param>
        /// <param name="contract">Contract of the symbol</param>
        /// <param name="symbol">The symbol from which we need to get the PriceMagnifier attribute to normalize the price</param>
        /// <returns>The price normalized to be brokerage expected unit</returns>
        public double NormalizePriceToBrokerage(decimal price, Contract contract, Symbol symbol)
        {
            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, Currencies.USD);
            var roundedPrice = RoundPrice(price, _contractSpecificationService.GetMinTick(contract, symbol));
            roundedPrice *= symbolProperties.PriceMagnifier;
            return Convert.ToDouble(roundedPrice);
        }

        /// <summary>
        /// Find contract details given a ticker and contract
        /// </summary>
        /// <param name="contract">Contract we are searching for</param>
        /// <param name="ticker">Ticker of this contract</param>
        /// <returns></returns>
        public IEnumerable<ContractDetails> FindContracts(Contract contract, string ticker)
        {
            const int timeout = 60; // sec

            var requestId = GetNextId();

            _requestInformation[requestId] = new RequestInformation
            {
                RequestId = requestId,
                RequestType = RequestType.FindContracts,
                Message = $"[Id={requestId}] FindContracts: {ticker} ({GetContractDescription(contract)})"
            }; ;

            var manualResetEvent = new ManualResetEvent(false);
            var contractDetails = new List<ContractDetails>();

            // define our event handlers
            EventHandler<IB.ContractDetailsEventArgs> clientOnContractDetails = (sender, args) =>
            {
                if (args.RequestId == requestId)
                {
                    contractDetails.Add(args.ContractDetails);
                }
            };

            EventHandler<IB.RequestEndEventArgs> clientOnContractDetailsEnd = (sender, args) =>
            {
                if (args.RequestId == requestId)
                {
                    manualResetEvent.Set();
                }
            };

            EventHandler<IB.ErrorEventArgs> clientOnError = (sender, args) =>
            {
                if (args.Id == requestId)
                {
                    manualResetEvent.Set();
                }
            };

            _client.ContractDetails += clientOnContractDetails;
            _client.ContractDetailsEnd += clientOnContractDetailsEnd;
            _client.Error += clientOnError;

            CheckRateLimiting();

            // make the request for data
            _client.ClientSocket.reqContractDetails(requestId, contract);

            if (!manualResetEvent.WaitOne(timeout * 1000))
            {
                Log.Error("InteractiveBrokersBrokerage.FindContracts(): failed to receive response from IB within {0} seconds", timeout);
            }

            // be sure to remove our event handlers
            _client.Error -= clientOnError;
            _client.ContractDetailsEnd -= clientOnContractDetailsEnd;
            _client.ContractDetails -= clientOnContractDetails;

            return contractDetails;
        }

        /// <summary>
        /// Handles error messages from IB
        /// </summary>
        private void HandleError(object sender, IB.ErrorEventArgs e)
        {
            // handles the 'connection refused' connect cases
            _connectEvent.Set();

            // https://interactivebrokers.github.io/tws-api/message_codes.html

            var requestId = e.Id;
            var errorCode = e.Code;
            var errorMsg = e.Message;

            // rewrite these messages to be single lined
            errorMsg = errorMsg.Replace("\r\n", ". ").Replace("\r", ". ").Replace("\n", ". ");

            // if there is additional information for the originating request, append it to the error message
            if (_requestInformation.TryGetValue(requestId, out var requestInfo))
            {
                errorMsg += ". Origin: " + requestInfo.Message;
            }

            // historical data request with no data returned
            if (errorCode == 162 && errorMsg.Contains("HMDS query returned no data"))
            {
                return;
            }

            Log.Trace($"InteractiveBrokersBrokerage.HandleError(): RequestId: {requestId} ErrorCode: {errorCode} - {errorMsg}");

            if (errorCode == 2105 || errorCode == 2103)
            {
                // 'connection is broken': if we haven't already let's trigger a gateway restart
                StartGatewayRestartTask();
            }
            else if (errorCode == 2106 || errorCode == 2104)
            {
                // 'connection is ok'
                StopGatewayRestartTask();
            }

            // figure out the message type based on our code collections below
            var brokerageMessageType = BrokerageMessageType.Information;
            if (ErrorCodes.Contains(errorCode))
            {
                brokerageMessageType = BrokerageMessageType.Error;
            }
            else if (WarningCodes.Contains(errorCode))
            {
                brokerageMessageType = BrokerageMessageType.Warning;
            }

            // code 1100 is a connection failure, we'll wait a minute before exploding gracefully
            if (errorCode == 1100)
            {
                StopGatewayRestartTask();
                if (!_stateManager.Disconnected1100Fired)
                {
                    _stateManager.Disconnected1100Fired = true;

                    // begin the try wait logic
                    TryWaitForReconnect();
                }
                else
                {
                    // The IB API sends many consecutive disconnect messages (1100) during nightly reset periods and weekends,
                    // so we send the message event only when we transition from connected to disconnected state,
                    // to avoid flooding the logs with the same message.
                    return;
                }
            }
            else if (errorCode == 1102)
            {
                // Connectivity between IB and TWS has been restored - data maintained.
                OnMessage(BrokerageMessageEvent.Reconnected(errorMsg));

                _stateManager.Disconnected1100Fired = false;
                return;
            }
            else if (errorCode == 1101)
            {
                // Connectivity between IB and TWS has been restored - data lost.
                OnMessage(BrokerageMessageEvent.Reconnected(errorMsg));

                _stateManager.Disconnected1100Fired = false;

                RestoreDataSubscriptions();
                return;
            }
            else if (errorCode == 506)
            {
                Log.Trace("InteractiveBrokersBrokerage.HandleError(): Server Version: " + _client.ClientSocket.ServerVersion);

                if (!_client.ClientSocket.IsConnected())
                {
                    // ignore the 506 error if we are not yet connected, will be checked by IB API later
                    // we have occasionally experienced this error after restarting IBGateway after the nightly reset
                    Log.Trace($"InteractiveBrokersBrokerage.HandleError(): Not connected, ignoring error, ErrorCode: {errorCode} - {errorMsg}");
                    return;
                }
            }
            else if (errorCode == 101)
            {
                if (_maxSubscribedSymbolsReached) return;
                // Improves error message with the additional notes IB provides on the the error messages page
                errorMsg = $"{e.Message} - The current number of active market data subscriptions in TWS and the API altogether has been exceeded ({_subscribedSymbols.Count}). This number is calculated based on a formula which is based on the equity, commissions, and quote booster packs in an account.";
                _maxSubscribedSymbolsReached = true;
            }
            else if (errorCode == 200)
            {
                // No security definition has been found for the request
                if (requestInfo is not null)
                {
                    // This is a common error when requesting historical data for expired contracts, in which case can ignore it
                    if (requestInfo.RequestType == RequestType.History)
                    {
                        MapFile mapFile = null;
                        if (requestInfo.AssociatedSymbol.RequiresMapping())
                        {
                            var resolver = _mapFileProvider.Get(AuxiliaryDataKey.Create(requestInfo.AssociatedSymbol));
                            mapFile = resolver.ResolveMapFile(requestInfo.AssociatedSymbol);
                        }
                        var historicalLimitDate = requestInfo.AssociatedSymbol.GetDelistingDate(mapFile).AddDays(1)
                            .ConvertToUtc(requestInfo.HistoryRequest.ExchangeHours.TimeZone);

                        if (DateTime.UtcNow.Date > historicalLimitDate)
                        {
                            Log.Trace($"InteractiveBrokersBrokerage.HandleError(): Expired contract historical data request, ignoring error.  ErrorCode: {errorCode} - {errorMsg}");
                            return;
                        }
                    }
                    // If the request is marked as a soft contract details reques, we won't exit if not found.
                    // This can happen when checking whether a cfd if a forex cfd to create a contract,
                    // the first contract details request might return this error if it's in fact a forex CFD
                    // since the whole symbol (e.g. EURUSD) will be used first. We can ignore it
                    else if (requestInfo.RequestType == RequestType.SoftContractDetails)
                    {
                        return;
                    }

                    if (_algorithm != null && _algorithm.Settings.IgnoreUnknownAssetHoldings)
                    {
                        // Let's make it a one time warning, we don't want to flood the logs with this message
                        if (requestInfo?.AssociatedSymbol != null)
                        {
                            lock (_unsupportedAssets)
                            {
                                if (!_unsupportedAssets.Add($"{requestInfo.AssociatedSymbol.Value}-{requestInfo.AssociatedSymbol.SecurityType}"))
                                {
                                    return;
                                }
                            }
                        }

                        brokerageMessageType = BrokerageMessageType.Warning;
                    }
                }
            }

            if (InvalidatingCodes.Contains(errorCode))
            {
                // let's unblock the waiting thread right away
                if (_pendingOrderResponse.TryRemove(requestId, out var eventSlim))
                {
                    eventSlim.Set();
                }

                var message = $"{errorCode} - {errorMsg}";
                Log.Trace($"InteractiveBrokersBrokerage.HandleError.InvalidateOrder(): IBOrderId: {requestId} ErrorCode: {message}");

                // invalidate the order
                var orders = _orderProvider.GetOrdersByBrokerageId(requestId);
                if (orders.IsNullOrEmpty())
                {
                    Log.Error($"InteractiveBrokersBrokerage.HandleError.InvalidateOrder(): Unable to locate order with BrokerageID {requestId}");
                }
                else
                {
                    OnOrderEvents(orders.Where(order => order != null).Select(order => new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = OrderStatus.Invalid,
                        Message = message
                    }).ToList());
                }
            }

            if (!FilteredCodes.Contains(errorCode) && errorCode != -1)
            {
                OnMessage(new BrokerageMessageEvent(brokerageMessageType, errorCode, errorMsg));
            }
        }

        /// <summary>
        /// Restores data subscriptions existing before the IB Gateway restart
        /// </summary>
        private void RestoreDataSubscriptions()
        {
            List<Symbol> subscribedSymbols;
            lock (_sync)
            {
                subscribedSymbols = _subscriptionManager.GetSubscribedSymbols().ToList();

                _subscribedSymbols.Clear();
                _subscribedTickers.Clear();
            }

            Subscribe(subscribedSymbols);
        }

        /// <summary>
        /// If we lose connection to TWS/IB servers we don't want to send the Error event if it is within
        /// the scheduled server reset times
        /// </summary>
        private void TryWaitForReconnect()
        {
            // IB has server reset schedule: https://www.interactivebrokers.com/en/?f=%2Fen%2Fsoftware%2FsystemStatus.php%3Fib_entity%3Dllc

            if (!_stateManager.Disconnected1100Fired)
            {
                return;
            }

            var isResetTime = _ibAutomater.IsWithinScheduledServerResetTimes();

            if (!isResetTime)
            {
                if (!_stateManager.PreviouslyInResetTime)
                {
                    // if we were disconnected and we're not within the reset times, send the error event
                    OnMessage(BrokerageMessageEvent.Disconnected("Connection with Interactive Brokers lost. " +
                                                                 "This could be because of internet connectivity issues or a log in from another location."
                        ));
                }
            }
            else
            {
                Log.Trace("InteractiveBrokersBrokerage.TryWaitForReconnect(): Within server reset times, trying to wait for reconnect...");

                // we're still not connected but we're also within the schedule reset time, so just keep polling
                Task.Delay(TimeSpan.FromMinutes(1)).ContinueWith(_ => TryWaitForReconnect());
            }

            _stateManager.PreviouslyInResetTime = isResetTime;
        }

        /// <summary>
        /// Stores all the account values
        /// </summary>
        private void HandleUpdateAccountValue(object sender, IB.UpdateAccountValueEventArgs e)
        {
            if (Log.DebuggingEnabled)
            {
                Log.Trace($"HandleUpdateAccountValue(): Key:{e.Key} Value:{e.Value} Currency:{e.Currency} AccountName:{e.AccountName}");
            }

            try
            {
                _accountData.AccountProperties[e.Currency + ":" + e.Key] = e.Value;

                // we want to capture if the user's cash changes so we can reflect it in the algorithm
                if (e.Key == AccountValueKeys.CashBalance && e.Currency != "BASE")
                {
                    var cashBalance = decimal.Parse(e.Value, CultureInfo.InvariantCulture);
                    _accountData.CashBalances.AddOrUpdate(e.Currency, cashBalance);

                    OnAccountChanged(new AccountEvent(e.Currency, cashBalance));
                }

                // IB does not explicitly return the account base currency, but we can find out using exchange rates returned
                if (e.Key == AccountValueKeys.ExchangeRate && e.Currency != "BASE" && e.Value.ToDecimal() == 1)
                {
                    AccountBaseCurrency = e.Currency;
                }
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.HandleUpdateAccountValue(): " + err);
            }
        }

        /// <summary>
        /// Handle order events from IB
        /// </summary>
        private void HandleOrderStatusUpdates(object sender, IB.OrderStatusEventArgs update)
        {
            try
            {
                // let's unblock the waiting thread right away
                if (_pendingOrderResponse.TryRemove(update.OrderId, out var eventSlim))
                {
                    eventSlim.Set();
                }

                Log.Trace($"InteractiveBrokersBrokerage.HandleOrderStatusUpdates(): {update}");

                if (!CheckIfConnected())
                {
                    return;
                }

                if (!TryGetLeanOrder(update.OrderId, nameof(HandleOrderStatusUpdates), out var orders))
                {
                    return;
                }

                var firstOrder = orders[0];
                if (firstOrder is StopLimitOrder stopLimitOrder && update.Status == IB.OrderStatus.PreSubmitted && !stopLimitOrder.StopTriggered)
                {
                    _preSubmittedStopLimitOrders.TryAdd(firstOrder.Id, stopLimitOrder);
                }

                var status = ConvertOrderStatus(update.Status);

                // Let's remove the contract for combo orders when they are canceled or filled
                if (firstOrder.GroupOrderManager != null && (status == OrderStatus.Filled || status == OrderStatus.Canceled))
                {
                    _comboOrdersContracts.TryRemove(firstOrder.GroupOrderManager.Id, out _);
                }

                if (status == OrderStatus.Filled || status == OrderStatus.PartiallyFilled)
                {
                    // fill events will be only processed in HandleExecutionDetails and HandleCommissionReports.
                    // but first, we want to check whether stop limit orders were triggered.
                    TryUpdateStopLimitTriggered(firstOrder, update.Status);

                    return;
                }

                var orderEvents = new List<OrderEvent>(orders.Count);
                foreach (var order in orders)
                {
                    // if we get a Submitted status and we had placed an order update, this new event is flagged as an update
                    var isUpdate = status == OrderStatus.Submitted && _orderUpdates.TryRemove(order.Id, out _);
                    // since there is not Lean status change when limit stop is triggered, we need to check the IB status
                    var limitStopTriggered = update.Status == IB.OrderStatus.Submitted && _preSubmittedStopLimitOrders.ContainsKey(order.Id);

                    // IB likes to duplicate/triplicate some events, so we fire non-fill events only if status changed
                    if (status != order.Status || isUpdate || limitStopTriggered)
                    {
                        if (order.Status.IsClosed())
                        {
                            // if the order is already in a closed state, we ignore the event
                            Log.Trace("InteractiveBrokersBrokerage.HandleOrderStatusUpdates(): ignoring update in closed state - order.Status: " + order.Status + ", status: " + status);
                        }
                        else if (order.Status == OrderStatus.PartiallyFilled && (status == OrderStatus.New || status == OrderStatus.Submitted) && !isUpdate)
                        {
                            // if we receive a New or Submitted event when already partially filled, we ignore it
                            Log.Trace("InteractiveBrokersBrokerage.HandleOrderStatusUpdates(): ignoring status " + status + " after partial fills");
                        }
                        else if (!TryUpdateStopLimitTriggered(order, update.Status))
                        {
                            orderEvents.Add(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Interactive Brokers Order Event")
                            {
                                Status = isUpdate ? OrderStatus.UpdateSubmitted : status
                            });
                        }
                    }
                }

                // fire the events
                if (orderEvents.Count > 0)
                {
                    OnOrderEvents(orderEvents);
                }
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.HandleOrderStatusUpdates(): " + err);
            }
        }

        /// <summary>
        /// Checks whether a given order is a stop limit order that was triggered and updates the order accordingly
        /// </summary>
        private bool TryUpdateStopLimitTriggered(Order order, string ibStatus)
        {
            if (order.Type == OrderType.StopLimit &&
                // Make sure the IB status changed from PreSubmitted to Submitted, which indicates the stop was triggered
                // and also allows to communicate Lean New -> Submitted status change back to the algorithm.
                ibStatus == IB.OrderStatus.Submitted &&
                _preSubmittedStopLimitOrders.TryRemove(order.Id, out _))
            {
                OnOrderUpdated(new OrderUpdateEvent { OrderId = order.Id, StopTriggered = true });
                return true;
            }

            return false;
        }

        /// <summary>
        /// Handle OpenOrder event from IB
        /// </summary>
        private void HandleOpenOrder(object sender, IB.OpenOrderEventArgs e)
        {
            try
            {
                Log.Trace($"InteractiveBrokersBrokerage.HandleOpenOrder(): {e}");

                if (!CheckIfConnected())
                {
                    // before we call get open orders which might not be fully initialized/loaded
                    return;
                }

                if (!TryGetLeanOrder(e.Order.OrderId, nameof(HandleOpenOrder), out var orders, e.Order.FaGroup))
                {
                    return;
                }

                if (orders[0].GroupOrderManager != null)
                {
                    _comboOrdersContracts[orders[0].GroupOrderManager.Id] = e.Contract;
                    return;
                }

                // the only changes we handle now are trail stop price updates
                var order = orders[0];
                if (order.Type != OrderType.TrailingStop)
                {
                    return;
                }

                var updatedStopPrice = e.Order.TrailStopPrice;
                var trailingStopOrder = (TrailingStopOrder)order;
                if ((double)trailingStopOrder.StopPrice != updatedStopPrice)
                {
                    OnOrderUpdated(new OrderUpdateEvent { OrderId = trailingStopOrder.Id, TrailingStopPrice = updatedStopPrice.SafeDecimalCast() });
                }
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.HandleOpenOrder(): " + err);
            }
        }

        /// <summary>
        /// Checks whether the client is connected and logs an error if not
        /// </summary>
        private bool CheckIfConnected()
        {
            if (!IsConnected)
            {
                if (_client != null)
                {
                    Log.Error($"InteractiveBrokersBrokerage.HandleOpenOrder(): Not connected; update dropped, _client.Connected: {_client.Connected}, _disconnected1100Fired: {_stateManager.Disconnected1100Fired}");
                }
                else
                {
                    Log.Error("InteractiveBrokersBrokerage.HandleOpenOrder(): Not connected; _client is null");
                }
                return false;
            }

            return true;
        }

        /// <summary>
        /// Attempts to retrieve Lean orders associated with the given brokerage order ID.
        /// Skips processing if the order was previously filtered by FA group,
        /// or if no corresponding Lean order can be found.
        /// </summary>
        /// <param name="brokerageOrderId">The brokerage-specific order ID.</param>
        /// <param name="caller">The name of the calling method, used for logging purposes.</param>
        /// <param name="leanOrders">The list of Lean orders associated with the brokerage ID, if found.</param>
        /// <param name="faGroup">Optional FA group associated with the order.</param>
        /// <returns><c>true</c> if the order is valid and found; otherwise, <c>false</c>.</returns>
        private bool TryGetLeanOrder(int brokerageOrderId, string caller, out List<Order> leanOrders, string faGroup = default)
        {
            leanOrders = default;

            // Ignore orders previously skipped due to FA group filtering
            if (_skippedOrdersByFaGroup.ContainsKey(brokerageOrderId) || TrySkipOrderByFaGroup(brokerageOrderId, faGroup))
            {
                return false;
            }

            leanOrders = _orderProvider.GetOrdersByBrokerageId(brokerageOrderId);
            if (leanOrders == null || leanOrders.Count == 0)
            {
                Log.Error($"InteractiveBrokersBrokerage.{caller}(): Unable to locate order with BrokerageID {brokerageOrderId}");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Determines whether the order should be skipped based on FA group filtering,
        /// and logs the reason if applicable.
        /// </summary>
        /// <param name="orderId">The brokerage order ID.</param>
        /// <param name="faGroup">The FA group of the order.</param>
        /// <returns><c>true</c> if the order was skipped; otherwise, <c>false</c>.</returns>
        private bool TrySkipOrderByFaGroup(int orderId, string faGroup)
        {
            if (IsFaGroupFlitterSet(faGroup))
            {
                if (_skippedOrdersByFaGroup.TryAdd(orderId, faGroup))
                {
                    Log.Trace($"InteractiveBrokersBrokerage.TrySkipOrderByFaGroup(): Skipping order {orderId} from FA group '{faGroup}' - does not match active filter: '{_financialAdvisorsGroupFilter}'");
                }
                return true;
            }

            return false;
        }

        /// <summary>
        /// Handle OpenOrderEnd event from IB
        /// </summary>
        private static void HandleOpenOrderEnd(object sender, EventArgs e)
        {
            Log.Trace("InteractiveBrokersBrokerage.HandleOpenOrderEnd()");
        }

        /// <summary>
        /// Handle execution events from IB
        /// </summary>
        /// <remarks>
        /// This needs to be handled because if a market order is executed immediately, there will be no OrderStatus event
        /// https://interactivebrokers.github.io/tws-api/order_submission.html#order_status
        /// </remarks>
        private void HandleExecutionDetails(object sender, IB.ExecutionDetailsEventArgs executionDetails)
        {
            try
            {
                // There are not guaranteed to be orderStatus callbacks for every change in order status. For example with market orders when the order is accepted and executes immediately,
                // there commonly will not be any corresponding orderStatus callbacks. For that reason it is recommended to monitor the IBApi.EWrapper.execDetails function in addition to
                // IBApi.EWrapper.orderStatus. From IB API docs
                // let's unblock the waiting thread right away
                if (_pendingOrderResponse.TryRemove(executionDetails.Execution.OrderId, out var eventSlim))
                {
                    eventSlim.Set();
                }

                Log.Trace("InteractiveBrokersBrokerage.HandleExecutionDetails(): " + executionDetails);

                if (!IsConnected)
                {
                    if (_client != null)
                    {
                        Log.Error($"InteractiveBrokersBrokerage.HandleExecutionDetails(): Not connected; update dropped, _client.Connected: {_client.Connected}, _disconnected1100Fired: {_stateManager.Disconnected1100Fired}");
                    }
                    else
                    {
                        Log.Error("InteractiveBrokersBrokerage.HandleExecutionDetails(): Not connected; _client is null");
                    }
                    return;
                }

                if (executionDetails.Contract.SecType == IB.SecurityType.Bag)
                {
                    // for combo order we get an initial event but later we get a global event for each leg in the combo
                    return;
                }

                var order = GetOrder(executionDetails);
                if (order == null)
                {
                    return;
                }

                // For financial advisor orders, we first receive executions and commission reports for the master order,
                // followed by executions and commission reports for all allocations.
                // We don't want to emit fills for these allocation events,
                // so we ignore events received after the order is completely filled or
                // executions for allocations which are already included in the master execution.

                if (_commissionReports.TryGetValue(executionDetails.Execution.ExecId, out var commissionReport))
                {
                    if (CanEmitFill(order, executionDetails.Execution))
                    {
                        // we have both execution and commission report, emit the fill
                        EmitOrderFill(order, executionDetails, commissionReport);
                    }

                    _commissionReports.TryRemove(commissionReport.ExecId, out commissionReport);
                }
                else
                {
                    // save execution in dictionary and wait for commission report
                    _orderExecutions[executionDetails.Execution.ExecId] = executionDetails;
                }
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.HandleExecutionDetails(): " + err);
            }
        }

        /// <summary>
        /// Handle commission report events from IB
        /// </summary>
        /// <remarks>
        /// This method matches commission reports with previously saved executions and fires the OrderEvents.
        /// </remarks>
        private void HandleCommissionReport(object sender, IB.CommissionReportEventArgs e)
        {
            try
            {
                Log.Trace("InteractiveBrokersBrokerage.HandleCommissionReport(): " + e);

                IB.ExecutionDetailsEventArgs executionDetails;
                if (!_orderExecutions.TryGetValue(e.CommissionReport.ExecId, out executionDetails))
                {
                    // save commission in dictionary and wait for execution event
                    _commissionReports[e.CommissionReport.ExecId] = e.CommissionReport;
                    return;
                }

                if (executionDetails.Contract.SecType == IB.SecurityType.Bag)
                {
                    // for combo order we get an initial event but later we get a global event for each leg in the combo
                    return;
                }

                var order = GetOrder(executionDetails);
                if (order == null)
                {
                    return;
                }

                if (CanEmitFill(order, executionDetails.Execution))
                {
                    // we have both execution and commission report, emit the fill
                    EmitOrderFill(order, executionDetails, e.CommissionReport);
                }

                // always remove previous execution
                _orderExecutions.TryRemove(e.CommissionReport.ExecId, out executionDetails);
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.HandleCommissionReport(): " + err);
            }
        }

        private Order GetOrder(IB.ExecutionDetailsEventArgs executionDetails)
        {
            var mappedSymbol = MapSymbol(executionDetails.Contract);
            var orders = _orderProvider.GetOrdersByBrokerageId(executionDetails.Execution.OrderId);
            var order = orders.Count == 1
                ? orders[0]
                : orders.SingleOrDefault(o => o.Symbol == mappedSymbol);

            if (order == null)
            {
                if (executionDetails.Execution.Liquidation == 1)
                {
                    var currentQuantityFilled = Convert.ToInt32(executionDetails.Execution.Shares);
                    if (executionDetails.Execution.Side == "SLD")
                    {
                        // BOT for bought, SLD for sold
                        currentQuantityFilled *= -1;
                    }
                    order = new MarketOrder(mappedSymbol, currentQuantityFilled, DateTime.UtcNow, "Brokerage Liquidation");
                    // this event will add the order into the lean engine
                    OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(order));
                }
                else if (orders.Count == 0)
                {
                    Log.Error($"InteractiveBrokersBrokerage.HandleExecutionDetails(): Unable to locate order with BrokerageID {executionDetails.Execution.OrderId}");
                }
                else
                {
                    Log.Error($"InteractiveBrokersBrokerage.HandleExecutionDetails(): Unable to locate order with BrokerageID {executionDetails.Execution.OrderId} " +
                        $"for symbol {mappedSymbol.ID}. Actual securities traded with this order ID are {string.Join(", ", orders.Select(x => x.Symbol.ID))}");
                }
            }

            return order;
        }

        /// <summary>
        /// Decide which fills should be emitted, accounting for different types of Financial Advisor orders
        /// </summary>
        private bool CanEmitFill(Order order, Execution execution)
        {
            if (order.Status == OrderStatus.Filled)
                return false;

            // non-FA orders
            if (!IsFinancialAdvisor)
                return true;

            var orderProperties = order.Properties as InteractiveBrokersOrderProperties;
            if (orderProperties == null)
                return true;

            return
                // FA master orders for groups/profiles
                string.IsNullOrWhiteSpace(orderProperties.Account) && execution.AcctNumber == _account ||

                // FA orders for single managed accounts
                !string.IsNullOrWhiteSpace(orderProperties.Account) && execution.AcctNumber == orderProperties.Account;
        }

        private Order TryGetOrderForFilling(int orderId)
        {
            if (_pendingGroupOrdersForFilling.TryGetValue(orderId, out var fillingParameters))
            {
                return fillingParameters.First().Order;
            }
            return null;
        }

        private List<PendingFillEvent> RemoveCachedOrdersForFilling(Order order)
        {
            if (order.GroupOrderManager == null)
            {
                // not a combo order
                _pendingGroupOrdersForFilling.TryGetValue(order.Id, out var details);
                _pendingGroupOrdersForFilling.Remove(order.Id);
                return details;
            }

            var pendingOrdersFillDetails = new List<PendingFillEvent>(order.GroupOrderManager.OrderIds.Count);

            foreach (var ordersId in order.GroupOrderManager.OrderIds)
            {
                if (_pendingGroupOrdersForFilling.TryGetValue(ordersId, out var details))
                {
                    pendingOrdersFillDetails.AddRange(details);
                    _pendingGroupOrdersForFilling.Remove(ordersId);
                }
            }

            return pendingOrdersFillDetails;
        }

        /// <summary>
        /// Emits an order fill (or partial fill) including the actual IB commission paid.
        ///
        /// For combo orders, it will wait until all the orders in the group have filled before emitting the events.
        /// If all orders in the group are not filled within a certain time, it will emit the events for the orders that have filled so far.
        /// </summary>
        private void EmitOrderFill(Order order, IB.ExecutionDetailsEventArgs executionDetails, CommissionAndFeesReport commissionReport, bool forceFillEmission = false)
        {
            List<PendingFillEvent> pendingOrdersFillDetails = null;

            lock (_pendingGroupOrdersForFilling)
            {
                if (!forceFillEmission)
                {
                    if (!_pendingGroupOrdersForFilling.TryGetValue(order.Id, out var pendingFillsForOrder))
                    {
                        _pendingGroupOrdersForFilling[order.Id] = pendingFillsForOrder = new();
                    }
                    pendingFillsForOrder.Add(new PendingFillEvent { Order = order, ExecutionDetails = executionDetails, CommissionReport = commissionReport });

                    // if this is a combo order, we will try to wait for all orders to fill before emitting the events
                    if (!order.TryGetGroupOrders(TryGetOrderForFilling, out _))
                    {
                        _comboOrdersFillTimeoutMonitor.AddPendingFill(order, executionDetails, commissionReport);
                        return;
                    }
                }

                pendingOrdersFillDetails = RemoveCachedOrdersForFilling(order);
            }

            var fillEvents = new List<OrderEvent>(pendingOrdersFillDetails.Count);

            for (var i = 0; i < pendingOrdersFillDetails.Count; i++)
            {
                var fillDetails = pendingOrdersFillDetails[i];

                var targetOrder = fillDetails.Order;
                var targetOrderExecutionDetails = fillDetails.ExecutionDetails;
                var targetOrderCommissionReport = fillDetails.CommissionReport;

                var absoluteQuantity = targetOrder.AbsoluteQuantity;
                var currentQuantityFilled = Convert.ToInt32(targetOrderExecutionDetails.Execution.Shares);
                var totalQuantityFilled = Convert.ToInt32(targetOrderExecutionDetails.Execution.CumQty);
                var remainingQuantity = Convert.ToInt32(absoluteQuantity - totalQuantityFilled);
                var price = NormalizePriceToLean(targetOrderExecutionDetails.Execution.Price, targetOrder.Symbol);
                var orderFee = new OrderFee(new CashAmount(
                    Convert.ToDecimal(targetOrderCommissionReport.CommissionAndFees),
                    targetOrderCommissionReport.Currency.ToUpperInvariant()));

                // set order status based on remaining quantity
                var status = remainingQuantity > 0 ? OrderStatus.PartiallyFilled : OrderStatus.Filled;

                // mark sells as negative quantities
                var fillQuantity = targetOrder.Direction == OrderDirection.Buy ? currentQuantityFilled : -currentQuantityFilled;
                var orderEvent = new OrderEvent(targetOrder, DateTime.UtcNow, orderFee, "Interactive Brokers Order Fill Event")
                {
                    Status = status,
                    FillPrice = price,
                    FillQuantity = fillQuantity
                };
                if (remainingQuantity != 0)
                {
                    orderEvent.Message += " - " + remainingQuantity + " remaining";
                }

                fillEvents.Add(orderEvent);
            }

            if (fillEvents.Count > 0)
            {
                // fire the order fill events
                OnOrderEvents(fillEvents);
            }
        }

        /// <summary>
        /// Handle portfolio changed events from IB
        /// </summary>
        private void HandlePortfolioUpdates(object sender, IB.UpdatePortfolioEventArgs e)
        {
            try
            {
                Log.Trace($"InteractiveBrokersBrokerage.HandlePortfolioUpdates(): {e}");

                // notify the transaction handler about all option position updates
                if (e.Contract.SecType is IB.SecurityType.Option or IB.SecurityType.FutureOption)
                {
                    var symbol = MapSymbol(e.Contract);

                    OnOptionNotification(new OptionNotificationEventArgs(symbol, e.Position));
                }

                _accountHoldingsResetEvent.Reset();
                if (_loadExistingHoldings)
                {
                    var holding = CreateHolding(e);
                    MergeHolding(_accountData.AccountHoldings, holding, e.AccountName);
                }
            }
            catch (Exception exception)
            {
                var contractStr = e.Contract?.ToString();
                lock (_invalidContracts)
                {
                    // only log these exceptions once per contracts to avoid a lot of noise in the logs we get them many times
                    if (string.IsNullOrEmpty(contractStr) || _invalidContracts.Add(contractStr))
                    {
                        Log.Error($"InteractiveBrokersBrokerage.HandlePortfolioUpdates(): {exception}");
                    }
                }

                if (e.Position != 0)
                {
                    // Force a runtime error only with a nonzero position for an unsupported security type,
                    // because after the user has manually closed the position and restarted the algorithm,
                    // he'll have a zero position but a nonzero realized PNL, so this event handler will be called again.

                    if (_algorithm == null || !_algorithm.Settings.IgnoreUnknownAssetHoldings)
                    {
                        _accountHoldingsLastException = exception;
                        _accountHoldingsResetEvent.Set();
                    }
                    else
                    {
                        CheckContractConversionError(exception, e.Contract, rethrow: false);
                    }
                }
            }
        }

        /// <summary>
        /// Merges a holding into the current holdings dictionary.
        /// If the symbol already exists, quantities are summed and a new weighted average price is calculated.
        /// If it's a new symbol, the holding is added directly.
        /// </summary>
        /// <param name="holdings">The dictionary of current holdings, keyed by <see cref="Symbol.Value"/>.</param>
        /// <param name="incoming">The incoming holding to merge, typically from an account or FA group update.</param>
        /// <remarks>
        /// This method is used when multiple updates for the same symbol can be received,
        /// such as when FA (Financial Advisor) group accounts return positions for multiple sub-accounts.
        /// </remarks>
        internal static void MergeHolding(IDictionary<Symbol, InteractiveBrokersAccountData.MergedHoldings> holdings, Holding incoming, string accountName)
        {
            lock (holdings)
            {
                if (!holdings.TryGetValue(incoming.Symbol, out var mergedHoldings))
                {
                    holdings[incoming.Symbol] = mergedHoldings = new InteractiveBrokersAccountData.MergedHoldings();
                }
                mergedHoldings.Merge(incoming, accountName);
            }
        }

        /// <summary>
        /// Converts a QC order to an IB order
        /// </summary>
        private IBApi.Order ConvertOrder(List<Order> orders, Contract contract, int ibOrderId)
        {
            OrderDirection direction;
            decimal quantity;

            var order = orders[0];
            if (order.GroupOrderManager != null)
            {
                quantity = order.GroupOrderManager.Quantity;
                direction = order.GroupOrderManager.Direction;
            }
            else
            {
                direction = order.Direction;
                quantity = order.Quantity;
            }

            var outsideRth = false;
            var orderProperties = order.Properties as InteractiveBrokersOrderProperties;
            if (order.Type == OrderType.Limit ||
                order.Type == OrderType.LimitIfTouched ||
                order.Type == OrderType.StopMarket ||
                order.Type == OrderType.StopLimit ||
                order.Type == OrderType.TrailingStop)
            {
                if (orderProperties != null)
                {
                    outsideRth = orderProperties.OutsideRegularTradingHours;
                }
            }

            var ibOrder = new IBApi.Order
            {
                ClientId = ClientId,
                OrderId = ibOrderId,
                Account = _account,
                Action = ConvertOrderDirection(direction),
                TotalQuantity = (int)Math.Abs(quantity),
                OrderType = ConvertOrderType(order.Type),
                AllOrNone = false,
                Tif = ConvertTimeInForce(order),
                Transmit = true,
                Rule80A = _agentDescription,
                OutsideRth = outsideRth
            };

            var gtdTimeInForce = order.TimeInForce as GoodTilDateTimeInForce;
            if (gtdTimeInForce != null)
            {
                DateTime expiryUtc;
                if (order.SecurityType == SecurityType.Forex)
                {
                    expiryUtc = gtdTimeInForce.GetForexOrderExpiryDateTime(order);
                }
                else
                {
                    var exchangeHours = MarketHoursDatabase.FromDataFolder()
                        .GetExchangeHours(order.Symbol.ID.Market, order.Symbol, order.SecurityType);

                    var expiry = exchangeHours.GetNextMarketClose(gtdTimeInForce.Expiry.Date, false);
                    expiryUtc = expiry.ConvertToUtc(exchangeHours.TimeZone);
                }

                // The IB format for the GoodTillDate order property is "yyyymmdd hh:mm:ss xxx" where yyyymmdd and xxx are optional.
                // E.g.: 20031126 15:59:00 EST
                // If no date is specified, current date is assumed. If no time-zone is specified, local time-zone is assumed.

                ibOrder.GoodTillDate = expiryUtc.ToString("yyyyMMdd HH:mm:ss UTC", CultureInfo.InvariantCulture);
            }

            var limitOrder = order as LimitOrder;
            var stopMarketOrder = order as StopMarketOrder;
            var stopLimitOrder = order as StopLimitOrder;
            var trailingStopOrder = order as TrailingStopOrder;
            var limitIfTouchedOrder = order as LimitIfTouchedOrder;
            var comboLimitOrder = order as ComboLimitOrder;
            var comboLegLimitOrder = order as ComboLegLimitOrder;
            var comboMarketOrder = order as ComboMarketOrder;
            if (comboLegLimitOrder != null)
            {
                // Combo per-leg prices are only supported for non-guaranteed smart combos with two legs
                AddGuaranteedTag(ibOrder, true);

                ibOrder.OrderComboLegs = new();
                foreach (var comboLegLimit in orders.OfType<ComboLegLimitOrder>().OrderBy(o => o.Id))
                {
                    var legContract = CreateContract(comboLegLimit.Symbol, includeExpired: false);

                    ibOrder.OrderComboLegs.Add(new OrderComboLeg
                    {
                        Price = NormalizePriceToBrokerage(comboLegLimit.LimitPrice, legContract, comboLegLimit.Symbol)
                    });
                }
            }
            else if (limitOrder != null)
            {
                ibOrder.LmtPrice = NormalizePriceToBrokerage(limitOrder.LimitPrice, contract, order.Symbol);
            }
            // check trailing stop before stop market because TrailingStopOrder inherits StopMarketOrder
            else if (trailingStopOrder != null)
            {
                ibOrder.TrailStopPrice = NormalizePriceToBrokerage(trailingStopOrder.StopPrice, contract, order.Symbol);
                if (trailingStopOrder.TrailingAsPercentage)
                {
                    ibOrder.TrailingPercent = (double)trailingStopOrder.TrailingAmount * 100;
                }
                else
                {
                    ibOrder.AuxPrice = NormalizePriceToBrokerage(trailingStopOrder.TrailingAmount, contract, order.Symbol);
                }
            }
            else if (stopMarketOrder != null)
            {
                ibOrder.AuxPrice = NormalizePriceToBrokerage(stopMarketOrder.StopPrice, contract, order.Symbol);
            }
            else if (stopLimitOrder != null)
            {
                ibOrder.LmtPrice = NormalizePriceToBrokerage(stopLimitOrder.LimitPrice, contract, order.Symbol);
                ibOrder.AuxPrice = NormalizePriceToBrokerage(stopLimitOrder.StopPrice, contract, order.Symbol);
            }
            else if (limitIfTouchedOrder != null)
            {
                ibOrder.LmtPrice = NormalizePriceToBrokerage(limitIfTouchedOrder.LimitPrice, contract, order.Symbol);
                ibOrder.AuxPrice = NormalizePriceToBrokerage(limitIfTouchedOrder.TriggerPrice, contract, order.Symbol);
            }
            else if (comboLimitOrder != null)
            {
                AddGuaranteedTag(ibOrder, orders.All(x => x.SecurityType == SecurityType.Equity));
                var baseContract = CreateContract(order.Symbol, includeExpired: false);
                ibOrder.LmtPrice = NormalizePriceToBrokerage(comboLimitOrder.GroupOrderManager.LimitPrice, baseContract, order.Symbol);
            }
            else if (comboMarketOrder != null)
            {
                AddGuaranteedTag(ibOrder, orders.All(x => x.SecurityType == SecurityType.Equity));
            }

            // add financial advisor properties
            if (IsFinancialAdvisor)
            {
                // https://interactivebrokers.github.io/tws-api/financial_advisor.html#gsc.tab=0
                if (!string.IsNullOrEmpty(_financialAdvisorsGroupFilter))
                {
                    ibOrder.FaGroup = _financialAdvisorsGroupFilter;
                }

                if (orderProperties != null)
                {
                    if (!_sentFAOrderPropertiesWarning &&
                        (!string.IsNullOrWhiteSpace(orderProperties.FaProfile) && !string.IsNullOrWhiteSpace(orderProperties.Account)
                        || !string.IsNullOrWhiteSpace(orderProperties.FaProfile) && !string.IsNullOrWhiteSpace(orderProperties.FaGroup)
                        || !string.IsNullOrWhiteSpace(orderProperties.Account) && !string.IsNullOrWhiteSpace(orderProperties.FaGroup)))
                    {
                        // warning these are mutually exclusive
                        _sentFAOrderPropertiesWarning = true;
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "PlaceOrder",
                            "Order properties 'FaProfile', 'FaGroup' & 'Account' are mutually exclusive"));
                    }

                    if (!string.IsNullOrWhiteSpace(orderProperties.Account))
                    {
                        // order for a single managed account
                        ibOrder.Account = orderProperties.Account;
                    }
                    else if (!string.IsNullOrWhiteSpace(orderProperties.FaGroup) || !string.IsNullOrWhiteSpace(orderProperties.FaProfile))
                    {
                        // order for an account group
                        ibOrder.FaGroup = orderProperties.FaGroup;
                        if (string.IsNullOrWhiteSpace(ibOrder.FaGroup))
                        {
                            // we were given a profile
                            ibOrder.FaGroup = orderProperties.FaProfile;
                        }

                        ibOrder.FaMethod = orderProperties.FaMethod;

                        if (ibOrder.FaMethod == "PctChange")
                        {
                            ibOrder.FaPercentage = orderProperties.FaPercentage.ToStringInvariant();
                            ibOrder.TotalQuantity = 0;
                        }
                    }
                    // IB docs say "Use an empty string if not applicable."  https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-ref/#order-ref
                    ibOrder.FaMethod ??= string.Empty;
                    ibOrder.FaGroup ??= string.Empty;
                    ibOrder.Account ??= string.Empty;
                }
            }

            // not yet supported
            //ibOrder.ParentId =
            //ibOrder.OcaGroup =

            return ibOrder;
        }

        private List<Order> ConvertOrders(IBApi.Order ibOrder, Contract contract, OrderState orderState)
        {
            var result = new List<Order>();
            var quantitySign = ConvertOrderDirection(ibOrder.Action) == OrderDirection.Sell ? -1 : 1;
            var quantity = Convert.ToInt32(ibOrder.TotalQuantity) * quantitySign;

            if (contract.SecType == IB.SecurityType.Bag)
            {
                // this is a combo order
                var group = new GroupOrderManager(_algorithm.Transactions.GetIncrementGroupOrderManagerId(), contract.ComboLegs.Count, quantity);

                var orderType = OrderType.ComboMarket;
                if (ibOrder.LmtPrice != double.MaxValue)
                {
                    orderType = OrderType.ComboLimit;
                }
                else if (!ibOrder.OrderComboLegs.IsNullOrEmpty() && ibOrder.OrderComboLegs.Any(p => p.Price != double.MaxValue))
                {
                    orderType = OrderType.ComboLegLimit;
                }

                for (var i = 0; i < contract.ComboLegs.Count; i++)
                {
                    var comboLeg = contract.ComboLegs[i];
                    var comboLegContract = new Contract { ConId = comboLeg.ConId };
                    var contractDetails = GetContractDetails(comboLegContract, string.Empty);
                    var quantitySignLeg = ConvertOrderDirection(comboLeg.Action) == OrderDirection.Sell ? -1 : 1;

                    var legLimitPrice = ibOrder.LmtPrice;
                    if (ibOrder.OrderComboLegs.Count == contract.ComboLegs.Count)
                    {
                        legLimitPrice = ibOrder.OrderComboLegs[i].Price;
                    }

                    if (!TryConvertOrder(ibOrder.Tif, ibOrder.GoodTillDate, ibOrder.OrderId, ibOrder.AuxPrice, orderType,
                            comboLeg.Ratio * quantitySignLeg * quantity, legLimitPrice, 0, 0, contractDetails.Contract, group, orderState,
                            out var leanOrder))
                    {
                        // if we fail to convert one leg, we fail the whole order
                        return new List<Order>();
                    }

                    result.Add(leanOrder);
                }
            }
            else if (TryConvertOrder(ibOrder.Tif, ibOrder.GoodTillDate, ibOrder.OrderId, ibOrder.AuxPrice, ConvertOrderType(ibOrder), quantity,
                ibOrder.LmtPrice, ibOrder.TrailStopPrice, ibOrder.TrailingPercent, contract, null, orderState, out var leanOrder))
            {
                result.Add(leanOrder);
            }

            return result;
        }

        private void CheckContractConversionError(Exception exception, Contract contract, bool rethrow = true)
        {
            var notSupportedException = exception as NotSupportedException;
            notSupportedException ??= exception.InnerException as NotSupportedException;
            if (notSupportedException != null && _algorithm.Settings.IgnoreUnknownAssetHoldings)
            {
                lock (_unsupportedAssets)
                {
                    if (contract == null || _unsupportedAssets.Add($"{contract.Symbol}-{contract.SecType}"))
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "ConvertOrders", notSupportedException.Message));
                    }
                }
            }
            else if (rethrow)
            {
                throw exception;
            }
        }

        private bool TryConvertOrder(string timeInForce, string goodTillDate, int ibOrderId, double auxPrice, OrderType orderType, decimal quantity,
            double limitPrice, double trailingStopPrice, double trailingPercentage, Contract contract, GroupOrderManager groupOrderManager, OrderState orderState,
            out Order leanOrder)
        {
            try
            {
                leanOrder = ConvertOrder(timeInForce, goodTillDate, ibOrderId, auxPrice, orderType, quantity,
                    limitPrice, trailingStopPrice, trailingPercentage, contract, groupOrderManager, orderState);
                return true;
            }
            catch (Exception ex)
            {
                leanOrder = null;
                CheckContractConversionError(ex, contract);
                return false;
            }
        }

        private Order ConvertOrder(string timeInForce, string goodTillDate, int ibOrderId, double auxPrice, OrderType orderType, decimal quantity,
            double limitPrice, double trailingStopPrice, double trailingPercentage, Contract contract, GroupOrderManager groupOrderManager, OrderState orderState)
        {
            // this function is called by GetOpenOrders which is mainly used by the setup handler to
            // initialize algorithm state.  So the only time we'll be executing this code is when the account
            // has orders sitting and waiting from before algo initialization...
            // because of this we can't get the time accurately

            Order order;
            var mappedSymbol = MapSymbol(contract);
            switch (orderType)
            {
                case OrderType.Market:
                    order = new MarketOrder(mappedSymbol,
                        quantity,
                        new DateTime() // not sure how to get this data
                        );
                    break;

                case OrderType.MarketOnOpen:
                    order = new MarketOnOpenOrder(mappedSymbol,
                        quantity,
                        new DateTime());
                    break;

                case OrderType.MarketOnClose:
                    order = new MarketOnCloseOrder(mappedSymbol,
                        quantity,
                        new DateTime()
                        );
                    break;

                case OrderType.Limit:
                    order = new LimitOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(limitPrice, mappedSymbol),
                        new DateTime()
                        );
                    break;

                case OrderType.StopMarket:
                    order = new StopMarketOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(auxPrice, mappedSymbol),
                        new DateTime()
                        );
                    break;

                case OrderType.StopLimit:
                    order = new StopLimitOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(auxPrice, mappedSymbol),
                        NormalizePriceToLean(limitPrice, mappedSymbol),
                        new DateTime()
                        );
                    break;

                case OrderType.TrailingStop:
                    decimal trailingAmount;
                    bool trailingAsPecentage;
                    if (trailingPercentage != double.MaxValue)
                    {
                        trailingAmount = trailingPercentage.SafeDecimalCast() / 100m;
                        trailingAsPecentage = true;
                    }
                    else
                    {
                        trailingAmount = NormalizePriceToLean(auxPrice, mappedSymbol);
                        trailingAsPecentage = false;
                    }

                    order = new TrailingStopOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(trailingStopPrice, mappedSymbol),
                        trailingAmount,
                        trailingAsPecentage,
                        new DateTime()
                    );
                    break;

                case OrderType.LimitIfTouched:
                    order = new LimitIfTouchedOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(auxPrice, mappedSymbol),
                        NormalizePriceToLean(limitPrice, mappedSymbol),
                        new DateTime()
                    );
                    break;

                case OrderType.ComboMarket:
                    order = new ComboMarketOrder(mappedSymbol,
                        quantity,
                        new DateTime(),
                        groupOrderManager
                    );
                    break;

                case OrderType.ComboLimit:
                    order = new ComboLimitOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(limitPrice, mappedSymbol),
                        new DateTime(),
                        groupOrderManager
                    );
                    break;

                case OrderType.ComboLegLimit:
                    order = new ComboLegLimitOrder(mappedSymbol,
                        quantity,
                        NormalizePriceToLean(limitPrice, mappedSymbol),
                        new DateTime(),
                        groupOrderManager
                    );
                    break;

                default:
                    throw new InvalidEnumArgumentException("orderType", (int)orderType, typeof(OrderType));
            }

            order.BrokerId.Add(ibOrderId.ToStringInvariant());
            order.Properties.TimeInForce = ConvertTimeInForce(timeInForce, goodTillDate);
            order.Status = ConvertOrderStatus(orderState.Status);

            return order;
        }

        /// <summary>
        /// Creates an IB contract from the order.
        /// </summary>
        /// <param name="symbol">The symbol whose contract we need to create</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="exchange">The exchange where the order will be placed, defaults to 'Smart'</param>
        /// <returns>A new IB contract for the order</returns>
        private Contract CreateContract(Symbol symbol, bool includeExpired, List<Order> orders = null, string exchange = null)
        {
            var securityType = ConvertSecurityType(symbol.SecurityType);
            var ibSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(
                symbol.ID.Market,
                symbol,
                symbol.SecurityType,
                Currencies.USD);

            var contract = new Contract
            {
                Symbol = ibSymbol,
                Exchange = exchange ?? GetSymbolExchange(symbol),
                SecType = securityType,
                Currency = symbolProperties.QuoteCurrency
            };

            if (symbol.ID.SecurityType == SecurityType.Forex)
            {
                // forex is special, so rewrite some of the properties to make it work
                contract.Exchange = "IDEALPRO";
                contract.Symbol = ibSymbol.Substring(0, 3);
                contract.Currency = ibSymbol.Substring(3);
            }
            else if (symbol.ID.SecurityType == SecurityType.Cfd)
            {
                // Let's try getting the contract details in order to get the type of CFD (stock, index or forex)
                var details = GetContractDetails(contract, symbol.Value, failIfNotFound: false);

                // if null, it might be a forex CFD, we need to split the symbol just like we do for forex
                if (details == null)
                {
                    contract.Exchange = "SMART";
                    contract.Symbol = ibSymbol.Substring(0, 3);
                    contract.Currency = ibSymbol.Substring(3);

                    details = GetContractDetails(contract, symbol.Value);

                    if (details == null || details.UnderSecType != IB.SecurityType.Cash)
                    {
                        Log.Error("InteractiveBrokersBrokerage.CreateContract(): Unable to resolve CFD symbol: " + symbol.Value);
                        return null;
                    }
                }
            }
            else if (symbol.ID.SecurityType == SecurityType.Equity)
            {
                contract.PrimaryExch = GetPrimaryExchange(contract, symbol);
            }
            // Indexes requires that the exchange be specified exactly
            else if (symbol.ID.SecurityType == SecurityType.Index)
            {
                if (string.Equals(symbol.ID.Market, Market.OSE, StringComparison.InvariantCultureIgnoreCase))
                {
                    contract.Exchange = "OSE.JPN";
                }
                else
                {
                    contract.Exchange = IndexSymbol.GetIndexExchange(symbol);
                }
            }
            else if (symbol.ID.SecurityType.IsOption())
            {
                // Subtract a day from Index Options, since their last trading date
                // is on the day before the expiry.
                var lastTradeDate = symbol.ID.Date;
                if (symbol.SecurityType == SecurityType.IndexOption)
                {
                    lastTradeDate = IndexOptionSymbol.GetLastTradingDate(symbol.ID.Symbol, symbol.ID.Date);
                }
                contract.LastTradeDateOrContractMonth = lastTradeDate.ToStringInvariant(DateFormat.EightCharacter);

                contract.Right = symbol.ID.OptionRight == OptionRight.Call ? IB.RightType.Call : IB.RightType.Put;

                if (symbol.ID.SecurityType == SecurityType.FutureOption)
                {
                    var underlyingContract = CreateContract(symbol.Underlying, includeExpired, null, exchange);
                    if (underlyingContract == null)
                    {
                        Log.Error($"CreateContract(): Failed to create the underlying future IB contract {symbol}");
                        return null;
                    }
                    contract.Strike = NormalizePriceToBrokerage(symbol.ID.StrikePrice, underlyingContract,
                        symbol.Underlying);
                }
                else
                {
                    contract.Strike = Convert.ToDouble(symbol.ID.StrikePrice);
                }
                contract.Symbol = ibSymbol;
                contract.Multiplier = GetSymbolProperties(symbol)
                    .ContractMultiplier
                    .ToStringInvariant();

                contract.TradingClass = _contractSpecificationService.GetTradingClass(contract, symbol);
                contract.IncludeExpired = includeExpired;
            }
            else if (symbol.ID.SecurityType == SecurityType.Future)
            {
                // we convert Market.* markets into IB exchanges if we have them in our map

                contract.Symbol = ibSymbol;
                contract.LastTradeDateOrContractMonth = symbol.ID.Date.ToStringInvariant(DateFormat.EightCharacter);
                contract.Exchange = GetSymbolExchange(symbol);

                contract.Multiplier = GetContractMultiplier(symbolProperties.ContractMultiplier);

                contract.IncludeExpired = includeExpired;
            }

            if (orders != null && orders.Count > 1)
            {
                // this is a combo order!
                contract.ComboLegs = new();
                contract.SecType = IB.SecurityType.Bag;
                foreach (var order in orders.OrderBy(o => o.Id))
                {
                    var legContract = CreateContract(order.Symbol, includeExpired: false);
                    var legContractDetails = GetContractDetails(legContract, order.Symbol.Value);
                    var ratio = order.Quantity.GetOrderLegRatio(order.GroupOrderManager);
                    contract.ComboLegs.Add(new ComboLeg
                    {
                        ConId = legContractDetails.Contract.ConId,
                        Action = ConvertOrderDirection(ratio > 0 ? OrderDirection.Buy : OrderDirection.Sell),
                        // the ratio is absolute the action above specifies if we are selling or buying
                        Ratio = Math.Abs((int)ratio),
                        Exchange = legContractDetails.Contract.Exchange
                    });
                }
            }

            return contract;
        }

        private SymbolProperties GetSymbolProperties(Symbol symbol)
        {
            return _symbolPropertiesDatabase.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType,
                        _algorithm != null ? _algorithm.Portfolio.CashBook.AccountCurrency : Currencies.USD);
        }

        /// <summary>
        /// Maps OrderDirection enumeration
        /// </summary>
        public static OrderDirection ConvertOrderDirection(string direction)
        {
            switch (direction)
            {
                case IB.ActionSide.Buy: return OrderDirection.Buy;
                case IB.ActionSide.Sell: return OrderDirection.Sell;
                case IB.ActionSide.Undefined: return OrderDirection.Hold;
                default:
                    throw new ArgumentException(direction, nameof(direction));
            }
        }

        /// <summary>
        /// Maps OrderDirection enumeration
        /// </summary>
        private static string ConvertOrderDirection(OrderDirection direction)
        {
            switch (direction)
            {
                case OrderDirection.Buy: return IB.ActionSide.Buy;
                case OrderDirection.Sell: return IB.ActionSide.Sell;
                case OrderDirection.Hold: return IB.ActionSide.Undefined;
                default:
                    throw new InvalidEnumArgumentException(nameof(direction), (int)direction, typeof(OrderDirection));
            }
        }

        /// <summary>
        /// Maps OrderType enum
        /// </summary>
        private static string ConvertOrderType(OrderType type)
        {
            switch (type)
            {
                case OrderType.Market: return IB.OrderType.Market;
                case OrderType.Limit: return IB.OrderType.Limit;
                case OrderType.StopMarket: return IB.OrderType.Stop;
                case OrderType.StopLimit: return IB.OrderType.StopLimit;
                case OrderType.TrailingStop: return IB.OrderType.TrailingStop;
                case OrderType.LimitIfTouched: return IB.OrderType.LimitIfTouched;
                case OrderType.MarketOnOpen: return IB.OrderType.Market;
                case OrderType.MarketOnClose: return IB.OrderType.MarketOnClose;
                case OrderType.ComboLegLimit: return IB.OrderType.Limit;
                case OrderType.ComboLimit: return IB.OrderType.Limit;
                case OrderType.ComboMarket: return IB.OrderType.Market;
                default:
                    throw new InvalidEnumArgumentException(nameof(type), (int)type, typeof(OrderType));
            }
        }

        /// <summary>
        /// Maps OrderType enum
        /// </summary>
        private static OrderType ConvertOrderType(IBApi.Order order)
        {
            switch (order.OrderType)
            {
                case IB.OrderType.Limit: return OrderType.Limit;
                case IB.OrderType.Stop: return OrderType.StopMarket;
                case IB.OrderType.StopLimit: return OrderType.StopLimit;
                case IB.OrderType.TrailingStop: return OrderType.TrailingStop;
                case IB.OrderType.LimitIfTouched: return OrderType.LimitIfTouched;
                case IB.OrderType.MarketOnClose: return OrderType.MarketOnClose;

                case IB.OrderType.Market:
                    if (order.Tif == IB.TimeInForce.MarketOnOpen)
                    {
                        return OrderType.MarketOnOpen;
                    }
                    return OrderType.Market;

                default:
                    throw new ArgumentException(order.OrderType, "order.OrderType");
            }
        }

        /// <summary>
        /// Maps TimeInForce from IB to LEAN
        /// </summary>
        private static TimeInForce ConvertTimeInForce(string timeInForce, string expiryDateTime)
        {
            switch (timeInForce)
            {
                case IB.TimeInForce.Day:
                    return TimeInForce.Day;

                case IB.TimeInForce.GoodTillDate:
                    return TimeInForce.GoodTilDate(ParseExpiryDateTime(expiryDateTime));

                //case IB.TimeInForce.FillOrKill:
                //    return TimeInForce.FillOrKill;

                //case IB.TimeInForce.ImmediateOrCancel:
                //    return TimeInForce.ImmediateOrCancel;

                case IB.TimeInForce.MarketOnOpen:
                case IB.TimeInForce.GoodTillCancel:
                default:
                    return TimeInForce.GoodTilCanceled;
            }
        }

        public static DateTime ParseExpiryDateTime(string expiryDateTime)
        {
            // NOTE: we currently ignore the time zone in this method for a couple of reasons:
            // - TZ abbreviations are ambiguous and unparsable to a unique time zone
            //   see this article for more info:
            //   https://codeblog.jonskeet.uk/2015/05/05/common-mistakes-in-datetime-formatting-and-parsing/
            // - IB seems to also have issues with Daylight Saving Time zones
            //   Example: an order submitted from Europe with GoodTillDate property set to "20180524 21:00:00 UTC"
            //   when reading the open orders, the same property will have this value: "20180524 23:00:00 CET"
            //   which is incorrect: should be CEST (UTC+2) instead of CET (UTC+1)

            // We can ignore this issue, because the method is only called by GetOpenOrders,
            // we only call GetOpenOrders during live trading, which means we won't be simulating time in force
            // and instead will rely on brokerages to apply TIF properly.

            var parts = expiryDateTime.Split(' ');
            if (parts.Length == 3)
            {
                expiryDateTime = expiryDateTime.Substring(0, expiryDateTime.LastIndexOf(" ", StringComparison.Ordinal));
            }

            return DateTime.ParseExact(expiryDateTime, "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture).Date;
        }

        /// <summary>
        /// Maps TimeInForce from LEAN to IB
        /// </summary>
        private static string ConvertTimeInForce(Order order)
        {
            if (order.Type == OrderType.MarketOnOpen)
            {
                return IB.TimeInForce.MarketOnOpen;
            }
            if (order.Type == OrderType.MarketOnClose)
            {
                return IB.TimeInForce.Day;
            }

            if (order.TimeInForce is DayTimeInForce)
            {
                return IB.TimeInForce.Day;
            }

            if (order.TimeInForce is GoodTilDateTimeInForce)
            {
                return IB.TimeInForce.GoodTillDate;
            }

            //if (order.TimeInForce is FillOrKillTimeInForce)
            //{
            //    return IB.TimeInForce.FillOrKill;
            //}

            //if (order.TimeInForce is ImmediateOrCancelTimeInForce)
            //{
            //    return IB.TimeInForce.ImmediateOrCancel;
            //}

            return IB.TimeInForce.GoodTillCancel;
        }

        /// <summary>
        /// Maps IB's OrderStats enum
        /// </summary>
        public static OrderStatus ConvertOrderStatus(string status)
        {
            switch (status)
            {
                case IB.OrderStatus.ApiPending:
                case IB.OrderStatus.PendingSubmit:
                    return OrderStatus.New;

                case IB.OrderStatus.PendingCancel:
                    return OrderStatus.CancelPending;

                case IB.OrderStatus.ApiCancelled:
                case IB.OrderStatus.Cancelled:
                    return OrderStatus.Canceled;

                case IB.OrderStatus.Submitted:
                case IB.OrderStatus.PreSubmitted:
                    return OrderStatus.Submitted;

                case IB.OrderStatus.Filled:
                    return OrderStatus.Filled;

                case IB.OrderStatus.PartiallyFilled:
                    return OrderStatus.PartiallyFilled;

                case IB.OrderStatus.Error:
                    return OrderStatus.Invalid;

                case IB.OrderStatus.Inactive:
                    Log.Error("InteractiveBrokersBrokerage.ConvertOrderStatus(): Inactive order");
                    return OrderStatus.None;

                case IB.OrderStatus.None:
                    return OrderStatus.None;

                // not sure how to map these guys
                default:
                    throw new ArgumentException(status, nameof(status));
            }
        }

        /// <summary>
        /// Maps SecurityType enum to an IBApi SecurityType value
        /// </summary>
        public static string ConvertSecurityType(SecurityType type)
        {
            switch (type)
            {
                case SecurityType.Equity:
                    return IB.SecurityType.Stock;

                case SecurityType.Option:
                case SecurityType.IndexOption:
                    return IB.SecurityType.Option;

                case SecurityType.Index:
                    return IB.SecurityType.Index;

                case SecurityType.FutureOption:
                    return IB.SecurityType.FutureOption;

                case SecurityType.Forex:
                    return IB.SecurityType.Cash;

                case SecurityType.Future:
                    return IB.SecurityType.Future;

                case SecurityType.Cfd:
                    return IB.SecurityType.ContractForDifference;

                default:
                    throw new ArgumentException($"The {type} security type is not currently supported.");
            }
        }

        /// <summary>
        /// Maps SecurityType enum
        /// </summary>
        private static SecurityType ConvertSecurityType(Contract contract)
        {
            return ConvertSecurityType(contract.SecType, contract.Symbol, GetContractDescription(contract));
        }

        /// <summary>
        /// Maps SecurityType enum
        /// </summary>
        public static SecurityType ConvertSecurityType(string securityType, string ticker, string error = null)
        {
            switch (securityType)
            {
                case IB.SecurityType.Stock:
                    return SecurityType.Equity;

                case IB.SecurityType.Option:
                    return IndexOptionSymbol.IsIndexOption(ticker)
                        ? SecurityType.IndexOption
                        : SecurityType.Option;

                case IB.SecurityType.Index:
                    return SecurityType.Index;

                case IB.SecurityType.FutureOption:
                    return SecurityType.FutureOption;

                case IB.SecurityType.Cash:
                    return SecurityType.Forex;

                case IB.SecurityType.Future:
                    return SecurityType.Future;

                case IB.SecurityType.ContractForDifference:
                    return SecurityType.Cfd;

                default:
                    throw new NotSupportedException(
                        $"An existing position or open order for an unsupported security type was found: {error ?? $"{securityType} {ticker}"}. " +
                        "Please manually close the position or cancel the order before restarting the algorithm.");
            }
        }

        /// <summary>
        /// Maps Resolution to IB representation
        /// </summary>
        /// <param name="resolution"></param>
        /// <returns></returns>
        private string ConvertResolution(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Tick:
                case Resolution.Second:
                    return IB.BarSize.OneSecond;

                case Resolution.Minute:
                    return IB.BarSize.OneMinute;

                case Resolution.Hour:
                    return IB.BarSize.OneHour;

                case Resolution.Daily:
                default:
                    return IB.BarSize.OneDay;
            }
        }

        /// <summary>
        /// Maps Resolution and history span to IB span
        /// </summary>
        /// <param name="historyRequestSpan">The history requests time span</param>
        /// <returns>The IB span</returns>
        public static string GetDuration(Resolution resolution, TimeSpan historyRequestSpan)
        {
            var days = (int)historyRequestSpan.TotalDays + 1;
            switch (resolution)
            {
                case Resolution.Tick:
                case Resolution.Second:
                    // seconds duration can't be smaller then 100
                    var maximum = Math.Max((int)historyRequestSpan.TotalSeconds + 1, 100);
                    // using second resolution doesn't accept higher duration
                    return $"{Math.Min(maximum, 2000)} S";

                case Resolution.Minute:
                    if (historyRequestSpan.TotalMinutes <= 120)
                    {
                        // seconds duration can't be smaller then 100
                        var seconds = Math.Max((int)historyRequestSpan.TotalSeconds + 60, 120);
                        // 120min
                        return $"{Math.Min(seconds, 7200)} S";
                    }
                    return $"{Math.Min(days, 7)} D";

                case Resolution.Hour:
                    if (days < 120)
                    {
                        return $"{days} D";
                    }
                    return "6 M";

                case Resolution.Daily:
                default:
                    // we can't send more than 365 days
                    if (days < 365)
                    {
                        return $"{days} D";
                    }
                    return "2 Y";
            }
        }

        private static TradeBar ConvertTradeBar(Symbol symbol, Resolution resolution, IB.HistoricalDataEventArgs historyBar, decimal priceMagnifier)
        {
            var time = resolution != Resolution.Daily ?
                Time.UnixTimeStampToDateTime(Convert.ToDouble(historyBar.Bar.Time, CultureInfo.InvariantCulture)) :
                DateTime.ParseExact(historyBar.Bar.Time, "yyyyMMdd", CultureInfo.InvariantCulture);

            return new TradeBar(time, symbol, (decimal)historyBar.Bar.Open / priceMagnifier, (decimal)historyBar.Bar.High / priceMagnifier,
                (decimal)historyBar.Bar.Low / priceMagnifier, (decimal)historyBar.Bar.Close / priceMagnifier, historyBar.Bar.Volume, resolution.ToTimeSpan());
        }

        /// <summary>
        /// Creates a holding object from the UpdatePortfolioEventArgs
        /// </summary>
        private Holding CreateHolding(IB.UpdatePortfolioEventArgs e)
        {
            var symbol = MapSymbol(e.Contract);

            var currencySymbol = Currencies.GetCurrencySymbol(
                e.Contract.Currency ??
                _symbolPropertiesDatabase.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, Currencies.USD).QuoteCurrency);

            var multiplier = e.Contract.Multiplier.ConvertInvariant<decimal>();
            if (multiplier == 0m) multiplier = 1m;

            return new Holding
            {
                Symbol = symbol,
                Quantity = e.Position,
                AveragePrice = Convert.ToDecimal(e.AverageCost) / multiplier,
                MarketPrice = Convert.ToDecimal(e.MarketPrice),
                CurrencySymbol = currencySymbol
            };
        }

        /// <summary>
        /// Maps the IB Contract's symbol to a QC symbol
        /// </summary>
        private Symbol MapSymbol(Contract contract)
        {
            try
            {
                var securityType = ConvertSecurityType(contract);

                var ibSymbol = contract.Symbol;
                if (securityType == SecurityType.Forex)
                {
                    ibSymbol += contract.Currency;
                }
                else if (securityType == SecurityType.Cfd)
                {
                    // If this is a forex CFD, we need to compose the symbol like we do for forex
                    var potentialCurrencyPair = contract.TradingClass.Replace(".", "");
                    if (CurrencyPairUtil.IsForexDecomposable(potentialCurrencyPair))
                    {
                        Forex.DecomposeCurrencyPair(potentialCurrencyPair, out var baseCurrency, out var quoteCurrency);
                        if (baseCurrency == contract.Symbol && quoteCurrency == contract.Currency)
                        {
                            ibSymbol += contract.Currency;
                        }
                    }
                }

                var market = InteractiveBrokersBrokerageModel.DefaultMarketMap[securityType];
                var isFutureOption = contract.SecType == IB.SecurityType.FutureOption;

                if (securityType.IsOption() && contract.LastTradeDateOrContractMonth == "0")
                {
                    // Try our best to recover from a malformed contract.
                    // You can read more about malformed contracts at the ParseMalformedContract method's documentation.
                    var exchange = GetSymbolExchange(securityType, market);

                    contract = InteractiveBrokersSymbolMapper.ParseMalformedContractOptionSymbol(contract, exchange);
                    ibSymbol = contract.Symbol;
                }
                else if (securityType == SecurityType.Future && contract.LastTradeDateOrContractMonth == "0")
                {
                    contract = _symbolMapper.ParseMalformedContractFutureSymbol(contract, _symbolPropertiesDatabase);
                    ibSymbol = contract.Symbol;
                }

                // Handle future options as a Future, up until we actually return the future.
                if (isFutureOption || securityType == SecurityType.Future)
                {
                    var leanSymbol = _symbolMapper.GetLeanRootSymbol(ibSymbol, securityType);
                    var defaultMarket = market;

                    if (!_symbolPropertiesDatabase.TryGetMarket(leanSymbol, SecurityType.Future, out market))
                    {
                        market = defaultMarket;
                    }

                    var contractExpiryDate = !string.IsNullOrEmpty(contract.LastTradeDateOrContractMonth)
                        ? DateTime.ParseExact(contract.LastTradeDateOrContractMonth, DateFormat.EightCharacter, CultureInfo.InvariantCulture)
                        : SecurityIdentifier.DefaultDate;

                    if (!isFutureOption)
                    {
                        return _symbolMapper.GetLeanSymbol(ibSymbol, SecurityType.Future, market, contractExpiryDate);
                    }

                    // Get the *actual* futures contract that this futures options contract has as its underlying.
                    // Futures options contracts can have a different contract month from their underlying future.
                    // As such, we resolve the underlying future to the future with the correct contract month.
                    // There's a chance this can fail, and if it does, we throw because this Symbol can't be
                    // represented accurately in Lean.
                    var futureSymbol = FuturesOptionsUnderlyingMapper.GetUnderlyingFutureFromFutureOption(leanSymbol, market, contractExpiryDate, _algorithm.Time);
                    if (futureSymbol == null)
                    {
                        // This is the worst case scenario, because we didn't find a matching futures contract for the FOP.
                        // Note that this only applies to CBOT symbols for now.
                        throw new ArgumentException($"The Future Option contract: {GetContractDescription(contract)} with trading class: {contract.TradingClass} has no matching underlying future contract.");
                    }

                    var right = contract.Right == IB.RightType.Call ? OptionRight.Call : OptionRight.Put;
                    // we don't have the Lean ticker yet, ticker is just used for logging
                    var strike = NormalizePriceToLean(contract.Strike, futureSymbol);

                    return Symbol.CreateOption(futureSymbol, market, OptionStyle.American, right, strike, contractExpiryDate);
                }

                if (securityType.IsOption())
                {
                    var expiryDate = DateTime.ParseExact(contract.LastTradeDateOrContractMonth, DateFormat.EightCharacter, CultureInfo.InvariantCulture);
                    if (securityType == SecurityType.IndexOption && !string.IsNullOrEmpty(contract.TradingClass))
                    {
                        // the option flavor ticket is in the trading class for IB, see related 'GetTradingClass'
                        ibSymbol = contract.TradingClass;
                    }
                    var right = contract.Right == IB.RightType.Call ? OptionRight.Call : OptionRight.Put;
                    var strike = Convert.ToDecimal(contract.Strike);

                    return _symbolMapper.GetLeanSymbol(ibSymbol, securityType, market, expiryDate, strike, right);
                }

                return _symbolMapper.GetLeanSymbol(ibSymbol, securityType, market);
            }
            catch (Exception error)
            {
                throw new Exception($"InteractiveBrokersBrokerage.MapSymbol(): Failed to convert contract for {contract.Symbol}; Contract description: {GetContractDescription(contract)}", error);
            }
        }

        private static decimal RoundPrice(decimal input, decimal minTick)
        {
            if (minTick == 0) return minTick;
            return Math.Round(input / minTick) * minTick;
        }

        /// <summary>
        /// Handles the threading issues of creating an IB OrderId/RequestId/TickerId
        /// </summary>
        /// <returns>The new IB OrderId/RequestId/TickerId</returns>
        private int GetNextId()
        {
            lock (_nextValidIdLocker)
            {
                // return the current value and increment
                return _nextValidId++;
            }
        }

        private void HandleBrokerTime(object sender, IB.CurrentTimeUtcEventArgs e)
        {
            _currentTimeEvent.Set();
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            // read values from the brokerage datas
            var port = Config.GetInt("ib-port", 4001);
            var host = Config.Get("ib-host", "127.0.0.1");
            var twsDirectory = Config.Get("ib-tws-dir", "C:\\Jts");
            var ibVersion = Config.Get("ib-version", DefaultVersion);

            var account = job.BrokerageData["ib-account"];
            var userId = job.BrokerageData["ib-user-name"];
            var password = job.BrokerageData["ib-password"];
            var tradingMode = job.BrokerageData["ib-trading-mode"];
            var agentDescription = job.BrokerageData["ib-agent-description"];

            var loadExistingHoldings = Config.GetBool("load-existing-holdings", true);
            if (job.BrokerageData.ContainsKey("load-existing-holdings"))
            {
                loadExistingHoldings = Convert.ToBoolean(job.BrokerageData["load-existing-holdings"]);
            }

            Initialize(null,
                null,
                null,
                account,
                host,
                port,
                twsDirectory,
                ibVersion,
                userId,
                password,
                tradingMode,
                agentDescription,
                loadExistingHoldings);

            if (!IsConnected)
            {
                Connect();
            }
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Submits a market data request (a subscription) for a given contract to IB.
        /// </summary>
        private void RequestMarketData(Contract contract, int requestId)
        {
            if (_enableDelayedStreamingData)
            {
                // Switch to delayed market data if the user does not have the necessary real time data subscription.
                // If live data is available, it will always be returned instead of delayed data.
                Client.ClientSocket.reqMarketDataType(3);
            }

            // we would like to receive OI (101)
            Client.ClientSocket.reqMktData(requestId, contract, "101", false, false, new List<TagValue>());
        }

        /// <summary>
        /// Handles the re-rout market data request event issued by the IB server
        /// </summary>
        private void HandleMarketDataReRoute(object sender, IB.RerouteMarketDataRequestEventArgs e)
        {
            var requestInformation = _requestInformation.GetValueOrDefault(e.RequestId);
            Log.Trace($"InteractiveBrokersBrokerage.Subscribe(): Re-routing {requestInformation?.AssociatedSymbol} CFD data request to underlying");

            // re-route the request to the underlying
            var underlyingContract = new Contract
            {
                ConId = e.ContractId,
                Exchange = e.UnderlyingPrimaryExchange,
            };

            RequestMarketData(underlyingContract, e.RequestId);
        }

        /// <summary>
        /// Adds the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be added keyed by SecurityType</param>
        private bool Subscribe(IEnumerable<Symbol> symbols)
        {
            if (!CanHandleSubscriptionRequest(symbols, "subscribe"))
            {
                return true;
            }

            try
            {
                foreach (var symbol in symbols)
                {
                    lock (_sync)
                    {
                        Log.Trace("InteractiveBrokersBrokerage.Subscribe(): Subscribe Request: " + symbol.Value);

                        if (!_subscribedSymbols.ContainsKey(symbol))
                        {
                            // processing canonical option and futures symbols
                            var subscribeSymbol = symbol;

                            // we ignore futures canonical symbol
                            if (symbol.ID.SecurityType == SecurityType.Future && symbol.IsCanonical())
                            {
                                continue;
                            }

                            // Skip subscribing to expired option contracts
                            if (OptionSymbol.IsOptionContractExpired(symbol, DateTime.UtcNow))
                            {
                                Log.Trace($"InteractiveBrokersBrokerage.Subscribe(): Skipping subscription for {symbol} because the contract has expired.");
                                continue;
                            }

                            var id = GetNextId();
                            var contract = CreateContract(subscribeSymbol, includeExpired: false);
                            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(subscribeSymbol.ID.Market, subscribeSymbol, subscribeSymbol.SecurityType, Currencies.USD);
                            var priceMagnifier = symbolProperties.PriceMagnifier;

                            _requestInformation[id] = new RequestInformation
                            {
                                RequestId = id,
                                RequestType = RequestType.Subscription,
                                AssociatedSymbol = symbol,
                                Message = $"[Id={id}] Subscribe: {symbol.Value} ({GetContractDescription(contract)})"
                            };

                            CheckRateLimiting();

                            // track subscription time for minimum delay in unsubscribe
                            _subscriptionTimes[id] = DateTime.UtcNow;

                            RequestMarketData(contract, id);

                            _subscribedSymbols[symbol] = id;
                            _subscribedTickers[id] = new SubscriptionEntry { Symbol = subscribeSymbol, PriceMagnifier = priceMagnifier };

                            Log.Trace($"InteractiveBrokersBrokerage.Subscribe(): Subscribe Processed: {symbol.Value} ({GetContractDescription(contract)}) # {id}. SubscribedSymbols.Count: {_subscribedSymbols.Count}");
                        }
                    }
                }
                return true;
            }
            catch (Exception err)
            {
                Log.Error(err, "InteractiveBrokersBrokerage.Subscribe(): " + err.Message);
            }
            return false;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        private bool CanHandleSubscriptionRequest(IEnumerable<Symbol> symbols, string message)
        {
            var result = !IsRestartInProgress() && IsConnected;
            if (!result)
            {
                // skip while restarting and not connected, once restart has ended and we are connected
                // we will restore data subscriptions asking the _subscriptionManager we want to avoid the race condition where
                // we are subscribing mid restart and we send IB an invalid request Id
                Log.Trace($"InteractiveBrokersBrokerage.CanHandleSubscription(): skip request for [{string.Join(",", symbols)}] {message}");
            }
            return result;
        }

        /// <summary>
        /// Removes the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be removed keyed by SecurityType</param>
        private bool Unsubscribe(IEnumerable<Symbol> symbols)
        {
            if (!CanHandleSubscriptionRequest(symbols, "unsubscribe"))
            {
                return true;
            }

            try
            {
                foreach (var symbol in symbols)
                {
                    if (CanSubscribe(symbol))
                    {
                        lock (_sync)
                        {
                            Log.Trace($"InteractiveBrokersBrokerage.Unsubscribe(): Unsubscribe Request: {symbol.Value}. SubscribedSymbols.Count: {_subscribedSymbols.Count}");

                            int id;
                            if (_subscribedSymbols.TryRemove(symbol, out id))
                            {
                                CheckRateLimiting();

                                // ensure minimum time span has elapsed since the symbol was subscribed
                                DateTime subscriptionTime;
                                if (_subscriptionTimes.TryGetValue(id, out subscriptionTime))
                                {
                                    var timeSinceSubscription = DateTime.UtcNow - subscriptionTime;
                                    if (timeSinceSubscription < _minimumTimespanBeforeUnsubscribe)
                                    {
                                        var delay = Convert.ToInt32((_minimumTimespanBeforeUnsubscribe - timeSinceSubscription).TotalMilliseconds);
                                        Thread.Sleep(delay);
                                    }

                                    _subscriptionTimes.Remove(id);
                                }

                                Client.ClientSocket.cancelMktData(id);

                                SubscriptionEntry entry;
                                return _subscribedTickers.TryRemove(id, out entry);
                            }
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Log.Error("InteractiveBrokersBrokerage.Unsubscribe(): " + err.Message);
            }
            return false;
        }

        /// <summary>
        /// Returns true if this data provide can handle the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol to be handled</param>
        /// <returns>True if this data provider can get data for the symbol, false otherwise</returns>
        private static bool CanSubscribe(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;

            if (symbol.Value.IndexOfInvariant("universe", true) != -1
                // continuous futures and canonical symbols not supported
                || symbol.IsCanonical())
            {
                return false;
            }

            // Include future options as a special case with no matching market, otherwise
            // our subscriptions are removed without any sort of notice.
            return
                (securityType == SecurityType.Equity && market == Market.USA) ||
                (securityType == SecurityType.Forex && market == Market.Oanda) ||
                (securityType == SecurityType.Option && market == Market.USA) ||
                (securityType == SecurityType.IndexOption && market == Market.USA) ||
                (securityType == SecurityType.Index && (market == Market.USA || market == Market.EUREX || market == Market.OSE || market == Market.HKFE)) ||
                (securityType == SecurityType.FutureOption) ||
                (securityType == SecurityType.Future) ||
                (securityType == SecurityType.Cfd && market == Market.InteractiveBrokers);
        }

        /// <summary>
        /// Returns a timestamp for a tick converted to the exchange time zone
        /// </summary>
        private DateTime GetRealTimeTickTime(Symbol symbol)
        {
            return DateTime.UtcNow.ConvertFromUtc(GetSecurityExchangeHours(symbol).TimeZone);
        }

        /// <summary>
        /// Gets exchange hours for the given <paramref name="symbol"/>, 
        /// loading from <see cref="MarketHoursDatabase"/> if not cached.
        /// </summary>
        /// <param name="symbol">The symbol to look up.</param>
        /// <returns>The exchange hours for the symbol.</returns>
        private SecurityExchangeHours GetSecurityExchangeHours(Symbol symbol)
        {
            var securityExchangeHours = default(SecurityExchangeHours);
            lock (_symbolExchangeHours)
            {
                if (!_symbolExchangeHours.TryGetValue(symbol, out securityExchangeHours))
                {
                    // read the exchange time zone from market-hours-database
                    securityExchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
                    _symbolExchangeHours.Add(symbol, securityExchangeHours);
                }
            }

            return securityExchangeHours;
        }

        private void HandleTickPrice(object sender, IB.TickPriceEventArgs e)
        {
            // tickPrice events are always followed by tickSize events,
            // so we save off the bid/ask/last prices and only emit ticks in the tickSize event handler.

            SubscriptionEntry entry;
            if (!_subscribedTickers.TryGetValue(e.TickerId, out entry))
            {
                return;
            }

            var symbol = entry.Symbol;

            // negative price (-1) means no price available, normalize to zero
            var price = e.Price < 0 ? 0 : Convert.ToDecimal(e.Price) / entry.PriceMagnifier;

            switch (e.Field)
            {
                case IBApi.TickType.BID:
                case IBApi.TickType.DELAYED_BID:

                    if (entry.LastQuoteTick == null)
                    {
                        entry.LastQuoteTick = new Tick
                        {
                            // in the event of a symbol change this will break since we'll be assigning the
                            // new symbol to the permtick which won't be known by the algorithm
                            Symbol = symbol,
                            TickType = TickType.Quote
                        };
                    }

                    // set the last bid price
                    entry.LastQuoteTick.BidPrice = price;
                    break;

                case IBApi.TickType.ASK:
                case IBApi.TickType.DELAYED_ASK:

                    if (entry.LastQuoteTick == null)
                    {
                        entry.LastQuoteTick = new Tick
                        {
                            // in the event of a symbol change this will break since we'll be assigning the
                            // new symbol to the permtick which won't be known by the algorithm
                            Symbol = symbol,
                            TickType = TickType.Quote
                        };
                    }

                    // set the last ask price
                    entry.LastQuoteTick.AskPrice = price;
                    break;

                case IBApi.TickType.LAST:
                case IBApi.TickType.DELAYED_LAST:

                    if (symbol.SecurityType == SecurityType.Index && symbol.Value.Equals("NDX", StringComparison.InvariantCultureIgnoreCase))
                    {
                        _ndxSecurityExchangeHours ??= MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
                        if (ShouldSkipTick(_ndxSecurityExchangeHours, GetRealTimeTickTime(symbol)))
                        {
                            // Clear the last trade tick to prevent using outdated data from the previous day.
                            entry.LastTradeTick = null;
                            return;
                        }
                    }

                    if (entry.LastTradeTick == null)
                    {
                        entry.LastTradeTick = new Tick
                        {
                            // in the event of a symbol change this will break since we'll be assigning the
                            // new symbol to the permtick which won't be known by the algorithm
                            Symbol = symbol,
                            TickType = TickType.Trade
                        };
                    }

                    // set the last traded price
                    entry.LastTradeTick.Value = price;
                    break;

                default:
                    return;
            }
        }

        private void HandleTickSize(object sender, IB.TickSizeEventArgs e)
        {
            SubscriptionEntry entry;
            if (!_subscribedTickers.TryGetValue(e.TickerId, out entry))
            {
                return;
            }

            var symbol = entry.Symbol;

            // negative size (-1) means no quantity available, normalize to zero
            var quantity = e.Size < 0 ? 0 : e.Size;

            if (quantity == decimal.MaxValue)
            {
                // we've seen this with SPX index bid size, not valid, expected for indexes
                quantity = 0;
            }

            Tick tick;
            switch (e.Field)
            {
                case IBApi.TickType.BID_SIZE:
                case IBApi.TickType.DELAYED_BID_SIZE:

                    tick = entry.LastQuoteTick;

                    if (tick == null)
                    {
                        // tick size message must be preceded by a tick price message
                        return;
                    }

                    tick.BidSize = quantity;

                    if (tick.BidPrice == 0)
                    {
                        // no bid price, do not emit tick
                        return;
                    }

                    if (tick.BidPrice > 0 && tick.AskPrice > 0 && tick.BidPrice >= tick.AskPrice)
                    {
                        // new bid price jumped at or above previous ask price, wait for new ask price
                        return;
                    }

                    if (tick.AskPrice == 0)
                    {
                        // we have a bid price but no ask price, use bid price as value
                        tick.Value = tick.BidPrice;
                    }
                    else
                    {
                        // we have both bid price and ask price, use mid price as value
                        tick.Value = (tick.BidPrice + tick.AskPrice) / 2;
                    }
                    break;

                case IBApi.TickType.ASK_SIZE:
                case IBApi.TickType.DELAYED_ASK_SIZE:

                    tick = entry.LastQuoteTick;

                    if (tick == null)
                    {
                        // tick size message must be preceded by a tick price message
                        return;
                    }

                    tick.AskSize = quantity;

                    if (tick.AskPrice == 0)
                    {
                        // no ask price, do not emit tick
                        return;
                    }

                    if (tick.BidPrice > 0 && tick.AskPrice > 0 && tick.BidPrice >= tick.AskPrice)
                    {
                        // new ask price jumped at or below previous bid price, wait for new bid price
                        return;
                    }

                    if (tick.BidPrice == 0)
                    {
                        // we have an ask price but no bid price, use ask price as value
                        tick.Value = tick.AskPrice;
                    }
                    else
                    {
                        // we have both bid price and ask price, use mid price as value
                        tick.Value = (tick.BidPrice + tick.AskPrice) / 2;
                    }
                    break;

                case IBApi.TickType.LAST_SIZE:
                case IBApi.TickType.DELAYED_LAST_SIZE:

                    tick = entry.LastTradeTick;

                    if (tick == null)
                    {
                        // tick size message must be preceded by a tick price message
                        return;
                    }

                    // set the traded quantity
                    tick.Quantity = quantity;
                    break;

                case IBApi.TickType.OPEN_INTEREST:
                case IBApi.TickType.OPTION_CALL_OPEN_INTEREST:
                case IBApi.TickType.OPTION_PUT_OPEN_INTEREST:

                    if (!symbol.ID.SecurityType.IsOption() && symbol.ID.SecurityType != SecurityType.Future)
                    {
                        return;
                    }

                    if (entry.LastOpenInterestTick == null)
                    {
                        entry.LastOpenInterestTick = new Tick { Symbol = symbol, TickType = TickType.OpenInterest };
                    }

                    tick = entry.LastOpenInterestTick;

                    tick.Value = quantity;
                    break;

                default:
                    return;
            }

            if (tick.IsValid())
            {
                tick = new Tick(tick)
                {
                    Time = GetRealTimeTickTime(symbol)
                };

                _aggregator.Update(tick);
            }
        }

        /// <summary>
        /// Method returns a collection of Symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search future/option chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Future/Option chain associated with the Symbol provided</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            // setting up exchange defaults and filters
            var exchangeSpecifier = GetSymbolExchange(symbol);
            var futuresExchanges = _futuresExchanges.Values.Reverse().ToArray();
            Func<string, int> exchangeFilter = exchange => symbol.SecurityType == SecurityType.Future ? Array.IndexOf(futuresExchanges, exchange) : 0;

            var lookupName = symbol.Value;

            if (symbol.SecurityType == SecurityType.Future)
            {
                lookupName = symbol.ID.Symbol;
            }
            else if (symbol.SecurityType == SecurityType.Option || symbol.SecurityType == SecurityType.IndexOption)
            {
                lookupName = symbol.Underlying.Value;
            }
            else if (symbol.SecurityType == SecurityType.FutureOption)
            {
                // Futures Options use the underlying Symbol ticker for their ticker on IB.
                lookupName = symbol.Underlying.ID.Symbol;
            }

            var symbolProperties = GetSymbolProperties(symbol);

            // setting up lookup request
            var contract = new Contract
            {
                Symbol = _symbolMapper.GetBrokerageRootSymbol(lookupName, symbol.SecurityType),
                Currency = securityCurrency ?? symbolProperties.QuoteCurrency,
                Exchange = exchangeSpecifier,
                SecType = ConvertSecurityType(symbol.SecurityType),
                IncludeExpired = includeExpired,
                Multiplier = GetContractMultiplier(symbolProperties.ContractMultiplier)
            };

            Log.Trace($"InteractiveBrokersBrokerage.LookupSymbols(): Requesting symbol list for {contract.Symbol} ...");

            var symbols = new List<Symbol>();

            if (symbol.SecurityType.IsOption())
            {
                // IB requests for full option chains are rate limited and responses can be delayed up to a minute for each underlying,
                // so we fetch them from the OCC website instead of using the IB API.
                // For futures options, we fetch the option chain from CME.
                if (_algorithm != null)
                {
                    symbols.AddRange(_algorithm.OptionChainProvider.GetOptionContractList(symbol, DateTime.Today));
                }
                else
                {
                    symbols.AddRange(Composer.Instance.GetPart<IOptionChainProvider>().GetOptionContractList(symbol, DateTime.Today));
                }
            }
            else if (symbol.SecurityType == SecurityType.Future)
            {
                // processing request
                var results = FindContracts(contract, contract.Symbol);

                // filtering results
                var filteredResults =
                    results
                        .Select(x => x.Contract)
                        .GroupBy(x => x.Exchange)
                        .OrderByDescending(g => exchangeFilter(g.Key))
                        .FirstOrDefault();

                if (filteredResults != null)
                {
                    symbols.AddRange(filteredResults.Select(MapSymbol));
                }
            }

            // Try to remove options or futures contracts that have expired
            if (!includeExpired)
            {
                if (symbol.SecurityType.IsOption() || symbol.SecurityType == SecurityType.Future)
                {
                    var removedSymbols = symbols.Where(x => x.ID.Date < GetRealTimeTickTime(x).Date).ToHashSet();

                    if (symbols.RemoveAll(x => removedSymbols.Contains(x)) > 0)
                    {
                        Log.Trace("InteractiveBrokersBrokerage.LookupSymbols(): Removed contract(s) for having expiry in the past: {0}", string.Join(",", removedSymbols.Select(x => x.Value)));
                    }
                }
            }

            Log.Trace($"InteractiveBrokersBrokerage.LookupSymbols(): Returning {symbols.Count} contract(s) for {contract.Symbol}");

            return symbols;
        }

        private static string GetContractMultiplier(decimal contractMultiplier)
        {
            if (contractMultiplier % 1 == 0)
            {
                // IB doesn't like 5000.0
                return Convert.ToInt32(contractMultiplier).ToStringInvariant();
            }
            // some like MYM have a contract mutiplier of 0.5
            return contractMultiplier.ToStringInvariant();
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            return !_ibAutomater.IsWithinScheduledServerResetTimes() && IsConnected;
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of bars covering the span specified in the request</returns>
        /// <remarks>For IB history limitations see https://www.interactivebrokers.com/en/software/api/apiguide/tables/historical_data_limitations.htm </remarks>
        public override IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            if (!IsConnected)
            {
                OnMessage(
                    new BrokerageMessageEvent(
                        BrokerageMessageType.Warning,
                        "GetHistoryWhenDisconnected",
                        "History requests cannot be submitted when disconnected."));
                return null;
            }

            // skipping universe and canonical symbols
            if (!CanSubscribe(request.Symbol))
            {
                return null;
            }

            // tick resolution not supported for now
            if (request.Resolution == Resolution.Tick)
            {
                // TODO: upgrade IB C# API DLL
                // In IB API version 973.04, the reqHistoricalTicks function has been added,
                // which would now enable us to support history requests at Tick resolution.
                return null;
            }

            if (request.TickType == TickType.OpenInterest)
            {
                if (!_historyOpenInterestWarning)
                {
                    _historyOpenInterestWarning = true;
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistoryOpenInterest", "IB does not provide open interest historical data"));
                }
                return null;
            }

            if (request.Symbol.SecurityType == SecurityType.Cfd && request.TickType == TickType.Trade)
            {
                if (!_historyCfdTradeWarning)
                {
                    _historyCfdTradeWarning = true;
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistoryCfdTrade", "IB does not provide CFD trade historical data"));
                }
                return null;
            }

            if (request.EndTimeUtc < request.StartTimeUtc)
            {
                if (!_historyInvalidPeriodWarning)
                {
                    _historyInvalidPeriodWarning = true;
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning,
                        "GetHistoryEndTimeBeforeStartTime", "The history request end time is before the start time. No history will be returned."));
                }
                return null;
            }

            // See https://interactivebrokers.github.io/tws-api/historical_limitations.html
            // We need to check this before creating the contract below, which could trigger an IB request that would fail for expired contracts and kill the algorithm
            if (request.Symbol.ID.SecurityType.IsOption() || request.Symbol.ID.SecurityType == SecurityType.Future)
            {
                var localNow = DateTime.UtcNow.ConvertFromUtc(request.ExchangeHours.TimeZone);
                // EOD of the expiration for options
                var historicalLimitDate = request.Symbol.ID.Date.AddDays(1);

                if (request.Symbol.ID.SecurityType == SecurityType.Future)
                {
                    // Expired futures data older than two years counting from the future's expiration date.
                    historicalLimitDate = request.Symbol.ID.Date.AddYears(2);
                }

                if (localNow > historicalLimitDate)
                {
                    if (!_historyExpiredAssetWarning)
                    {
                        var message = request.Symbol.ID.SecurityType == SecurityType.Future ? "IB does not provide historical data of expired futures older than 2 years"
                            : "IB does not provide historical data of expired options";

                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistoryExpired", message));
                        _historyExpiredAssetWarning = true;
                    }
                    Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Skip request of expired asset: {request.Symbol.Value}. {localNow} > {historicalLimitDate}");
                    return Enumerable.Empty<BaseData>();
                }
            }
            else if (request.Symbol.ID.SecurityType == SecurityType.Equity)
            {
                var localNow = DateTime.UtcNow.ConvertFromUtc(request.ExchangeHours.TimeZone);
                var resolver = _mapFileProvider.Get(AuxiliaryDataKey.Create(request.Symbol));
                var mapfile = resolver.ResolveMapFile(request.Symbol);
                if (localNow > mapfile.DelistingDate)
                {
                    if (!_historyDelistedAssetWarning)
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistoryDelisted", "IB does not provide historical data of delisted assets"));
                        _historyDelistedAssetWarning = true;
                    }

                    Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Skip request of delisted asset: {request.Symbol.Value}. DelistingDate: {mapfile.DelistingDate}");
                    return Enumerable.Empty<BaseData>();
                }
            }

            var exchangeTimeZone = GetSecurityExchangeHours(request.Symbol).TimeZone;

            // preparing the data for IB request
            var contract = CreateContract(request.Symbol, includeExpired: true);
            if (contract.SecType == IB.SecurityType.ContractForDifference)
            {
                var contractDetails = GetContractDetails(contract, request.Symbol.Value);
                // IB does not have data for equity and forex CFDs, we need to use the underlying security
                var underlyingSecurityType = contractDetails.UnderSecType switch
                {
                    IB.SecurityType.Stock => SecurityType.Equity,
                    IB.SecurityType.Cash => SecurityType.Forex,
                    _ => (SecurityType?)null
                };

                if (underlyingSecurityType.HasValue)
                {
                    var underlyingSymbol = Symbol.Create(request.Symbol.Value, underlyingSecurityType.Value, request.Symbol.ID.Market);
                    contract = CreateContract(underlyingSymbol, includeExpired: true);
                }
            }

            var resolution = ConvertResolution(request.Resolution);
            var startTime = request.Resolution == Resolution.Daily ? request.StartTimeUtc.Date : request.StartTimeUtc;
            var startTimeLocal = startTime.ConvertFromUtc(exchangeTimeZone);
            var endTime = request.Resolution == Resolution.Daily ? request.EndTimeUtc.Date : request.EndTimeUtc;
            var endTimeLocal = request.EndTimeUtc.ConvertFromUtc(exchangeTimeZone);

            // See https://interactivebrokers.github.io/tws-api/historical_limitations.html
            if (request.Resolution == Resolution.Second)
            {
                var minimumLocalStartTime = DateTime.UtcNow.ConvertFromUtc(exchangeTimeZone).AddMonths(-6);
                if (startTimeLocal < minimumLocalStartTime)
                {
                    startTimeLocal = minimumLocalStartTime;
                    startTime = startTimeLocal.ConvertFromUtc(exchangeTimeZone);

                    if (!_historySecondResolutionWarning)
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistorySecondResolution", "IB does not provide Resolution.Second historical data older than 6 months"));
                        _historySecondResolutionWarning = true;
                    }

                    if (startTime > endTime)
                    {
                        Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Skip too old 'Resolution.Second' request {startTimeLocal} < {minimumLocalStartTime}");
                        return Enumerable.Empty<BaseData>();
                    }
                }
            }

            Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Submitting request: {request.Symbol.Value} ({GetContractDescription(contract)}): {request.Resolution}/{request.TickType} {startTime} UTC -> {endTime} UTC");

            IEnumerable<BaseData> history;
            if (request.TickType == TickType.Quote)
            {
                // Quotes need two separate IB requests for Bid and Ask,
                // each pair of TradeBars will be joined into a single QuoteBar
                var historyBid = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, resolution, HistoricalDataType.Bid);
                var historyAsk = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, resolution, HistoricalDataType.Ask);

                history = historyBid.Join(historyAsk,
                    bid => bid.Time,
                    ask => ask.Time,
                    (bid, ask) => new QuoteBar(
                        bid.Time,
                        bid.Symbol,
                        new Bar(bid.Open, bid.High, bid.Low, bid.Close),
                        0,
                        new Bar(ask.Open, ask.High, ask.Low, ask.Close),
                        0,
                        bid.Period));
            }
            else
            {
                // other assets will have TradeBars
                history = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, resolution, HistoricalDataType.Trades);
            }

            return FilterHistory(history, request, startTimeLocal, endTimeLocal, contract);
        }

        private IEnumerable<BaseData> FilterHistory(
            IEnumerable<BaseData> history,
            HistoryRequest request,
            DateTime startTimeLocal,
            DateTime endTimeLocal,
            Contract contract)
        {
            // cleaning the data before returning it back to user
            foreach (var bar in history.Where(bar => bar.Time >= startTimeLocal && bar.EndTime <= endTimeLocal))
            {
                if (request.Symbol.SecurityType == SecurityType.Equity ||
                    request.ExchangeHours.IsOpen(bar.Time, bar.EndTime, request.IncludeExtendedMarketHours))
                {
                    yield return bar;
                }
            }

            Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Download completed: {request.Symbol.Value} ({GetContractDescription(contract)})");
        }

        private IEnumerable<TradeBar> GetHistory(
            HistoryRequest request,
            Contract contract,
            DateTime startTime,
            DateTime endTime,
            DateTimeZone exchangeTimeZone,
            string resolution,
            string dataType)
        {
            const int timeOut = 60; // seconds timeout

            var history = new List<TradeBar>();
            using var dataDownloaded = new AutoResetEvent(false);

            // This is needed because when useRTH is set to 1, IB will return data only
            // during Equity regular trading hours (for any asset type, not only for equities)
            var useRegularTradingHours = request.Symbol.SecurityType == SecurityType.Equity
                ? Convert.ToInt32(!request.IncludeExtendedMarketHours)
                : 0;

            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(request.Symbol.ID.Market, request.Symbol, request.Symbol.SecurityType, Currencies.USD);
            var priceMagnifier = symbolProperties.PriceMagnifier;

            // making multiple requests if needed in order to download the history
            while (endTime >= startTime)
            {
                // before we do anything let's check our rate limits
                CheckRateLimiting();
                CheckHighResolutionHistoryRateLimiting(request.Resolution);
                _concurrentHistoryRequests.Wait();

                try
                {
                    // let's refetch the duration for each request, so for example we don't request 2 years for of data for 1 extra day
                    var duration = GetDuration(request.Resolution, endTime - startTime);

                    var pacing = false;
                    var dataDownloadedCount = 0;
                    TradeBar oldestDataPoint = null;
                    var historicalTicker = GetNextId();

                    _requestInformation[historicalTicker] = new RequestInformation
                    {
                        RequestId = historicalTicker,
                        RequestType = RequestType.History,
                        AssociatedSymbol = request.Symbol,
                        Message = $"[Id={historicalTicker}] GetHistory: {request.Symbol.Value} ({GetContractDescription(contract)})",
                        HistoryRequest = request
                    };

                    EventHandler<IB.HistoricalDataEventArgs> clientOnHistoricalData = (sender, args) =>
                    {
                        if (args.RequestId == historicalTicker)
                        {
                            var bar = ConvertTradeBar(request.Symbol, request.Resolution, args, priceMagnifier);
                            if (request.Resolution != Resolution.Daily)
                            {
                                bar.Time = bar.Time.ConvertFromUtc(exchangeTimeZone);

                                if (request.Resolution == Resolution.Hour)
                                {
                                    // let's make sure to round down to the requested resolution, if requesting hourly, non extended market hours, IB returns 9:30 for the first bar
                                    bar.Time = bar.Time.RoundDown(request.Resolution.ToTimeSpan());
                                }
                            }

                            oldestDataPoint ??= bar;
                            history.Add(bar);

                            Interlocked.Increment(ref dataDownloadedCount);
                        }
                    };

                    EventHandler<IB.HistoricalDataEndEventArgs> clientOnHistoricalDataEnd = (sender, args) =>
                    {
                        if (args.RequestId == historicalTicker)
                        {
                            dataDownloaded.Set();
                        }
                    };

                    EventHandler<IB.ErrorEventArgs> clientOnError = (sender, args) =>
                    {
                        if (args.Id == historicalTicker)
                        {
                            if (args.Code == 162 && args.Message.Contains("pacing violation"))
                            {
                                // pacing violation happened
                                pacing = true;
                            }
                            else
                            {
                                if (args.Message.Contains("invalid step"))
                                {
                                    // this shouldn't happen if it does we want to know about it
                                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "History", args.Message));
                                }
                                dataDownloaded.Set();
                            }
                        }
                    };

                    Client.Error += clientOnError;
                    Client.HistoricalData += clientOnHistoricalData;
                    Client.HistoricalDataEnd += clientOnHistoricalDataEnd;

                    Client.ClientSocket.reqHistoricalData(historicalTicker, contract, endTime.ToStringInvariant("yyyyMMdd HH:mm:ss UTC"),
                        duration, resolution, dataType, useRegularTradingHours, 2, false, new List<TagValue>());

                    var waitResult = 0;
                    while (waitResult == 0)
                    {
                        waitResult = WaitHandle.WaitAny(new WaitHandle[] { dataDownloaded }, timeOut * 1000);

                        if (waitResult == WaitHandle.WaitTimeout)
                        {
                            if (Interlocked.Exchange(ref dataDownloadedCount, 0) != 0)
                            {
                                // timeout but data is being downloaded, so we are good, let's wait again but clear the data download count
                                waitResult = 0;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }

                    Client.Error -= clientOnError;
                    Client.HistoricalData -= clientOnHistoricalData;
                    Client.HistoricalDataEnd -= clientOnHistoricalDataEnd;

                    if (waitResult == WaitHandle.WaitTimeout)
                    {
                        if (pacing)
                        {
                            // we received 'pacing violation' error from IB. So we had to wait
                            Log.Trace("InteractiveBrokersBrokerage::GetHistory() Pacing violation. Paused for {0} secs.", timeOut);
                            continue;
                        }

                        Log.Trace("InteractiveBrokersBrokerage::GetHistory() History request timed out ({0} sec)", timeOut);
                        break;
                    }

                    // if no data has been received this time, we exit
                    if (oldestDataPoint == null)
                    {
                        break;
                    }

                    // moving endTime to the new position to proceed with next request (if needed)
                    endTime = oldestDataPoint.Time.ConvertToUtc(exchangeTimeZone);
                }
                finally
                {
                    _concurrentHistoryRequests.Release();
                }
            }

            return history.OrderBy(x => x.Time);
        }

        /// <summary>
        /// Gets the exchange the Symbol should be routed to
        /// </summary>
        /// <param name="securityType">SecurityType of the Symbol</param>
        /// <param name="market">Market of the Symbol</param>
        /// <param name="ticker">Ticker for the symbol</param>
        public static string GetSymbolExchange(SecurityType securityType, string market)
        {
            switch (securityType)
            {
                case SecurityType.Forex:
                    return "IDEALPRO"; // IB's Forex market is always IDEALPRO
                case SecurityType.Option:
                case SecurityType.IndexOption:
                    // Regular equity options uses default, in this case "Smart"
                    goto default;

                // Futures options share the same market as the underlying Symbol
                case SecurityType.FutureOption:
                case SecurityType.Future:
                    if (_futuresExchanges.TryGetValue(market, out var result))
                    {
                        return result;
                    }
                    return market;

                default:
                    return "Smart";
            }
        }

        /// <summary>
        /// Gets the exchange the Symbol should be routed to
        /// </summary>
        /// <param name="symbol">Symbol to route</param>
        private string GetSymbolExchange(Symbol symbol)
        {
            return GetSymbolExchange(symbol.SecurityType, symbol.ID.Market);
        }

        /// <summary>
        /// Returns whether the brokerage should perform the cash synchronization
        /// </summary>
        /// <param name="currentTimeUtc">The current time (UTC)</param>
        /// <returns>True if the cash sync should be performed</returns>
        public override bool ShouldPerformCashSync(DateTime currentTimeUtc)
        {
            return base.ShouldPerformCashSync(currentTimeUtc)
                && !_ibAutomater.IsWithinScheduledServerResetTimes()
                && IsConnected;
        }

        private void CheckRateLimiting()
        {
            if (!_messagingRateLimiter.WaitToProceed(TimeSpan.Zero))
            {
                Log.Trace("The IB API request has been rate limited.");

                _messagingRateLimiter.WaitToProceed();
            }
        }

        private void CheckHighResolutionHistoryRateLimiting(Resolution resolution)
        {
            if (resolution != Resolution.Tick && resolution != Resolution.Second)
            {
                return;
            }

            if (!_historyHighResolutionRateLimiter.WaitToProceed(TimeSpan.Zero))
            {
                var message = "History request rate limited. IB allows up to 60 requests in a 10 minute period for bars of 30 seconds or less";
                Log.Trace(message);

                if (!_historyHighResolutionRateLimitWarning)
                {
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "GetHistorySecondResolution", message));
                    _historyHighResolutionRateLimitWarning = true;
                }

                _historyHighResolutionRateLimiter.WaitToProceed();
            }
        }

        private void OnIbAutomaterOutputDataReceived(object sender, OutputDataReceivedEventArgs e)
        {
            if (string.IsNullOrEmpty(e.Data))
            {
                return;
            }

            if (e.Data.Contains("Waiting for 2FA confirmation", StringComparison.InvariantCultureIgnoreCase))
            {
                // composer get IResultHandler send message
                var resultHandler = Composer.Instance.GetPart<IResultHandler>();
                resultHandler?.DebugMessage("Logging into account. Check phone for two-factor authentication verification...");
            }
            else if (e.Data.Contains("2FA maximum attempts reached", StringComparison.InvariantCultureIgnoreCase))
            {
                Task.Factory.StartNew(() =>
                {
                    _ibAutomater.Stop();
                    var message = "2FA authentication confirmation required to reconnect.";
                    OnMessage(BrokerageMessageEvent.Disconnected(message));
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.ActionRequired, "2FAAuthRequired", message));
                });
            }

            Log.Trace($"InteractiveBrokersBrokerage.OnIbAutomaterOutputDataReceived(): {e.Data}");
        }

        private void StopGatewayRestartTask()
        {
            if (IsRestartInProgress())
            {
                _gatewayRestartTokenSource.Cancel();
                Log.Trace($"InteractiveBrokersBrokerage.StopGatewayRestartTask(): cancelled");
            }
        }

        private bool IsRestartInProgress()
        {
            // we take the lock to avoid it getting disposed while we are evaluating it
            lock (_gatewayRestartTokenSource ?? new object())
            {
                // check if we are restarting
                return _gatewayRestartTokenSource != null && !_gatewayRestartTokenSource.IsCancellationRequested;
            }
        }

        /// <summary>
        /// Rarely the gateways goes into an invalid state until it's restarted, so we restart the gateway from within so 2FA is not requested
        /// </summary>
        private void StartGatewayRestartTask()
        {
            try
            {
                if (_isDisposeCalled || IsRestartInProgress())
                {
                    // if we are disposed or we already triggered the restart skip a new call
                    var message = _isDisposeCalled ? "we are disposed" : "restart task already scheduled";
                    Log.Trace($"InteractiveBrokersBrokerage.StartGatewayRestartTask(): skipped request: {message}");
                    return;
                }

                var delay = GetRestartDelay();
                Log.Trace($"InteractiveBrokersBrokerage.StartGatewayRestartTask(): start restart in: {delay}...");

                // we take the lock to avoid it getting disposed while consumers are evaluating it
                lock (_gatewayRestartTokenSource ?? new object())
                {
                    // dispose of the previous cancellation token
                    _gatewayRestartTokenSource.DisposeSafely();
                    _gatewayRestartTokenSource = new CancellationTokenSource();
                }
                Task.Delay(delay, _gatewayRestartTokenSource.Token).ContinueWith((_) =>
                {
                    try
                    {
                        if (_isDisposeCalled || _gatewayRestartTokenSource.IsCancellationRequested)
                        {
                            Log.Trace($"InteractiveBrokersBrokerage.StartGatewayRestartTask(): skip restart. Disposed: {_isDisposeCalled} Cancelled {_gatewayRestartTokenSource.IsCancellationRequested}");
                            return;
                        }

                        if (_ibAutomater.IsWithinScheduledServerResetTimes())
                        {
                            // delay it
                            _gatewayRestartTokenSource.Cancel();
                            StartGatewayRestartTask();
                        }
                        else
                        {
                            Log.Trace($"InteractiveBrokersBrokerage.StartGatewayRestartTask(): triggering soft restart");
                            _ibAutomater.SoftRestart();
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex);
                    }
                });
            }
            catch (Exception ex)
            {
                Log.Error(ex);
            }
        }

        /// <summary>
        /// Recurring task to schedule the weekly gateway restart, which requires 2FA and can be configured by the user.
        /// This allows to have a scheduled weekly restart that users can configure in order to be able to confirm the weekly 2FA
        /// request at a expected time.
        /// </summary>
        private void StartGatewayWeeklyRestartTask()
        {
            if (_isDisposeCalled)
            {
                Log.Trace("InteractiveBrokersBrokerage.StartGatewayWeeklyRestartTask(): skipped request: we are disposed");
                return;
            }

            var utcNow = DateTime.UtcNow;
            // get the next weekly restart time counting from tomorrow in case today is a Sunday,
            // which will set the delay for today when it should be next Sunday instead
            var restartDate = GetNextWeeklyRestartTimeUtc(utcNow.AddDays(1));
            // we subtract _defaultRestartDelay to avoid potential race conditions with the IBAutomater.Exited event handler and
            // to ensure the 2FA confirmation is requested as close to the configured time as possible.
            var delay = restartDate - utcNow - _defaultRestartDelay;

            Log.Trace($"InteractiveBrokersBrokerage.StartGatewayWeeklyRestartTask(): scheduled weekly restart to {restartDate} (in {delay})");

            Task.Delay(delay).ContinueWith(_ =>
            {
                if (_isDisposeCalled)
                {
                    Log.Trace("InteractiveBrokersBrokerage.StartGatewayWeeklyRestartTask(): skip restart: we are disposed");
                    return;
                }

                var restart = false;

                lock (_lastIBAutomaterExitTimeLock)
                {
                    // if the gateway hasn't yet exited today, we restart manually
                    if (_lastIBAutomaterExitTime.Date < DateTime.UtcNow.Date)
                    {
                        restart = true;
                    }
                    else
                    {
                        Log.Trace($"InteractiveBrokersBrokerage.StartGatewayWeeklyRestartTask(): skip restart: gateway already exited today and should have been automatically restarted.");
                    }
                }

                if (restart)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.StartGatewayWeeklyRestartTask(): triggering weekly restart manually");

                    try
                    {
                        if (_ibAutomater.IsRunning())
                        {
                            // stopping the gateway will make the IBAutomater emit the exit event, which will trigger the restart
                            _ibAutomater?.Stop();
                        }
                        else
                        {
                            // if the gateway is not running, we start it
                            CheckIbAutomaterError(_ibAutomater.Start(false));
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex);
                    }
                }

                // schedule the next weekly restart
                StartGatewayWeeklyRestartTask();
            });
        }

        private void OnIbAutomaterErrorDataReceived(object sender, ErrorDataReceivedEventArgs e)
        {
            if (e.Data == null) return;

            Log.Trace($"InteractiveBrokersBrokerage.OnIbAutomaterErrorDataReceived(): {e.Data}");
        }

        private void OnIbAutomaterExited(object sender, ExitedEventArgs e)
        {
            lock (_lastIBAutomaterExitTimeLock)
            {
                _lastIBAutomaterExitTime = DateTime.UtcNow;
            }

            Log.Trace($"InteractiveBrokersBrokerage.OnIbAutomaterExited(): Exit code: {e.ExitCode}");

            _stateManager.Reset();
            StopGatewayRestartTask();

            if (_isDisposeCalled)
            {
                return;
            }

            // check if IBGateway was closed because of an IBAutomater error, die if so
            var result = _ibAutomater.GetLastStartResult();
            if (IsRecuperable2FATimeout(result))
            {
                return;
            }
            CheckIbAutomaterError(result, false);

            if (!result.HasError)
            {
                // IBGateway was closed by IBAutomater because the auto-restart token expired or it was closed manually (less likely)
                Log.Trace("InteractiveBrokersBrokerage.OnIbAutomaterExited(): IBGateway close detected, restarting IBAutomater...");

                try
                {
                    // disconnect immediately so orders will not be submitted to the API while waiting for reconnection
                    Disconnect();
                }
                catch (Exception exception)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.OnIbAutomaterExited(): error in Disconnect(): {exception}");
                }

                var delay = GetWeeklyRestartDelay();

                Log.Trace($"InteractiveBrokersBrokerage.OnIbAutomaterExited(): Delay before restart: {delay:d'd 'h'h 'm'm 's's'}");

                Task.Delay(delay).ContinueWith(_ =>
                {
                    try
                    {
                        Log.Trace("InteractiveBrokersBrokerage.OnIbAutomaterExited(): restarting...");

                        var result = _ibAutomater.Start(false);
                        CheckIbAutomaterError(result);

                        // Has error but we are still running, we might be waiting for 2FA user required action after timeout, let's not connect in that case
                        if (!result.HasError)
                        {
                            Connect();
                        }
                    }
                    catch (Exception exception)
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "IBAutomaterRestartError", exception.ToString()));
                    }
                }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            }
            else
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "IBAutomaterError", result.ErrorMessage));
            }
        }

        private TimeSpan GetRestartDelay()
        {
            // during weekends wait until one hour before FX market open before restarting IBAutomater
            return _ibAutomater.IsWithinWeekendServerResetTimes()
               ? GetNextWeekendReconnectionTimeUtc() - DateTime.UtcNow
               : _defaultRestartDelay;
        }

        private TimeSpan GetWeeklyRestartDelay()
        {
            var utcNow = DateTime.UtcNow;
            // during weekends (including the whole Sunday) we wait until the time configured by the user
            if (_ibAutomater.IsWithinWeekendServerResetTimes() || utcNow.DayOfWeek == DayOfWeek.Sunday)
            {
                var delay = GetNextWeeklyRestartTimeUtc(utcNow) - utcNow;

                // if the delay is negative, it means the restart time has already passed for today, so we set it for _defaultRestartDelay
                if (delay > TimeSpan.Zero)
                {
                    return delay;
                }
            }

            return _defaultRestartDelay;
        }

        private void OnIbAutomaterRestarted(object sender, EventArgs e)
        {
            Log.Trace("InteractiveBrokersBrokerage.OnIbAutomaterRestarted()");

            _stateManager.Reset();
            StopGatewayRestartTask();

            // check if IBGateway was closed because of an IBAutomater error
            var result = _ibAutomater.GetLastStartResult();
            CheckIbAutomaterError(result, false);

            if (!result.HasError && !_isDisposeCalled)
            {
                // IBGateway was restarted automatically
                Log.Trace("InteractiveBrokersBrokerage.OnIbAutomaterRestarted(): IBGateway restart detected, reconnecting...");

                try
                {
                    Disconnect();

                    Connect();
                }
                catch (Exception exception)
                {
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "IBAutomaterAutoRestartError", exception.ToString()));
                }
            }
        }

        public static DateTime GetNextSundayFromDate(DateTime date)
        {
            var daysUntilSunday = (DayOfWeek.Sunday - date.DayOfWeek + 7) % 7;

            return date.AddDays(daysUntilSunday);
        }

        /// <summary>
        /// Gets the time (UTC) of the next reconnection attempt.
        /// </summary>
        private static DateTime GetNextWeekendReconnectionTimeUtc()
        {
            // return the UTC time at one hour before Sunday FX market open,
            // ignoring holidays as we should be able to connect with closed markets anyway
            var nextDate = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork).Date;
            nextDate = GetNextSundayFromDate(nextDate);

            return new DateTime(nextDate.Year, nextDate.Month, nextDate.Day, 16, 0, 0).ConvertToUtc(TimeZones.NewYork);
        }

        /// <summary>
        /// Gets the time (UTC) of the next IBAutomater weekly restart on the given time of day
        /// </summary>
        public static DateTime ComputeNextWeeklyRestartTimeUtc(TimeSpan weeklyRestartUtcTimeOfDay, DateTime currentDate)
        {
            var nextSunday = GetNextSundayFromDate(currentDate);

            return nextSunday.Date.Add(weeklyRestartUtcTimeOfDay);
        }

        /// <summary>
        /// Gets the time (UTC) of the next IBAutomater weekly restart, including 2FA.
        /// </summary>
        private DateTime GetNextWeeklyRestartTimeUtc(DateTime currentDate)
        {
            return ComputeNextWeeklyRestartTimeUtc(_weeklyRestartUtcTime, currentDate);
        }

        private void CheckIbAutomaterError(StartResult result, bool throwException = true)
        {
            if (IsRecuperable2FATimeout(result))
            {
                return;
            }

            if (result.HasError)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, result.ErrorCode.ToString(), result.ErrorMessage));

                if (throwException)
                {
                    throw new Exception($"InteractiveBrokersBrokerage.CheckIbAutomaterError(): {result.ErrorCode} - {result.ErrorMessage}");
                }
            }
        }

        private bool IsRecuperable2FATimeout(StartResult result)
        {
            if (_pastFirstConnection && result.ErrorCode == ErrorCode.TwoFactorConfirmationTimeout)
            {
                Log.Trace($"InteractiveBrokersBrokerage.IsRecuperable2FATimeout(): will trigger user action request");
                return true;
            }
            return false;
        }

        private void HandleAccountSummary(object sender, IB.AccountSummaryEventArgs e)
        {
            Log.Trace($"InteractiveBrokersBrokerage.HandleAccountSummary(): Request id: {e.RequestId}, Account: {e.Account}, Tag: {e.Tag}, Value: {e.Value}, Currency: {e.Currency}");
        }

        private void HandleFamilyCodes(object sender, IB.FamilyCodesEventArgs e)
        {
            foreach (var familyCode in e.FamilyCodes)
            {
                Log.Trace($"InteractiveBrokersBrokerage.HandleFamilyCodes(): Account id: {familyCode.AccountID}, Family code: {familyCode.FamilyCodeStr}");
            }
        }

        private void HandleManagedAccounts(object sender, IB.ManagedAccountsEventArgs e)
        {
            Log.Trace($"InteractiveBrokersBrokerage.HandleManagedAccounts(): Account list: {e.AccountList}");
        }

        private void AddGuaranteedTag(IBApi.Order ibOrder, bool nonGuaranteed)
        {
            ibOrder.SmartComboRoutingParams = new List<TagValue>
            {
                new TagValue("NonGuaranteed", nonGuaranteed ? "1" : "0")
            };
        }

        private bool _maxSubscribedSymbolsReached = false;
        private readonly ConcurrentDictionary<Symbol, int> _subscribedSymbols = new ConcurrentDictionary<Symbol, int>();
        private readonly ConcurrentDictionary<int, SubscriptionEntry> _subscribedTickers = new ConcurrentDictionary<int, SubscriptionEntry>();

        /// <summary>
        /// Determines whether the current tick for the NDX index should be skipped,
        /// based on whether the symbol's local time has reached or passed the next scheduled market open.
        /// This is used to avoid processing unreliable ticks during the first 30 seconds of market open.
        /// If the condition is met, the next skip time is updated to the following market open.
        /// </summary>
        /// <param name="exchangeHours">The exchange hours used to determine market open times.</param>
        /// <param name="symbolTickTime">The local time of the tick to evaluate.</param>
        /// <returns>
        /// <c>true</c> if the tick should be skipped (i.e., it's within the first 30 seconds after market open); otherwise, <c>false</c>.
        /// </returns>
        internal static bool ShouldSkipTick(SecurityExchangeHours exchangeHours, DateTime symbolTickTime)
        {
            // Subtracting 30 seconds here is intentional:
            // When the market opens (e.g., 9:30 AM EST), the first tick received for NDX via the IB API
            // often contains the *previous day's close* as the price. This stale tick appears at or just after open.
            //
            // The *second* tick that arrives - still within the first few seconds - contains the correct
            // open price and is the one displayed in IB TWS's open bar.
            //
            // By subtracting 30 seconds, we ensure we look *just before* the current tick time,
            // so `GetNextMarketOpen()` gives us today's 9:30 AM open (not tomorrow's).
            // This allows us to create a small "skip window" right after market open,
            // avoiding use of inaccurate initial pricing.
            if (_nextNdxMarketOpenSkipTime == default)
            {
                _nextNdxMarketOpenSkipTime = exchangeHours.GetNextMarketOpen(symbolTickTime.AddSeconds(-30), false);
            }

            if (symbolTickTime >= _nextNdxMarketOpenSkipTime)
            {
                _nextNdxMarketOpenSkipTime = exchangeHours.GetNextMarketOpen(symbolTickTime, false);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Determines whether the specified financial advisor group is allowed
        /// based on the current group filter. If no filter is set, all groups are allowed.
        /// </summary>
        /// <param name="groupName">The name of the financial advisor group to check.</param>
        /// <returns><c>true</c> if the group is allowed; otherwise, <c>false</c>.</returns>
        private static bool IsFaGroupFlitterSet(string groupName)
        {
            return !string.IsNullOrEmpty(_financialAdvisorsGroupFilter)
                && !string.IsNullOrEmpty(groupName)
                && !groupName.Equals(_financialAdvisorsGroupFilter, StringComparison.InvariantCultureIgnoreCase);
        }

        private class SubscriptionEntry
        {
            public Symbol Symbol { get; set; }
            public decimal PriceMagnifier { get; set; }
            public Tick LastTradeTick { get; set; }
            public Tick LastQuoteTick { get; set; }
            public Tick LastOpenInterestTick { get; set; }
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;
            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                var productId = 181;
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId is specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if we are null, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                Environment.Exit(1);
            }
        }

        private static class AccountValueKeys
        {
            public const string CashBalance = "CashBalance";
            public const string ExchangeRate = "ExchangeRate";
        }

        // these are fatal errors from IB
        private static readonly HashSet<int> ErrorCodes = new HashSet<int>
        {
            100, 101, 103, 138, 139, 142, 143, 144, 145, 200, 203, 300,301,302,306,308,309,310,311,316,317,320,321,322,323,324,326,327,330,331,332,333,344,346,354,357,365,366,381,384,401,414,431,432,438,501,502,503,504,505,506,507,508,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,10000,10001,10005,10013,10015,10016,10021,10022,10023,10024,10025,10026,10027,1300
        };

        // these are warning messages from IB
        private static readonly HashSet<int> WarningCodes = new HashSet<int>
        {
            102, 104, 105, 106, 107, 109, 110, 111, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 129, 131, 132, 133, 134, 135, 136, 137, 140, 141, 146, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 201, 303,312,313,314,315,319,325,328,329,334,335,336,337,338,339,340,341,342,343,345,347,348,349,350,352,353,355,356,358,359,360,361,362,363,364,367,368,369,370,371,372,373,374,375,376,377,378,379,380,382,383,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,402,403,404,405,406,407,408,409,410,411,412,413,417,418,419,420,421,422,423,424,425,426,427,428,429,430,433,434,435,436,437,439,440,441,442,443,444,445,446,447,448,449,450,10002,10003,10006,10007,10008,10009,10010,10011,10012,10014,10018,10019,10020,10052,10147,10148,10149,2100,2101,2102,2109,2148
        };

        // these require us to issue invalidated order events
        private static readonly HashSet<int> InvalidatingCodes = new HashSet<int>
        {
            104, // Can't modify a filled order
            10148, // OrderId <OrderId> that needs to be cancelled can not be cancelled, state:
            105, 106, 107, 109, 110, 111, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 129, 131, 132, 133, 134, 135, 136, 137, 140, 141, 146, 147, 148, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 163, 167, 168, 201,312,313,314,315,325,328,329,334,335,336,337,338,339,340,341,342,343,345,347,348,349,350,352,353,355,356,358,359,360,361,362,363,364,367,368,369,370,371,372,373,374,375,376,377,378,379,380,382,383,387,388,389,390,391,392,393,394,395,396,397,398,400,401,402,403,405,406,407,408,409,410,411,412,413,417,418,419,421,423,424,427,428,429,433,434,435,436,437,439,440,441,442,443,444,445,446,447,448,449,463,10002,10006,10007,10008,10009,10010,10011,10012,10014,10020,10058,2102
        };

        // these are warning messages not sent as brokerage message events
        private static readonly HashSet<int> FilteredCodes = new HashSet<int>
        {
            // 'Request Account Data Sending Error' can happen while connecting sometimes let's ignore it else it bubbles up to the user even if we connected successfully later
            542,
            10148, // we are going to handle it as an order event
            1100, 1101, 1102, 2103, 2104, 2105, 2106, 2107, 2108, 2119, 2157, 2158, 10197
        };

        // the default delay for IBAutomater restart
        private static readonly TimeSpan _defaultRestartDelay = TimeSpan.FromMinutes(5);

        private static TimeSpan _defaultWeeklyRestartUtcTime = GetNextWeekendReconnectionTimeUtc().TimeOfDay;

        private enum RequestType
        {
            PlaceOrder,
            UpdateOrder,
            CancelOrder,
            Subscription,
            FindContracts,
            ContractDetails,
            History,
            Executions,
            SoftContractDetails,    // Don't fail if we can't find the contract
        }

        private class RequestInformation
        {
            public int RequestId { get; set; }

            public RequestType RequestType { get; set; }

            public Symbol? AssociatedSymbol { get; set; }

            public string Message { get; set; }

            public HistoryRequest? HistoryRequest { get; set; }
        }
    }
}
