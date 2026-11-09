![header-cheetah](https://user-images.githubusercontent.com/79997186/184224088-de4f3003-0c22-4a17-8cc7-b341b8e5b55d.png)

&nbsp;
&nbsp;
&nbsp;

## Introduction

This repository hosts the Interactive Brokers (IB) Brokerage Plugin Integration with the QuantConnect LEAN Algorithmic Trading Engine. LEAN is a brokerage agnostic operating system for quantitative finance. Thanks to open-source plugins such as this [LEAN](https://github.com/QuantConnect/Lean) can route strategies to almost any market.

[LEAN](https://github.com/QuantConnect/Lean) is maintained primarily by [QuantConnect](https://www.quantconnect.com), a US based technology company hosting a cloud algorithmic trading platform. QuantConnect has successfully hosted more than 200,000 live algorithms since 2015, and trades more than $1B volume per month.


### About Interactive Brokers

<p align="center">
<picture >
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/79997186/188234310-4d3cf2e5-4a88-4f8b-abaf-1f4c8f5ae71e.png">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/79997186/188234305-1924c76a-86c1-46cf-b9bf-b29f8449e97e.png">
  <img alt="introduction" width="40%">
</picture>
<p>

IB was founded by Thomas Peterffy in 1993 with the goal to "create technology to provide liquidity on better terms. Compete on price, speed, size, diversity of global products and advanced trading tools". IB provides access to trading Equities, ETFs, Options, Futures, Future Options, Forex, Gold, Warrants, Bonds, and Mutual Funds for clients in over [200 countries and territories](https://www.interactivebrokers.com/en/index.php?f=7021) with no minimum deposit. IB also provides paper trading, a trading platform, and educational services.

For more information about the IB brokerage, see the [QuantConnect-IB Integration Page](https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/interactive-brokers).

## Using the Brokerage Plugin
  
### Deploying IB with VSCode User Interace

  You can deploy using a visual interface in the QuantConnect cloud. For instructions, see the [QuantConnect-IB Integration Page](https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/interactive-brokers). 
  
  ![deploy-ib](https://user-images.githubusercontent.com/38889814/207988504-86a110b9-dc74-4d8a-83ac-e0f0572d413f.gif) 

  In the QuantConnect Cloud Platform, you can harness the QuantConnect Live Data Feed, the IB Live Data Feed, or both. For most users, this is substantially cheaper and easier than self-hosting.
  
### Deploying Interactive Brokers with LEAN CLI

To deploy live algorithms with Interactive Brokers through the LEAN CLI, see [Interactive Brokers](https://www.quantconnect.com/docs/v2/lean-cli/live-trading/brokerages/interactive-brokers) in the LEAN CLI documentation.

## Account Types

The IB API does not support the IBKR LITE plan. You need an IBKR PRO plan. Individual and Financial Advisor (FA) accounts are available. IB supports cash and margin accounts.

## Order Types and Asset Classes

The following table describes the order types that IB supports. For specific details about each order type, refer to the IB documentation.

| Order Type  | IB Documentation Page |
| ----------- | ----------- |
| `MarketOrder` | [Market Orders](https://www.interactivebrokers.com/en/index.php?f=602) |
| `LimitOrder` | [Limit Orders](https://www.interactivebrokers.com/en/index.php?f=593) |
| `LimitIfTouchedOrder` | [Limit if Touched Orders](https://www.interactivebrokers.com/en/index.php?f=592) |
| `StopMarketOrder` | [Stop Orders](https://www.interactivebrokers.com/en/index.php?f=609) |
| `StopLimitOrder` | [Stop-Limit Orders](https://www.interactivebrokers.com/en/index.php?f=608) |
| `TrailingStopOrder` | [Trailing Stop Orders](https://www.interactivebrokers.com/en/trading/orders/trailing-stops.php) |
| `MarketOnOpenOrder` | [Market-on-Open (MOO) Orders](https://www.interactivebrokers.com/en/index.php?f=598) |
| `MarketOnCloseOrder` | [Market-on-Close (MOC) Orders](https://www.interactivebrokers.com/en/index.php?f=599) |
| `ExerciseOption` | [Options Exercise](https://www.interactivebrokers.ca/en/trading/exerciseCloseout.php) |


## Downloading Data

For local deployment, the algorithm needs to download the following datasets:

- [US Equities Security Master](https://www.quantconnect.com/datasets/quantconnect-us-equity-security-master) provided by QuantConnect
- [US Equities](https://www.quantconnect.com/datasets/algoseek-us-equities)
- [US Coarse Universe](https://www.quantconnect.com/datasets/quantconnect-us-coarse-universe-constituents)
- [US Equity Options](https://www.quantconnect.com/datasets/algoseek-us-equity-options)
- [FOREX Data](https://www.quantconnect.com/datasets/oanda-forex)
- [US Futures Security Master](https://www.quantconnect.com/datasets/quantconnect-us-futures-security-master)
- [US Futures](https://www.quantconnect.com/datasets/algoseek-us-futures)
- [US Future Options](https://www.quantconnect.com/datasets/algoseek-us-future-options)
- [US Cash Indices](https://www.quantconnect.com/datasets/tickdata-us-cash-indices)
- [US Index Options](https://www.quantconnect.com/datasets/algoseek-us-index-options)

## Brokerage Model

Lean models the brokerage behavior for backtesting purposes. The margin model is used in live trading to avoid placing orders that will be rejected due to insufficient buying power.

You can set the Brokerage Model with the following statements

    SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage, AccountType.Cash);
    SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage, AccountType.Margin);

[Read Documentation](https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/interactive-brokers)

### Fees

We model the order fees of IB for each asset class. For information about each asset class, see [Fees](https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/interactive-brokers#07-Fees).

### Margin

We model buying power and margin calls to ensure your algorithm stays within the margin requirements.

[Read Documentation](https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/interactive-brokers)

#### Buying Power

In the US, IB allows up to 2x leverage on Equity trades for margin accounts. In other countries, IB may offer different amounts of leverage. To figure out how much leverage you can access, check with your local legislation or contact an IB representative. We model the US version of IB leverage by default.

#### Margin Calls

Regulation T margin rules apply. When the amount of margin remaining in your portfolio drops below 5% of the total portfolio value, you receive a [warning](https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/margin-calls#08-Monitor-Margin-Call-Events). When the amount of margin remaining in your portfolio drops to zero or goes negative, the portfolio sorts the generated margin call orders by their unrealized profit and executes each order synchronously until your portfolio is within the margin requirements.

### Slippage

Orders through IB do not experience slippage in backtests. In paper trading and live trading, your orders may experience slippage.

### Fills

We fill market orders immediately and completely in backtests. In live trading, if the quantity of your market orders exceeds the quantity available at the top of the order book, your orders are filled according to what is available in the order book.

### Settlements

If you trade with a margin account, trades settle immediately. If you trade with a cash account, Equity trades settle 3 days after the transaction date (T+3) and Option trades settle on the business day following the transaction (T+1).


### Deposits and Withdraws

You can deposit and withdraw cash from your brokerage account while you run an algorithm that's connected to the account. We sync the algorithm's cash holdings with the cash holdings in your brokerage account every day at 7:45 AM Eastern Time (ET).


&nbsp;
&nbsp;
&nbsp;

![whats-lean](https://user-images.githubusercontent.com/79997186/184042682-2264a534-74f7-479e-9b88-72531661e35d.png)

&nbsp;
&nbsp;
&nbsp;

LEAN Engine is an open-source algorithmic trading engine built for easy strategy research, backtesting, and live trading. We integrate with common data providers and brokerages, so you can quickly deploy algorithmic trading strategies.

The core of the LEAN Engine is written in C#, but it operates seamlessly on Linux, Mac and Windows operating systems. To use it, you can write algorithms in Python 3.8 or C#. QuantConnect maintains the LEAN project and uses it to drive the web-based algorithmic trading platform on the website.

## Contributions

Contributions are warmly very welcomed but we ask you to read the existing code to see how it is formatted, commented and ensure contributions match the existing style. All code submissions must include accompanying tests. Please see the [contributor guide lines](https://github.com/QuantConnect/Lean/blob/master/CONTRIBUTING.md).

## Code of Conduct

We ask that our users adhere to the community [code of conduct](https://www.quantconnect.com/codeofconduct) to ensure QuantConnect remains a safe, healthy environment for
high quality quantitative trading discussions.

## License Model

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You
may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
