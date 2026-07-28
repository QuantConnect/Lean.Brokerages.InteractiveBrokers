# ADR 0001: One-Cancels-the-Other (OCO) orders at InteractiveBrokers

## Status

Proposed - 2026-07-28

## Purpose

This is the InteractiveBrokers part of the OCO work. The Lean core part is
`Lean/Documentation/ADR/0002-one-cancels-the-other-order-group.md`. That one adds the group to the
engine. This one makes the group work live at IB.

It also answers a question that came up while testing: **why does only one order report
`PartiallyFilled`, and not all of them?**

## What is an OCO group

You want to sell 500 shares of AAPL. You have two wishes:

- sell high, at $360, to take profit;
- sell fast, at $320, to stop a loss.

You place both orders together in one group. Both wait in the market. The first one that fills wins.
The broker then cancels the other one. So you sell 500 shares, never 1000.

That is the whole idea: **one group, one winner.**

## How IB builds the group

IB has no single "OCO request". There is no one message you send. Instead, every leg is a normal
order, and two extra fields tie them together:

- `OcaGroup` - a text label. Every order with the same label is in the same group.
- `OcaType` - what happens when one leg fills. We use `1`, which means "cancel the other legs, and
  only route one leg at a time".

We build the label from the Lean group id, so it looks like `lean-oco-1583941058`.

Doc: https://interactivebrokers.github.io/tws-api/oca.html

### Each leg is its own order

A ratio combo order in Lean is **one** IB order with one id and one basket (BAG) contract. An OCO
group is not that. Each leg is a **separate** IB order:

- its own IB order id;
- its own plain contract, not a BAG;
- its own `placeOrder` call.

That matters because each leg has to be filled, canceled and reported on its own.

### About the `Transmit` flag

The IB docs show a sample that sets `Transmit = false` on every leg except the last one. The idea is
that the last leg wakes up the whole group at once.

**That does not work in IB Gateway.** We tried it and the held legs never came alive. Only the last
leg ever reached the market; the others stayed silent, with no event at all.

The reason: an untransmitted order is a TWS **window** feature. It shows up as a grey row that a
person clicks to send. Gateway has no window and no person, so the row never gets clicked. Also, the
"the last one sends the earlier ones" line only appears in the VB sample in the docs, not the C# one.

So every leg is sent with `Transmit = true`. Safety does not come from `Transmit`. It comes from
`OcaType = 1`, which the docs describe as: *"only one order in the group will be routed at a time to
remove the possibility of an overfill."*

## How `PartiallyFilled` works

This is the part that surprised us, so here it is in detail.

### The short answer

**Partial fills belong to one order, not to the group.** You buy 500 shares with one order. The
market hands them to you in small batches. Each batch is one `PartiallyFilled` event on **that same
order**. When the last batch arrives, the event is `Filled` instead.

Sibling legs never share the work. They do not each fill a piece. If they did, you would buy far
more than 500 shares, and that is exactly the mistake OCO exists to prevent.

### A picture

You go to a shop and ask for 500 apples. The shop does not have 500 apples on the counter. So:

- the clerk brings 80 apples -> "here are 80, 420 to go"
- the clerk brings 20 apples -> "here are 20, 400 to go"
- the clerk brings 80 apples -> "here are 80, 320 to go"
- ... and so on ...
- the clerk brings the last 80 -> "done, you have all 500"

Every trip is a `PartiallyFilled` event. The last trip is a `Filled` event. It is **one order** and
**one clerk** the whole time. The other clerks in the shop (the sibling legs) are not helping. They
are waiting, and when your order is done they are sent home.

### What the real log showed

From a live paper-trading run (`bin/Debug/log.txt`, 2026-07-28 19:40:50). Buy 500 AAPL, limit price
set far through the market so it fills at once. Lean order 1, IB order id 2:

| IB execution | shares | running total | Lean event         |
| ------------ | ------ | ------------- | ------------------ |
| 1            | 80     | 80            | PartiallyFilled 80 |
| 2            | 20     | 100           | PartiallyFilled 20 |
| 3            | 80     | 180           | PartiallyFilled 80 |
| 4            | 80     | 260           | PartiallyFilled 80 |
| 5            | 80     | 340           | PartiallyFilled 80 |
| 6            | 80     | 420           | PartiallyFilled 80 |
| 7            | 80     | 500           | **Filled** 80      |

Six `PartiallyFilled` events, then one `Filled`. All of them carry **OrderID 1**. The sum is 500,
which is the full order. No other Lean order id appears in any fill event.

### Where the events come from in the code

Two IB callbacks work as a pair, and Lean needs both before it can send anything:

1. `HandleExecutionDetails` - "80 shares traded at 339.25". This says *what* traded.
2. `HandleCommissionReport` - "that trade cost 1.00024 USD". This says *what it cost*.

They arrive separately and can arrive in either order. Whichever comes second calls `EmitOrderFill`,
which builds the Lean `OrderEvent`. The status is decided by one simple line:

```csharp
var status = remainingQuantity > 0 ? OrderStatus.PartiallyFilled : OrderStatus.Filled;
```

So `PartiallyFilled` simply means "shares are still owed". Nothing about groups is involved.

### One thing we had to fix

The IB plugin used to hold back fills for **any** order that belongs to a group. It waited until
every leg had filled, then sent all the fill events together. That is right for a ratio combo, where
all legs trade as one unit.

It is wrong for OCO. In an OCO group only one leg ever fills, so the wait could never finish. Every
fill would sit in the buffer until a 30 second timeout ran out.

Now the wait only applies when the group is a real combo (`ComboType.Combo`). An OCO leg reports its
fill straight away.

## What happens to the legs that do not win

In the same test run, legs 2 to 5 were sent **after** leg 1 had already filled. IB answered each one
with:

```
201 - Order rejected - reason:OCA group is already filled
```

The IB status was `Inactive`, and Lean turned that into `Invalid`.

This is IB doing its job. It refused to buy another 2000 shares we did not want. But note what it
means: the group never really existed as five live legs. Leg 1 filled before the others were even
sent.

That only happens because this test prices every leg to fill at once, which is not how OCO is used
in real life. In the normal case (a take-profit and a stop-loss that both sit and wait), all legs
reach the market first, and then the winner's fill cancels the rest properly.

Two open points from this:

- **Rejected vs canceled.** A losing leg reports `Invalid` here, but Lean core's lifecycle table
  expects `Canceled`. Worth deciding whether error 201 should map to `Canceled` when the order is
  part of an OCO group.
- **Placement race.** Legs are sent one after another. If a leg can fill instantly, it can win before
  its siblings are sent. Resting legs do not have this problem.

## Files changed

All in `QuantConnect.InteractiveBrokersBrokerage/InteractiveBrokersBrokerage.cs`:

- `IBPlaceOrder` - sends each OCO leg as its own order, and lets a single leg update on its own.
- `ConvertOrder` - sets `OcaGroup` and `OcaType`, and takes quantity and direction from the leg
  itself instead of the group (there is no ratio math in an OCO group).
- `EmitOrderFill` - only waits for all legs when the group is a real combo.
- `HandleOrderStatusUpdates` / `HandleOpenOrder` - only cache the shared combo contract for a real
  combo.
- `GetOpenOrdersInternal` / `ConvertOrders` - after a restart, orders that share an `OcaGroup` label
  are put back into one Lean group.

Lean core also needs `InteractiveBrokersBrokerageModel.SupportsGroupExecution` to return `true` for
`OneCancelsTheOther`. Without it the engine blocks the group before the plugin ever sees it.

## Tests

`QuantConnect.InteractiveBrokersBrokerage.Tests/InteractiveBrokersBrokerageAdditionalTests.cs`:

- `SendOneCancelsTheOtherOrder` - two resting legs. Checks both legs are placed and each one gets its
  own broker id.
- `SendOneCancelsTheOtherOrderWithPartialFill` - five legs priced to fill at once. Shows the partial
  fill chain above, and that only one leg can win.

Both need a live IB Gateway, so they are marked `Explicit`.
