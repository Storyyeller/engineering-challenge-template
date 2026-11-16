# Read before getting started!!

First, read [src/main.rs](https://github.com/voluntas-io/engineering-challenge/blob/main/src/main.rs). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.


# Kalshi orderbook collector

Follows every open, non-multivariate Kalshi market and writes their order books to stdout, as SBE
messages written with the helpers above.

```sh
export KALSHI_API_KEY_ID=...                   # the key's UUID
export KALSHI_PRIVATE_KEY_PATH=path/to/key.pem # Ed25519 or RSA
cargo run --release > orderbooks.sbe           # ctrl-c to stop; logs go to stderr
cargo run --release -- series:KXHIGHNY > ny.sbe   # or only some series / events

cargo run --release --example dump -- orderbooks.sbe            # print the messages
cargo run --release --example dump -- orderbooks.sbe --check    # check the stream is consistent
```

Other settings (shards, replicas, verification, discovery, recycling) are env vars, documented at
the top of [src/main.rs](src/main.rs). Logs are labelled with the connection they came from (e.g.
`conn{shard=3 replica=1}`); `RUST_LOG=engineering_challenge=debug` adds per-market detail
(subscriptions, lifecycle events, resyncs), and `RUST_LOG=warn` shows only problems.

## Output

- A `PriceLevelUpdate` for every change to a book, with the level's new absolute quantity (zero
  removes it). `backtest_timestamp` is when we received the change, to the microsecond, and
  `exchange_timestamp` is Kalshi's timestamp for it (millisecond precision).
- A `Snapshot` when a book is first synced, re-synced after a problem, or switches to another
  connection (see below).
- A `Snapshot` of every book in sync at the start of every minute. Its `received_at` is the minute
  boundary exactly; it reflects everything received before it. (A book in the middle of switching
  connection is left out; its switch snapshot follows moments later.)
- Messages are in the order they were received (`backtest_timestamp`/`received_at` never go
  backwards), so every update applies to the latest snapshot before it.
- Kalshi books hold bids for YES and bids for NO; a NO bid at p is a YES ask at $1 - p. The output is
  the YES book: bids are YES bids, asks are NO bids at the complementary price.
- The symbol is the Kalshi market ticker. Prices and quantities are fixed point
  (1_000_000 = $1 / one contract).

The writer pads output to 64 MiB chunks (the size `AlignedSbeReader` reads), so it reaches stdout in
64 MiB pieces; ctrl-c flushes the last one.

## Design

- [`src/kalshi/`](src/kalshi): everything Kalshi-specific. `session`, `subscriptions` and `book`
  are the I/O-free logic for one connection (subscribing in batches, retries and backoff,
  sequence-gap detection, resyncing books from snapshots, periodic verification against fresh
  snapshots). They're tested with unit tests and a randomized simulated exchange that injects
  faults (`sim.rs`). `connection` is the websocket I/O around it, and `discovery` lists markets
  over REST.
- [`src/collector/`](src/collector): the exchange-agnostic half. Connections report what happens to
  their books as `SourceEvent`s; the `Publisher` merges them into the output stream.

**Load balancing.** Markets are split into `KALSHI_SHARDS` (default 32) shards by a hash of the
ticker, each on its own websocket. New markets are picked up from `market_lifecycle_v2`
notifications, settled ones are dropped, and the full list is re-read over REST every 15 minutes,
and after a reconnect or a lifecycle sequence gap. Markets missing from two full listings in a row
are dropped too, in case their settlement notification was missed.

**Redundancy.** Each shard is followed on `KALSHI_REPLICAS` (default 2) connections, each keeping
its own books. For each market the publisher follows one connection and switches to another,
publishing a snapshot of its book, when:
- the followed connection's book goes out of sync (sequence gap, bad message, failed check);
- the connection drops or stops following the market;
- another connection has stayed ahead of it (seen a later exchange clock for the market) for a
  second. This catches stalled or buffering connections that haven't dropped.

Connections are also recycled every 6 hours (`KALSHI_RECYCLE_MINS`), with replicas of a shard
taking turns. Everything runs in one process, since the output goes to stdout. A sturdier setup
would put collectors on separate machines and merge their streams downstream, but the publisher
already merges independent sources, so the same logic would apply.

## Findings from running against the live exchange

- About 166k open non-multivariate markets; about 30k book updates/s exchange-wide; about 20 MB/s
  inbound with two replicas. The whole collector used about 2 cores and 560 MB.
- Kalshi gives each connection one `orderbook_delta` subscription. A second `subscribe` to the
  channel is merged into the first (its markets are added, and it's answered with a plain `ok`).
  So markets can only be spread over **connections**, not subscriptions.
- With about 20k markets per connection (8 shards), Kalshi dropped one connection's subscription
  in most shards within a couple of minutes, with `error 25: Subscription buffer overflow`. It
  then rejects commands for it (`error 7: Unknown subscription ID`). The markets on it just stop
  updating, with no sequence gap. The collector now reconnects when this happens, and the
  replicas covered it in the meantime. With 32 shards (about 5k markets per connection) there
  were no overflows in several 5-minute runs.
- Listing every market over REST takes about 170 pages and runs into rate limits (HTTP 429). It's
  paced, retries pages, and publishes partial results as it goes, so subscribing starts within
  seconds.
- In the first few minutes, while every connection is working through its initial snapshots,
  a few thousand markets switch connection because one replica gets ahead of the other. After
  that, switches are rare.
