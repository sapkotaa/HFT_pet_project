# hft_lob

A limit order book and matching engine in C++20, built up in three
runnable layers:

1. **An offline latency harness** (`hft_lob`) — feed → book → strategy in
   one process, measuring tick-to-book latency with a percentile
   histogram. Runs out of the box with a synthetic feed, or replay a CSV.
2. **A UDP multicast demo** (`feed_publisher` / `engine_subscriber` /
   `supervisor`) — the same matching engine as a standalone process
   joining a multicast feed over the real network stack, plus a process
   supervisor that restarts it on crash.
3. **A live gateway system** — a TCP order-entry gateway in front of a
   sequencer, a matching-engine consumer, a write-ahead log, a SQLite
   store, and a web UI. This is the part that behaves like an actual
   (toy) exchange: clients connect, send orders over the wire, and get
   asynchronous execution reports back as the matching engine actually
   processes them.

All three share the same core types (`include/types.hpp`,
`include/order_book.hpp`) — the gateway system is not a rewrite, it's the
first two layers' matching engine given a real front door, a durable
log, and a queryable database.

---

## Part 1 — Offline latency harness

```
include/
  types.hpp             Order / Trade / Side — the shared vocabulary
  object_pool.hpp        Fixed-capacity pool allocator (no malloc/new per order)
  spsc_ring_buffer.hpp   Lock-free single-producer/single-consumer queue
  order_book.hpp         Price-time priority book + matching logic
  market_data_feed.hpp   CSV replay, or a synthetic random-walk generator
  strategy.hpp           Minimal market maker that quotes around mid
  timer.hpp              RDTSC-based nanosecond timer, self-calibrating
  latency_histogram.hpp  Log-linear percentile histogram (HdrHistogram-style)
src/main.cpp              Wires all of the above into one running program
data/sample_feed.csv      300 rows of sample order flow for --data
```

### How data actually flows

```
feed thread                    engine thread
------------                   -------------
generate/read orders   -->     pop from ring buffer
push into SPSC ring            add_limit_order() into the book
                                (matches against opposite side first,
                                 rests any unfilled remainder)
                                strategy.on_tick() reads best bid/ask,
                                 requotes if the mid moved enough
                                record RDTSC latency for this event
```

Two threads, one lock-free queue between them — the same shape as a real
feed handler / matching engine split, just without the network layer.

### Run

```
./build/hft_lob                               # 200k synthetic events
./build/hft_lob --count 1000000               # 1M synthetic events
./build/hft_lob --data data/sample_feed.csv   # replay real-ish sample data
./build/hft_lob --test                        # matching-engine correctness checks
```

### Sample output (300k synthetic events, -O3 -march=native)

```
Processed 300000 orders, generated 265943 trades
Resting orders remaining: 25521 (548 bid levels, 567 ask levels)

=== order-to-book tick latency (n=300000) ===
  min    :         32 ns
  mean   :        166.4 ns
  p50    :        126 ns
  p90    :        226 ns
  p99    :        668 ns
  p99.9  :       3648 ns
  p99.99 :      29184 ns
  max    :     502194 ns
```

The tail (p99.9 and up) is dominated by `std::map`'s O(log n) price-level
lookup occasionally hitting a cache miss, plus OS scheduling noise from
running two threads on a shared, non-isolated core.

---

## Part 2 — UDP multicast demo

```
src/feed_publisher.cpp     Sends synthetic or CSV-replayed order flow over UDP multicast
src/engine_subscriber.cpp  Standalone process: joins the multicast group, runs its own
                            OrderBook + MarketMaker, measures wire-to-book latency
src/supervisor.cpp         fork/exec's both as real child processes, restarts
                            engine_subscriber on crash with backoff
```

This is a separate, self-contained demo from the gateway system below —
`engine_subscriber` has its own in-memory `OrderBook` and never talks to
the gateway, sequencer, or database. It exists to measure a genuinely
different, more honest number: latency from wire receipt to book update,
including the OS network stack, rather than the in-process version's
tick-to-book.

```
./build/supervisor ./build              # launches both, restarts subscriber on crash
# or run them separately:
./build/engine_subscriber --group 239.255.0.1 --port 30001 &
./build/feed_publisher --count 200000
```

---

## Part 3 — Live gateway system

### Why a sequencer

A gateway that pushes client orders directly into a matching engine has
two producers of book mutations (client orders, and eventually replayed
market data) with no defined order between them. Real venues resolve
this with a **sequencer**: one component that assigns a global,
monotonic sequence number to every inbound event and publishes them as
one ordered stream. The matching engine becomes a pure, deterministic
function of that stream — nothing else is a source of truth. Recovery
becomes "replay the log from sequence N," market data becomes a
projection of the same stream, and any bug becomes a reproducible
sequence range.

### Architecture

```
                        ┌─────────────┐        ┌──────────────────┐
  TCP clients  ───────► │   gateway   │──push──►│ Sequencer.inputs_ │
  (order entry)         │ (1 thread,  │         │ [ProducerId::*]   │
                         │  kqueue)    │         └────────┬──────────┘
                         └──────▲──────┘                  │ Sequencer thread:
                                │                          │ round-robin drain,
                          drain_acks()                     │ stamp seq_num,
                                │                          │ publish
                         ┌──────┴──────┐                   ▼
                         │  AckQueue   │◄──────┐  ┌──────────────────┐
                         │(SPSC, matcher       │  │   BroadcastRing   │
                         │  -> gateway)        │  │ (1 writer, N      │
                         └──────▲──────┘       │  │  independent      │
                                │              │  │  readers)          │
                    push_ack()  │              │  └───┬────────┬───────┘
                         ┌──────┴──────────┐   │      │        │
                         │ MatchingEngine  │◄──┘  ┌────┘        └────┐
                         │ Consumer        │      │                 │
                         │ (owns OrderBook)│      ▼                 ▼
                         └──────┬──────────┘  ┌─────────┐    ┌─────────────┐
                                │ push_ack()   │   WAL   │    │ DbConsumer  │
                         ┌──────┴──────┐      │ Consumer │    │ (SQLite)    │
                         │  fills queue │      └────┬────┘    └──────┬──────┘
                         │(SPSC, matcher│           ▼                ▼
                         │  -> DB)      │   wal/events.bin   db/hft_lob.db
                         └──────┬───────┘                           ▲
                                └──────────────────────────────────►┘
                                        (fills queue, drained by DbConsumer)

  db/hft_lob.db ◄── read-only, WAL journal mode ──── scripts/web_bridge.py ◄── browser (ui/)
```

Four threads: the gateway's own event loop (main thread), the sequencer,
the matching engine consumer, the WAL consumer, and the DB consumer —
each of the last three is an independent reader of the same
`BroadcastRing`, none can stall the others, and none is the source of
truth except the sequence itself.

### File map

```
include/event.hpp                    SequencedEvent — the unified, wire-independent event type
include/broadcast_ring.hpp           Single-writer, N-independent-reader ring buffer
include/sequencer.hpp                Drains per-producer SPSC queues, stamps seq_num, publishes
include/outbound_ack.hpp             OutboundAck POD + AckQueue typedef (matcher -> gateway / DB)
include/matching_engine_consumer.hpp Owns the OrderBook, applies events, produces acks
include/wal_consumer.hpp             Appends every event to an append-only binary log
include/db_consumer.hpp              Appends orders + execution reports to SQLite
include/gateway_server.hpp           kqueue TCP server, wire protocol dispatch
include/gateway_protocol.hpp         Wire message structs (Logon, NewOrder, CancelOrder, ...)
include/frame_reader.hpp             Byte stream -> discrete framed messages
include/session_state.hpp            Per-connection logon/heartbeat/seq-gap tracking
src/gateway_main.cpp                 Wires all of the above together, owns startup/shutdown order

scripts/manual_gateway_client.py     Scripted CLI scenario, no browser needed
scripts/web_bridge.py                HTTP/SSE bridge: browsers can't open raw TCP sockets
ui/index.html, ui/app.js             The operator console served by the bridge
```

### Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

The gateway and its tests link SQLite3 via CMake's `FindSQLite3` module
(`find_package(SQLite3 REQUIRED)`). macOS ships it with the Xcode command
line tools; on Linux install a dev package first (e.g.
`apt install libsqlite3-dev`).

### Run it

```
./build/gateway [port] [wal_path] [db_path]     # defaults: 9000, wal/events.bin, db/hft_lob.db
python3 scripts/web_bridge.py --gateway-port 9000 --http-port 8080
# open http://127.0.0.1:8080/
```

Or skip the browser entirely and run the scripted scenario:

```
python3 scripts/manual_gateway_client.py 127.0.0.1 9000
```

Both talk the same raw binary protocol defined in
`include/gateway_protocol.hpp` — a 7-byte header (`body_length`,
`msg_type`, `seq_num`) followed by a fixed-size, `#pragma pack(1)` body
per message type. `FrameReader` (`include/frame_reader.hpp`) turns a raw
byte stream back into discrete frames on the receiving end.

### Order lifecycle and the order-id scheme

A client's `client_order_id` is only unique *within its own session* —
different connections routinely reuse the same values (every client
tends to start counting at 1). `OrderBook::OrderId` must be globally
unique within one `OrderBook` instance, so `MatchingEngineConsumer` uses
each `NewOrder` event's own `seq_num` (globally unique and monotonic,
stamped once by the single-writer `Sequencer`) as the exchange-side
order id. A `CancelOrder` event never mints an id — it resolves the
*existing* order via an in-memory `(session_id, client_order_id) ->
exchange_order_id` map the matching engine maintains itself, since it's
the only place both ids are known together.

Every `OutboundAck` (Accepted / PartialFill / Filled / Canceled /
Rejected) carries `exchange_order_id` set to the *originating* order's
own `seq_num` — including the ack sent to a resting order's owner when
someone else's incoming order fills it. That makes `exchange_order_id`
the correct join key for reconstructing an order's full lifecycle later
(see the history query below) — `(session_id, client_order_id)` is
**not** safe for that once a session disconnects, because `session_id`
is the raw file descriptor and fds get reused (see Known limitations).

### Persistence: WAL vs. SQLite

Both are independent `BroadcastRing` consumers, both batch their commits
on the same "N records or T ms, whichever first" policy, and both
sacrifice a small durability window in exchange for not paying a full
fsync/commit per event:

- **`wal/events.bin`** — every `SequencedEvent`, raw `memcpy`'d as a
  fixed-size binary record, fsync'd every 256 records or 50ms. This is
  the true source of truth for replay: the WAL plus the initial state is
  everything needed to reproduce the exact same book.
- **`db/hft_lob.db`** (SQLite, WAL journal mode) — the same events plus
  every execution report, in two queryable tables, committed every 256
  rows or 50ms. This is for asking questions, not for replay.

```sql
CREATE TABLE orders (
  seq_num INTEGER PRIMARY KEY,   -- also NewOrder's exchange_order_id
  ts_ns INTEGER NOT NULL,
  event_type INTEGER NOT NULL,   -- 1=NewOrder, 2=CancelOrder
  session_id INTEGER NOT NULL,   -- raw fd — see Known limitations
  client_order_id INTEGER NOT NULL,
  side INTEGER NOT NULL,         -- 0=Buy, 1=Sell (meaningless for cancels)
  price INTEGER NOT NULL,
  quantity INTEGER NOT NULL
);

CREATE TABLE execution_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ns INTEGER NOT NULL,
  session_id INTEGER NOT NULL,
  client_order_id INTEGER NOT NULL,
  exchange_order_id INTEGER NOT NULL,   -- the *originating* order's seq_num
  status INTEGER NOT NULL,              -- 1=Accepted 2=Rejected 3=Filled 4=PartialFill 5=Canceled
  fill_price INTEGER NOT NULL,
  fill_quantity INTEGER NOT NULL,
  leaves_quantity INTEGER NOT NULL
);
```

Example queries (`sqlite3 db/hft_lob.db`):

```sql
-- Full lifecycle of one order, oldest first
SELECT ts_ns, status, fill_price, fill_quantity, leaves_quantity
FROM execution_reports WHERE exchange_order_id = 6 ORDER BY id;

-- Last 20 fills across the whole book
SELECT ts_ns, exchange_order_id, fill_price, fill_quantity
FROM execution_reports WHERE status IN (3,4) ORDER BY id DESC LIMIT 20;

-- Reject rate
SELECT
  SUM(status = 2) * 1.0 / COUNT(*) AS reject_rate
FROM execution_reports;
```

`scripts/web_bridge.py`'s `/api/history` endpoint runs a fuller version
of the first two queries (a window-function join reconstructing latest
status per order) — see that file for the exact SQL, and the comment
directly above it explaining why it joins on `exchange_order_id` and not
`(session_id, client_order_id)`.

### Web UI

`scripts/web_bridge.py` is a stdlib-only Python process that holds one
persistent, logged-on TCP session to the gateway and re-exposes it as
plain HTTP, since a browser can't open a raw TCP socket:

| Route | Method | Does |
|---|---|---|
| `/` | GET | Serves `ui/index.html` |
| `/app.js` | GET | Serves `ui/app.js` |
| `/events` | GET | Server-Sent Events: a snapshot on connect, then live order updates |
| `/api/order` | POST | `{side, price, quantity}` → submits a `NewOrder` |
| `/api/cancel` | POST | `{client_order_id}` → submits a `CancelOrder` |
| `/api/history` | GET | `?limit=N` — persisted order/execution history from SQLite |
| `/api/stats` | GET | `{max_seq, order_count}` — the true global sequence position, from the DB |

`ui/index.html` + `ui/app.js` is an "operator console": an order-entry
ticket (side/price/qty, with `B`/`S`/`Enter`/`F1`–`F3` keyboard
shortcuts), a live blotter, a trade tape, a persisted-history panel
(proves the DB survives a bridge or browser restart — the other two
panels don't), a live sequence-counter readout sourced from `/api/stats`
(the one place that reflects the *true* sequencer position, since a
`CancelOrder`'s own `seq_num` is never echoed back to any client), and a
round-trip latency readout measured client-side from order submission to
first ack.

### Testing

```
cd build && ctest --output-on-failure
```

| Test | Covers |
|---|---|
| `frame_reader_tests` | Byte stream → framed messages |
| `session_state_tests` | Logon, heartbeat timing, inbound seq-gap detection |
| `broadcast_ring_tests` | Multi-consumer fairness, lapping detection, the late-registration stale-read hazard |
| `matching_engine_consumer_tests` | Order-id scheme, fill routing to both sides of a cross, cancel resolution, client_order_id reuse across sessions |
| `wal_consumer_tests` | Binary record round-trip |
| `db_consumer_tests` | SQLite row persistence, session-collision isolation |

Manual end-to-end: `python3 scripts/manual_gateway_client.py` exercises
Logon → resting order → crossing fill → cancel → reject-unknown-cancel
against a running gateway and prints every execution report as it
arrives asynchronously.

### Known limitations

- **`session_id` is the raw file descriptor.** It gets reused once a
  connection closes. This isn't hypothetical — it's exactly what broke
  the first version of `/api/history`'s SQL during testing (see the
  comment above `HISTORY_QUERY` in `web_bridge.py`). The matching
  engine's own in-memory state isn't affected (an order is always fully
  resolved — filled or canceled — before its session could plausibly be
  reused), but anything doing historical analysis must join on
  `exchange_order_id`, never on `(session_id, client_order_id)`.
- **`RiskEngine::check()` is a stub** (`include/gateway_server.hpp`) —
  always accepts. No max-size, notional, or price-collar checks yet.
- **Single-process only.** `BroadcastRing` lives in plain process memory,
  not shared memory — every consumer has to be a thread inside the
  `gateway` binary. There's no cross-process or cross-host fan-out.
- **`ingress_ts_ns` is never stamped** by the gateway (`SequencedEvent`'s
  own comment says it should be) — a two-line follow-up, not done here
  to avoid fixing it asymmetrically for only one message type.
- **No snapshotting or WAL-replay recovery tooling yet.** The WAL has
  everything needed to rebuild the book from scratch, but nothing reads
  it back in on startup — a crash currently means starting from an empty
  book, not replaying.
- **Backpressure drops, it doesn't degrade gracefully everywhere.** The
  ack-to-gateway path spins with a bounded cap before dropping (needed
  to avoid a shutdown deadlock — see the comment on
  `MatchingEngineConsumer::push_ack`); the fills-to-DB path is a single
  non-blocking try, since losing a persistence row is a lesser evil than
  losing a client-facing ack.

### Status vs. the long-term roadmap

This project has an aspirational 20-week, 5-phase roadmap (sequencer →
performance → protocol realism → strategy/simulation → operability).
Where things actually stand:

| Item | Status |
|---|---|
| 1.1 Sequencer / event bus | **Done** |
| 1.2 Write-ahead log | **Done** |
| Matching engine as a sequencer consumer | **Done** |
| — SQLite persistence (not on the original roadmap) | **Done** |
| — Web UI / control plane (roadmap item 5.5) | **Done** (live blotter, trade tape, history, order entry) |
| 1.3 Snapshotting + recovery | Not started |
| 1.4 Pre-trade risk engine | Stub only (`RiskEngine::check` always returns true) |
| 1.5 Drop copy | Not started |
| Phase 2 (perf: array price levels, shm transport, CPU pinning, io_uring) | Not started |
| Phase 3 (ITCH decoder, FIX adapter, MD fanout) | Not started |
| Phase 4 (strategy plugin API, deterministic replay, options) | Not started |
| Phase 5 (structured logging, config hot-reload, benchmark suite, chaos testing) | Not started |

---

## Design decisions worth defending in an interview

- **Integer prices, never floats.** Prices are ticks (`int64_t`/`uint32_t`
  on the wire), not dollars-as-double. Floating point comparison and
  rounding in a matching engine is a correctness bug waiting to happen.
- **Object pool, not malloc per order.** `ObjectPool<T, Capacity>` is a
  fixed array with an intrusive free list — acquire/release are O(1) and
  touch no allocator after startup.
- **std::map for price levels, on purpose (for now).** Correctness-first.
  O(log n) per touch, not O(1) — a known, intentional trade-off (see
  Optimization roadmap below), not an oversight.
- **`seq_num` as the exchange order id, not `client_order_id`.** The only
  identifier that's actually global by construction — see "Order
  lifecycle" above.
- **Every consumer is independent and can't be blocked by another.**
  `BroadcastRing` gives the matcher, the WAL, and the DB writer their own
  cursor each; a slow DB commit can't add latency to matching, and a slow
  WAL fsync can't delay a client's ack.
- **Backpressure is documented per-queue, not uniform.** The sequencer
  spins forever rather than drop (total order must never have gaps); the
  gateway-facing ack queue spins with a bounded cap (unconditional
  spinning here could deadlock shutdown — see the code comment); the
  DB-facing fills queue drops immediately on overflow (analytics, not
  correctness-critical).
- **SPSC ring buffer, not a mutex-guarded queue**, everywhere two threads
  hand off a stream: `head_`/`tail_` (or per-consumer cursors) on
  separate cache lines to avoid false sharing.

## Optimization roadmap (the stretch goals)

This is deliberately the *correct, readable* version first. In rough
order of effort-to-payoff:

1. **Array-indexed price levels.** Replace `std::map<Price, ...>` with a
   flat array indexed directly by `price - min_price`. Turns level
   lookup from O(log n) into O(1) and kills the pointer-chasing behind
   the p99.9+ tail above.
2. **Intrusive linked orders instead of `std::deque<Order*>`.** Avoids
   the deque's own internal allocation churn on cancel-heavy workloads.
3. **Pin threads to isolated cores** (`sched_setaffinity`, `isolcpus`
   kernel boot param) and disable turbo/frequency scaling.
4. **Real market data protocol.** An ITCH 5.0 or FIX/binary decoder
   feeding the sequencer as a `ProducerId::FeedReplay` producer — the
   structural slot already exists, nothing feeds it yet.
5. **Kernel-bypass networking** (`io_uring` first — biggest win for the
   least exotic hardware requirement; DPDK/RDMA are worth being able to
   discuss, not worth building for this project).
6. **Snapshotting + WAL replay on startup**, closing out roadmap item 1.3
   — the WAL already has everything needed.
