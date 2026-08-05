# hft_lob

A limit order book, matching engine, market data replay system, and a toy
market-making strategy — wired together end-to-end, with a latency
histogram measuring every order's time through the engine. Runs out of
the box with a synthetic feed, or replay a CSV of real order flow.

## What's actually in here

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

## Build

```
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

(Or directly: `g++ -std=c++20 -O3 -march=native -Iinclude src/main.cpp -o hft_lob -pthread`)

## Run

```
./build/hft_lob                          # 200k synthetic events
./build/hft_lob --count 1000000          # 1M synthetic events
./build/hft_lob --data data/sample_feed.csv   # replay real-ish sample data
./build/hft_lob --test                   # matching-engine correctness checks
```

## Sample output (300k synthetic events, -O3 -march=native)

```
Processed 300000 orders, generated 265943 trades
Resting orders remaining: 25521 (548 bid levels, 567 ask levels)

Final book snapshot:
  asks (best first):
    11387 x 341
    ...
  bids (best first):
    11378 x 10
    ...

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

That's the number that goes on a resume:
*"Built a limit order book and matching engine in C++20 with a lock-free
SPSC feed pipeline, achieving P99 tick-to-book latency of ~670ns and P50
of ~126ns on synthetic order flow."*

The tail (p99.9 and up) is dominated by std::map's O(log n) price-level
lookup occasionally hitting a cache miss, plus OS scheduling noise from
running two threads on a shared, non-isolated core — see Optimization
Roadmap below for what actually fixes that.

## Design decisions worth defending in an interview

- **Integer prices, never floats.** Prices are ticks (`int64_t`), not
  dollars-as-double. Floating point comparison and rounding in a matching
  engine is a correctness bug waiting to happen.
- **Object pool, not malloc per order.** `ObjectPool<T, Capacity>` is a
  fixed array with an intrusive free list — acquire/release are O(1) and
  touch no allocator after startup.
- **std::map for price levels, on purpose (for now).** This is the
  correctness-first version. It's O(log n) per touch, not O(1) — that's
  the known, intentional trade-off documented below, not an oversight.
- **Pool exhaustion drops the order, doesn't crash.** `rest()` checks for
  a null pointer from the pool and returns early rather than dereferencing
  it. In a real system you'd alert on this, not silently drop — but it
  won't segfault your demo.
- **SPSC ring buffer, not a mutex-guarded queue.** Single producer, single
  consumer, `head_`/`tail_` on separate cache lines to avoid false
  sharing. This is the standard building block for feed handler → engine
  handoff in real systems.

## Optimization roadmap (the stretch goals)

This is deliberately the *correct, readable* version first. The
resume-differentiating work is measuring it, then cutting the tail down —
in rough order of effort-to-payoff:

1. **Array-indexed price levels.** Replace `std::map<Price, ...>` with a
   flat array indexed directly by `price - min_price` (or a small
   open-addressed hash for sparse books). Turns level lookup from
   O(log n) into O(1) and kills the pointer-chasing that's causing the
   p99.9+ tail above.
2. **Intrusive linked orders instead of `std::deque<Order*>`.** Avoids
   the deque's own internal allocation churn on cancel-heavy workloads.
3. **Pin threads to isolated cores** (`sched_setaffinity`, `isolcpus`
   kernel boot param) and disable turbo/frequency scaling. Right now the
   two threads compete for scheduler time on whatever cores are free —
   that's most of what's in your p99.99 and max.
4. **Real market data protocol.** Swap `MarketDataFeed::load_csv` for an
   ITCH 5.0 or FIX/binary decoder reading from a UDP multicast socket —
   NASDAQ publishes sample ITCH data for this.
5. **Kernel-bypass networking** (DPDK or `io_uring`) if you want the feed
   handler itself to stop being the bottleneck once the engine is fast.

Build one of these, rerun `--count 1000000`, and put the before/after
p99.9 numbers side by side in your writeup — that comparison is worth
more on a resume than any single latency number alone.
