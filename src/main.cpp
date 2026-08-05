#include <atomic>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../include/latency_histogram.hpp"
#include "../include/market_data_feed.hpp"
#include "../include/order_book.hpp"
#include "../include/spsc_ring_buffer.hpp"
#include "../include/strategy.hpp"
#include "../include/timer.hpp"
#include "../include/types.hpp"
#include "../include/results.hpp"

namespace {

constexpr size_t kRingCapacity = 1 << 14;  // must be power of two
constexpr size_t kPoolCapacity = 1 << 20;  // headroom above default event counts

// Sanity checks for the matching engine itself — run with --test.
// This is the kind of thing you'd expand into a real test suite
// (googletest / catch2) for the resume-writeup version of this project.
bool self_test() {
    auto book_ptr = std::make_unique<OrderBook<kPoolCapacity>>();
    auto& book = *book_ptr;
    std::vector<Trade> trades;

    book.add_limit_order(1, Side::Buy, 100, 10, 0, trades);
    Price bp;
    Qty bq;
    if (!book.best_bid(bp, bq) || bp != 100 || bq != 10) {
        std::printf("FAIL: resting bid not reflected in best_bid\n");
        return false;
    }

    trades.clear();
    book.add_limit_order(2, Side::Sell, 100, 4, 0, trades);
    if (trades.size() != 1 || trades[0].qty != 4 || trades[0].price != 100) {
        std::printf("FAIL: crossing order did not produce expected trade\n");
        return false;
    }
    book.best_bid(bp, bq);
    if (bq != 6) {
        std::printf("FAIL: remaining qty after partial fill wrong (got %u, want 6)\n", bq);
        return false;
    }

    if (!book.cancel(1)) {
        std::printf("FAIL: cancel of resting order failed\n");
        return false;
    }
    if (book.best_bid(bp, bq)) {
        std::printf("FAIL: bid should be gone after cancelling the only resting order\n");
        return false;
    }

    std::printf("self-test: all checks passed\n");
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::string data_file;
    size_t synthetic_count = 200'000;
    bool run_self_test = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--test") {
            run_self_test = true;
        } else if (arg == "--data" && i + 1 < argc) {
            data_file = argv[++i];
        } else if (arg == "--count" && i + 1 < argc) {
            synthetic_count = std::stoull(argv[++i]);
        } else if (arg == "--help") {
            std::printf(
                "usage: hft_lob [--data feed.csv] [--count N] [--test]\n"
                "  --data FILE   replay a CSV feed (side,price,qty per line)\n"
                "  --count N     number of synthetic events if --data not given (default 200000)\n"
                "  --test        run matching-engine self-tests and exit\n");
            return 0;
        }
    }

    if (run_self_test) return self_test() ? 0 : 1;

    std::vector<Order> feed_events = data_file.empty()
                                          ? MarketDataFeed::generate_synthetic(synthetic_count)
                                          : MarketDataFeed::load_csv(data_file);

    std::printf("Loaded %zu feed events %s\n", feed_events.size(),
                data_file.empty() ? "(synthetic)" : ("from " + data_file).c_str());

    SpscRingBuffer<Order, kRingCapacity> ring;
    std::atomic<bool> feed_done{false};
    TscClock clock;  // one shared calibration for both threads

    // --- Producer thread: market data feed handler ---
    std::thread feed_thread([&] {
        for (auto order : feed_events) {
            order.ts_ns = static_cast<Nanos>(clock.cycles_to_ns(TscClock::rdtsc()));
            while (!ring.push(order)) {
                std::this_thread::yield();  // backpressure: ring buffer full
            }
        }
        feed_done.store(true, std::memory_order_release);
    });

    // --- Consumer thread: matching engine + strategy ---
    // Heap-allocated: OrderBook embeds a ~1M-slot object pool inline
    // (see object_pool.hpp), which is far too large for the stack.
    auto book_ptr = std::make_unique<OrderBook<kPoolCapacity>>();
    auto& book = *book_ptr;
    MarketMaker<kPoolCapacity> mm(/*half_spread=*/3, /*quote_size=*/10, /*requote_threshold=*/2);
    OrderId strategy_id_cursor = 1'000'000'000;

    LatencyHistogram hist;
    std::vector<Trade> trades;
    trades.reserve(64);
    uint64_t processed = 0, trade_count = 0;

    std::thread engine_thread([&] {
        Order order;
        while (true) {
            if (ring.pop(order)) {
                uint64_t t0 = TscClock::rdtsc();
                trades.clear();
                book.add_limit_order(order.id, order.side, order.price, order.qty, order.ts_ns,
                                      trades);
                mm.on_tick(book, strategy_id_cursor, order.ts_ns, trades);
                uint64_t t1 = TscClock::rdtsc();
                hist.record(static_cast<uint64_t>(clock.cycles_to_ns(t1 - t0)));
                processed++;
                trade_count += trades.size();
            } else if (feed_done.load(std::memory_order_acquire) && ring.empty()) {
                break;
            } else {
                std::this_thread::yield();
            }
        }
    });

    feed_thread.join();
    engine_thread.join();

    std::printf("\nProcessed %llu orders, generated %llu trades\n",
                (unsigned long long)processed, (unsigned long long)trade_count);
    std::printf("Resting orders remaining: %zu (%zu bid levels, %zu ask levels)\n\n",
                book.resting_orders(), book.bid_levels(), book.ask_levels());

    std::printf("Final book snapshot:\n");
    book.print_top(std::cout);
    std::cout << "\n";

    hist.print_summary("order-to-book tick latency");
    append_result_csv("results/history.csv",data_file.empty() ?"synthetic" : "csv_replay",processed,hist);
    return 0;
}
