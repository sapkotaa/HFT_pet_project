#pragma once
#include <vector>

#include "order_book.hpp"
#include "types.hpp"

// A minimal market-making strategy: keeps a two-sided quote centered on
// the book's mid price, requoting only when the mid moves past a
// threshold (to avoid pointless cancel/replace churn). This exists to
// close the loop end-to-end — data in, book updates, strategy reacts,
// new orders go back into the book. Swap in real alpha for anything
// more serious.
template <size_t PoolCapacity>
class MarketMaker {
public:
    MarketMaker(Price half_spread, Qty quote_size, Price requote_threshold)
        : half_spread_(half_spread), quote_size_(quote_size),
          requote_threshold_(requote_threshold) {}

    void on_tick(OrderBook<PoolCapacity>& book, OrderId& next_id, Nanos ts_ns,
                 std::vector<Trade>& trades_out) {
        Price bid_px, ask_px;
        Qty bid_qty, ask_qty;
        if (!book.best_bid(bid_px, bid_qty) || !book.best_ask(ask_px, ask_qty)) return;

        Price mid = (bid_px + ask_px) / 2;
        Price delta = mid - last_mid_;
        if (delta < 0) delta = -delta;
        if (quoting_ && delta < requote_threshold_) return;

        if (quoting_) {
            book.cancel(my_bid_id_);
            book.cancel(my_ask_id_);
        }
        my_bid_id_ = next_id++;
        my_ask_id_ = next_id++;
        book.add_limit_order(my_bid_id_, Side::Buy, mid - half_spread_, quote_size_, ts_ns,
                              trades_out);
        book.add_limit_order(my_ask_id_, Side::Sell, mid + half_spread_, quote_size_, ts_ns,
                              trades_out);
        last_mid_ = mid;
        quoting_ = true;
    }

private:
    Price half_spread_;
    Qty quote_size_;
    Price requote_threshold_;
    bool quoting_ = false;
    Price last_mid_ = 0;
    OrderId my_bid_id_ = 0, my_ask_id_ = 0;
};
