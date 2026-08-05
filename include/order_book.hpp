#pragma once
#include <algorithm>
#include <deque>
#include <functional>
#include <map>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "object_pool.hpp"
#include "types.hpp"

// Price-time priority limit order book with matching built into
// add_limit_order(). This is the correctness-first version: std::map
// price levels (O(log n) touch per level) and an ObjectPool for order
// storage, so orders themselves cost no malloc/free. Swapping the price
// levels for a flat, array-indexed structure (ticks as direct indices)
// is the natural next optimization step for shaving the O(log n) off —
// see README.
template <size_t PoolCapacity = 1 << 16>
class OrderBook {
public:
    // Adds a limit order, matching against the opposite side first
    // (price-time priority). Any unfilled remainder rests in the book.
    // Appends resulting trades to `trades_out` (does not clear it first).
    void add_limit_order(OrderId id, Side side, Price price, Qty qty, Nanos ts_ns,
                          std::vector<Trade>& trades_out) {
        if (side == Side::Buy) {
            match_against(asks_, id, side, price, qty, ts_ns, trades_out,
                          [](Price book_px, Price order_px) { return book_px <= order_px; });
            if (qty > 0) rest(bids_, id, side, price, qty, ts_ns);
        } else {
            match_against(bids_, id, side, price, qty, ts_ns, trades_out,
                          [](Price book_px, Price order_px) { return book_px >= order_px; });
            if (qty > 0) rest(asks_, id, side, price, qty, ts_ns);
        }
    }

    // Cancels a resting order by id. Returns true if found and removed.
    bool cancel(OrderId id) {
        auto it = locate_.find(id);
        if (it == locate_.end()) return false;
        auto [side, price] = it->second;
        bool removed = side == Side::Buy ? remove_from(bids_, price, id)
                                          : remove_from(asks_, price, id);
        if (removed) locate_.erase(it);
        return removed;
    }

    bool best_bid(Price& price, Qty& qty) const { return best_of(bids_, price, qty); }
    bool best_ask(Price& price, Qty& qty) const { return best_of(asks_, price, qty); }

    size_t bid_levels() const { return bids_.size(); }
    size_t ask_levels() const { return asks_.size(); }
    size_t resting_orders() const { return locate_.size(); }

    void print_top(std::ostream& os, int depth = 5) const {
        os << "  asks (best first):\n";
        int shown = 0;
        for (auto it = asks_.begin(); it != asks_.end() && shown < depth; ++it, ++shown) {
            Qty q = 0;
            for (auto* o : it->second) q += o->qty;
            os << "    " << it->first << " x " << q << "\n";
        }
        os << "  bids (best first):\n";
        shown = 0;
        for (auto it = bids_.begin(); it != bids_.end() && shown < depth; ++it, ++shown) {
            Qty q = 0;
            for (auto* o : it->second) q += o->qty;
            os << "    " << it->first << " x " << q << "\n";
        }
    }

private:
    template <typename Cmp>
    using LevelMap = std::map<Price, std::deque<Order*>, Cmp>;
    LevelMap<std::greater<Price>> bids_;  // best bid = highest price = begin()
    LevelMap<std::less<Price>>    asks_;  // best ask = lowest price  = begin()

    ObjectPool<Order, PoolCapacity> pool_;
    std::unordered_map<OrderId, std::pair<Side, Price>> locate_;

    template <typename Book, typename Crosses>
    void match_against(Book& opposite, OrderId id, Side side, Price price, Qty& qty,
                        Nanos ts_ns, std::vector<Trade>& trades_out, Crosses crosses) {
        while (qty > 0 && !opposite.empty()) {
            auto level_it = opposite.begin();
            if (!crosses(level_it->first, price)) break; // no more crossing prices
            auto& dq = level_it->second;
            while (qty > 0 && !dq.empty()) {
                Order* resting = dq.front();
                Qty fill = std::min(qty, resting->qty);
                trades_out.push_back(Trade{side == Side::Buy ? id : resting->id,
                                            side == Side::Buy ? resting->id : id,
                                            level_it->first, fill, ts_ns});
                qty -= fill;
                resting->qty -= fill;
                if (resting->qty == 0) {
                    locate_.erase(resting->id);
                    pool_.release(resting);
                    dq.pop_front();
                }
            }
            if (dq.empty()) opposite.erase(level_it);
        }
    }

    template <typename Book>
    void rest(Book& book, OrderId id, Side side, Price price, Qty qty, Nanos ts_ns) {
        Order* order = pool_.acquire(Order{id, side, price, qty, ts_ns});
        if (!order) return; // pool exhausted — drop rather than crash; see README
        book[price].push_back(order);
        locate_[id] = {side, price};
    }

    template <typename Book>
    bool remove_from(Book& book, Price price, OrderId id) {
        auto lvl = book.find(price);
        if (lvl == book.end()) return false;
        auto& dq = lvl->second;
        for (auto it = dq.begin(); it != dq.end(); ++it) {
            if ((*it)->id == id) {
                pool_.release(*it);
                dq.erase(it);
                if (dq.empty()) book.erase(lvl);
                return true;
            }
        }
        return false;
    }

    template <typename Book>
    bool best_of(const Book& book, Price& price, Qty& qty) const {
        if (book.empty()) return false;
        auto it = book.begin();
        price = it->first;
        qty = 0;
        for (auto* o : it->second) qty += o->qty;
        return true;
    }
};
