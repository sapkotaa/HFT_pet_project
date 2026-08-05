#pragma once
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "types.hpp"

// Produces a sequence of order-add events, either by reading a CSV replay
// file (side,price,qty) or by generating a synthetic feed (a random walk
// on the mid price) so the whole project runs out of the box with no
// external data file required.
class MarketDataFeed {
public:
    // CSV format: side,price,qty   (one order per line; header row optional)
    // 'side' is B or S. Order ids and timestamps are assigned at load /
    // send time, not read from the file.
    static std::vector<Order> load_csv(const std::string& path) {
        std::ifstream file(path);
        if (!file) throw std::runtime_error("cannot open feed file: " + path);

        std::vector<Order> events;
        std::string line;
        OrderId next_id = 1;
        while (std::getline(file, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::stringstream ss(line);
            std::string side_str, price_str, qty_str;
            if (!std::getline(ss, side_str, ',')) continue;
            if (side_str == "side") continue; // skip header row
            std::getline(ss, price_str, ',');
            std::getline(ss, qty_str, ',');
            if (price_str.empty() || qty_str.empty()) continue;

            Order o;
            o.id = next_id++;
            o.side = (side_str == "B" || side_str == "b") ? Side::Buy : Side::Sell;
            o.price = std::stoll(price_str);
            o.qty = static_cast<Qty>(std::stoul(qty_str));
            o.ts_ns = 0; // assigned by the replay loop at send time
            events.push_back(o);
        }
        return events;
    }

    // Generates `count` synthetic orders around `start_price` with a
    // simple symmetric random walk on the mid price.
    static std::vector<Order> generate_synthetic(size_t count, Price start_price = 10000,
                                                   Qty max_qty = 20, uint32_t seed = 42) {
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> step_dist(-3, 3);
        std::uniform_int_distribution<int> spread_dist(1, 5);
        std::uniform_int_distribution<Qty> qty_dist(1, max_qty);
        std::bernoulli_distribution side_dist(0.5);

        std::vector<Order> events;
        events.reserve(count);
        Price mid = start_price;
        OrderId next_id = 1;
        for (size_t i = 0; i < count; ++i) {
            mid += step_dist(rng);
            bool buy = side_dist(rng);
            Price px = buy ? mid - spread_dist(rng) : mid + spread_dist(rng);
            events.push_back(Order{next_id++, buy ? Side::Buy : Side::Sell, px,
                                    qty_dist(rng), 0});
        }
        return events;
    }
};
