#pragma once
#include <cstdint>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdio>

// Log-linear histogram for nanosecond latency samples.
//
// Values are bucketed by octave (highest set bit), and each octave is
// subdivided into `sub_bucket_count` linear slots. This gives roughly
// constant *relative* precision (~1 / sub_bucket_count) at any scale,
// from nanoseconds to seconds, using fixed, bounded memory — the same
// core idea HdrHistogram uses, simplified for a single-threaded harness.
class LatencyHistogram {
public:
    explicit LatencyHistogram(uint64_t max_value_ns = 1ull << 34, // ~17s ceiling
                               uint32_t sub_bucket_bits = 7)       // 128 slots/octave
        : sub_bucket_bits_(sub_bucket_bits),
          sub_bucket_count_(1u << sub_bucket_bits),
          counts_(static_cast<size_t>(highest_bit(max_value_ns) + 2) * sub_bucket_count_, 0) {}

    void record(uint64_t value_ns) {
        total_count_++;
        sum_ns_ += value_ns;
        min_ns_ = std::min(min_ns_, value_ns);
        max_ns_ = std::max(max_ns_, value_ns);
        counts_[index_for(value_ns)]++;
    }

    uint64_t count() const { return total_count_; }
    uint64_t min() const { return total_count_ ? min_ns_ : 0; }
    uint64_t max() const { return max_ns_; }
    double mean() const { return total_count_ ? double(sum_ns_) / total_count_ : 0.0; }

    // p in (0, 100]
    uint64_t percentile(double p) const {
        if (total_count_ == 0) return 0;
        uint64_t target = std::max<uint64_t>(
            1, static_cast<uint64_t>(std::ceil(p / 100.0 * total_count_)));
        uint64_t running = 0;
        for (size_t i = 0; i < counts_.size(); ++i) {
            running += counts_[i];
            if (running >= target) return value_for_index(i);
        }
        return max_ns_;
    }

    void print_summary(const char* label = "latency") const {
        std::printf("=== %s (n=%llu) ===\n", label, (unsigned long long)total_count_);
        std::printf("  min    : %10llu ns\n", (unsigned long long)min());
        std::printf("  mean   : %13.1f ns\n", mean());
        std::printf("  p50    : %10llu ns\n", (unsigned long long)percentile(50));
        std::printf("  p90    : %10llu ns\n", (unsigned long long)percentile(90));
        std::printf("  p99    : %10llu ns\n", (unsigned long long)percentile(99));
        std::printf("  p99.9  : %10llu ns\n", (unsigned long long)percentile(99.9));
        std::printf("  p99.99 : %10llu ns\n", (unsigned long long)percentile(99.99));
        std::printf("  max    : %10llu ns\n", (unsigned long long)max());
    }

private:
    uint32_t sub_bucket_bits_;
    uint32_t sub_bucket_count_;
    std::vector<uint64_t> counts_;

    uint64_t total_count_ = 0;
    uint64_t sum_ns_ = 0;
    uint64_t min_ns_ = ~0ull;
    uint64_t max_ns_ = 0;

    static uint32_t highest_bit(uint64_t v) {
        uint32_t b = 0;
        while (v >>= 1) ++b;
        return b;
    }

    size_t index_for(uint64_t v) const {
        if (v == 0) return 0;
        uint32_t octave = highest_bit(v);
        uint64_t base = 1ull << octave;
        uint64_t offset = v - base;
        // For octave >= sub_bucket_bits_, compress `offset` into a slot.
        // For smaller octaves, 2^octave < sub_bucket_count_, so each raw
        // offset gets its own slot (width 1) — must match value_for_index.
        uint32_t sub = static_cast<uint32_t>(
            octave >= sub_bucket_bits_
                ? offset >> (octave - sub_bucket_bits_)
                : offset);
        sub = std::min(sub, sub_bucket_count_ - 1);
        size_t idx = static_cast<size_t>(octave) * sub_bucket_count_ + sub;
        return std::min(idx, counts_.size() - 1);
    }

    uint64_t value_for_index(size_t idx) const {
        uint32_t octave = static_cast<uint32_t>(idx / sub_bucket_count_);
        uint32_t sub = static_cast<uint32_t>(idx % sub_bucket_count_);
        uint64_t base = 1ull << octave;
        uint64_t width = octave >= sub_bucket_bits_ ? (1ull << (octave - sub_bucket_bits_)) : 1;
        return base + static_cast<uint64_t>(sub) * width;
    }
};
