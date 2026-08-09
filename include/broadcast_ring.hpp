#pragma once
#include <atomic>
#include <array>
#include <cstdint>
#include <cstddef>
#include <type_traits>

// One writer (the sequencer), N readers (matcher, WAL, drop copy, UI bridge).
// Readers never block each other and never block the writer, except by
// falling so far behind they'd be lapped — which is a real condition the
// writer must detect, not silently overwrite.
template <typename T, size_t Capacity, size_t MaxConsumers = 8>
class BroadcastRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>);

public:
    size_t register_consumer() {
        return consumer_count_.fetch_add(1, std::memory_order_acq_rel);
    }

    bool try_publish(const T& item) {
        const uint64_t write = write_cursor_.load(std::memory_order_relaxed);

        // Gating: only rescan consumer cursors when the cached minimum
        // says we'd lap someone. Rescanning every publish would touch
        // MaxConsumers cache lines on the hot path.
        if (write - cached_min_.load(std::memory_order_relaxed) >= Capacity) {
            const uint64_t fresh_min = scan_min_consumer_cursor();
            cached_min_.store(fresh_min, std::memory_order_relaxed);
            if (write - fresh_min >= Capacity) return false;   // slowest consumer is lapping-distance behind
        }

        slots_[write & kMask] = item;
        write_cursor_.store(write + 1, std::memory_order_release);
        return true;
    }

    bool try_consume(size_t consumer_id, T& out) {
        auto& cursor = cursors_[consumer_id].value;
        const uint64_t read = cursor.load(std::memory_order_relaxed);
        if (read >= write_cursor_.load(std::memory_order_acquire)) return false;

        out = slots_[read & kMask];
        cursor.store(read + 1, std::memory_order_release);
        return true;
    }

    uint64_t published_count() const { return write_cursor_.load(std::memory_order_acquire); }

private:
    uint64_t scan_min_consumer_cursor() const {
        const size_t n = consumer_count_.load(std::memory_order_acquire);
        uint64_t min = UINT64_MAX;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t c = cursors_[i].value.load(std::memory_order_acquire);
            if (c < min) min = c;
        }
        return (n == 0) ? write_cursor_.load(std::memory_order_relaxed) : min;
    }

    static constexpr uint64_t kMask = Capacity - 1;

    // Each cursor on its own cache line — this is the false-sharing fix
    // flagged earlier, applied at design time instead of retrofitted.
    struct alignas(64) Cursor { std::atomic<uint64_t> value{0}; };

    alignas(64) std::atomic<uint64_t> write_cursor_{0};
    alignas(64) std::atomic<uint64_t> cached_min_{0};
    alignas(64) std::atomic<size_t>   consumer_count_{0};
    std::array<Cursor, MaxConsumers>  cursors_;
    std::array<T, Capacity>           slots_;
};