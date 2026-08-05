#pragma once
#include <cstdint>
#include <chrono>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#define LH_HAS_RDTSC 1
#else
#define LH_HAS_RDTSC 0
#endif

// Wraps the CPU's timestamp counter (RDTSC) and calibrates it against
// std::chrono::steady_clock once at startup, so raw cycle deltas can be
// converted to nanoseconds without paying a syscall per measurement
// (which is what steady_clock::now() costs on most platforms).
class TscClock {
public:
    TscClock() { calibrate(); }

    static inline uint64_t rdtsc() {
#if LH_HAS_RDTSC
        _mm_lfence();               // serialize so out-of-order execution
        uint64_t t = __rdtsc();     // doesn't leak work across the fence
        _mm_lfence();
        return t;
#else
        return static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
#endif
    }

    inline double cycles_to_ns(uint64_t cycles) const {
        return static_cast<double>(cycles) * ns_per_cycle_;
    }

    double ns_per_cycle() const { return ns_per_cycle_; }

private:
    double ns_per_cycle_ = 1.0;

    void calibrate() {
#if LH_HAS_RDTSC
        using namespace std::chrono;
        constexpr auto calib_duration = milliseconds(100);
        uint64_t start_tsc = rdtsc();
        auto start_wall = steady_clock::now();
        while (steady_clock::now() - start_wall < calib_duration) {
            // spin — deliberately busy, not sleeping, so the core stays
            // at a stable frequency during calibration
        }
        uint64_t end_tsc = rdtsc();
        auto end_wall = steady_clock::now();
        double wall_ns =
            duration_cast<duration<double, std::nano>>(end_wall - start_wall).count();
        ns_per_cycle_ = wall_ns / static_cast<double>(end_tsc - start_tsc);
#else
        ns_per_cycle_ = 1.0;  // fallback path already returns ns-scale ticks
#endif
    }
};
