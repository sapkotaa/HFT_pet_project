#pragma once
#include <atomic>
#include <chrono>
#include <thread>
#include "timer.hpp"
#include "event.hpp"
#include "broadcast_ring.hpp"
#include "spsc_ring_buffer.hpp"   // your existing one

using InputQueue  = SpscRingBuffer<SequencedEvent, 1u << 16>;   // adjust to your actual template signature
using OutputRing  = BroadcastRing<SequencedEvent, 1u << 20>;

class Sequencer {
public:
    InputQueue& input_for(ProducerId p) {
        return inputs_[static_cast<size_t>(p)];
    }

    OutputRing& output() { return output_; }



    void start() {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

private:
    void run() {
        while (running_.load(std::memory_order_acquire)) {
            bool did_work = false;

            // Fixed arbitration order, bounded batch per producer per pass:
            // no producer can monopolize the sequencer, and the drain order
            // is deterministic given the same input contents.
            for (size_t p = 0; p < static_cast<size_t>(ProducerId::Count); ++p) {
                constexpr int kMaxBatch = 32;
                for (int i = 0; i < kMaxBatch; ++i) {
                    SequencedEvent ev;
                    if (!inputs_[p].pop(ev)) break;

                    ev.seq_num        = next_seq_++;
                    ev.sequence_ts_ns = now_ns();
                    ev.producer       = static_cast<ProducerId>(p);

                    // Backpressure: if the slowest consumer is lapping-distance
                    // behind, spin rather than drop. Dropping would break the
                    // total order the whole design rests on.
                    while (!output_.try_publish(ev)) {
                        ++publish_stalls_;
                    }
                    did_work = true;
                }
            }

            if (!did_work) cpu_pause();
        }
    }

uint64_t now_ns(){
     return static_cast<uint64_t>(clock_.cycles_to_ns(TscClock::rdtsc()));
    }

    static void cpu_pause() {
    #if defined(__x86_64__)
        __builtin_ia32_pause();
    #elif defined(__aarch64__)
        asm volatile("yield");
    #endif
    }


    std::array<InputQueue, static_cast<size_t>(ProducerId::Count)> inputs_;
    OutputRing        output_;
    uint64_t          next_seq_ = 1;
    uint64_t          publish_stalls_ = 0;
    std::atomic<bool> running_{false};
    std::thread       thread_;
    TscClock clock_;
};