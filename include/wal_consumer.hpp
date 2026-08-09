#pragma once
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>       // fsync
#include "event.hpp"
#include "sequencer.hpp"  // OutputRing typedef

// Second independent consumer of the sequencer's BroadcastRing. Appends
// every SequencedEvent as a fixed-size binary record (raw memcpy — it's
// already static_assert-trivially-copyable, per event.hpp's own comment
// "straight to the WAL with no serialization step"). Uses FILE*/fwrite
// rather than std::ofstream (the only prior file-I/O precedent in this
// codebase is results.hpp, which doesn't need durability) specifically
// because durability requires fsync(2) on the underlying fd, which
// std::ofstream doesn't expose portably.
class WalConsumer {
public:
    explicit WalConsumer(OutputRing& ring, std::string path,
                          size_t fsync_every_n = 256,
                          std::chrono::milliseconds fsync_interval = std::chrono::milliseconds(50))
        : ring_(ring), consumer_id_(ring.register_consumer()), path_(std::move(path)),
          fsync_every_n_(fsync_every_n), fsync_interval_(fsync_interval) {}

    ~WalConsumer() { if (file_) std::fclose(file_); }   // defensive; stop() should already have closed it

    // Opens the WAL file (creating its parent directory if needed).
    // Split out from start() so tests can drive append() directly on
    // the calling thread, with no background thread ever running —
    // append()/maybe_fsync() are not safe to call concurrently from two
    // threads, so a test must never open() and then also start().
    bool open() {
        std::filesystem::path p(path_);
        if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
        file_ = std::fopen(path_.c_str(), "ab");
        if (!file_) return false;
        last_sync_ = std::chrono::steady_clock::now();
        return true;
    }

    bool start() {
        if (!open()) return false;
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
        return true;
    }

    // Precondition: the Sequencer has already been stop()'d and joined
    // — see MatchingEngineConsumer::stop() for why that ordering matters.
    void stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

    // Flushes, fsyncs, and closes the file. Only safe to call when no
    // background thread is running (either stop() already joined it, or
    // open() — not start() — was used, e.g. in tests).
    void close() {
        if (!file_) return;
        maybe_fsync(/*force=*/true);
        std::fclose(file_);
        file_ = nullptr;
    }

    // Directly callable, no thread required — for tests. Requires
    // open() (not start()) to have been called first.
    void append(const SequencedEvent& ev) {
        std::fwrite(&ev, sizeof(SequencedEvent), 1, file_);
        ++records_written_;
        ++unsynced_count_;
        maybe_fsync(/*force=*/false);
    }

    uint64_t records_written() const { return records_written_; }

private:
    void run() {
        while (running_.load(std::memory_order_acquire)) {
            bool did_work = false;
            SequencedEvent ev;
            for (int i = 0; i < 32 && ring_.try_consume(consumer_id_, ev); ++i) {
                append(ev);
                did_work = true;
            }
            maybe_fsync(/*force=*/false);
            if (!did_work) cpu_pause();
        }
        SequencedEvent ev;
        while (ring_.try_consume(consumer_id_, ev)) append(ev);
        close();
    }

    // Fsync policy: whichever threshold hits first — fsync_every_n_
    // records written, or fsync_interval_ wall time elapsed since the
    // last sync. Deliberate, documented tradeoff (matching the house
    // style, cf. the UDP receive-buffer comment in engine_subscriber.cpp):
    // up to that window's worth of events can be lost on power loss / OS
    // crash before they're durable, since exec-report acks come from the
    // matching engine — an independent consumer with no ordering
    // dependency on the WAL — rather than waiting on this fsync.
    // fsync-per-event would remove that window at a large throughput cost.
    void maybe_fsync(bool force) {
        if (unsynced_count_ == 0 && !force) return;
        if (!force && unsynced_count_ < fsync_every_n_ &&
            std::chrono::steady_clock::now() - last_sync_ < fsync_interval_) return;
        std::fflush(file_);
        ::fsync(fileno(file_));
        unsynced_count_ = 0;
        last_sync_ = std::chrono::steady_clock::now();
    }

    static void cpu_pause() {
    #if defined(__x86_64__)
        __builtin_ia32_pause();
    #elif defined(__aarch64__)
        asm volatile("yield");
    #endif
    }

    OutputRing& ring_;
    size_t      consumer_id_;
    std::string path_;
    std::FILE*  file_ = nullptr;
    size_t      fsync_every_n_;
    std::chrono::milliseconds fsync_interval_;
    size_t      unsynced_count_ = 0;
    std::chrono::steady_clock::time_point last_sync_;
    uint64_t    records_written_ = 0;
    std::atomic<bool> running_{false};
    std::thread thread_;
};
