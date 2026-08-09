#pragma once
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <thread>
#include "event.hpp"
#include "outbound_ack.hpp"
#include "sequencer.hpp"   // OutputRing typedef

// Third independent consumer of the sequencer's BroadcastRing (alongside
// MatchingEngineConsumer and WalConsumer), plus a second, separate feed:
// MatchingEngineConsumer also pushes every ack it produces into a
// dedicated `fills` SpscRingBuffer (distinct from the gateway's AckQueue
// — same struct, different queue instance, single-producer/single-consumer
// on each), so this consumer never contends with the gateway's own drain.
//
// Persists two tables: `orders` (one row per NewOrder/CancelOrder request,
// keyed by seq_num — the raw inbound log, same content as the WAL but
// queryable) and `execution_reports` (one row per ack the matching engine
// produced — Accepted/PartialFill/Filled/Canceled/Rejected — which is
// what lets a `client_order_id`'s full lifecycle be reconstructed with a
// single SQL query).
//
// SQLite is a single-writer database, so inserts are batched inside an
// explicit transaction and committed on the same threshold policy as
// WalConsumer's fsync (N records or T ms, whichever first) — deliberate,
// documented tradeoff: up to that window's worth of rows can be lost on
// crash before they're durable, in exchange for not paying a full commit
// (which fsyncs the WAL-mode journal) per row. WAL journal mode is what
// lets the web bridge query this file concurrently while it's being
// written — a plain rollback-journal SQLite db would block readers
// during a writer's transaction.
class DbConsumer {
public:
    DbConsumer(OutputRing& ring, AckQueue& fills, std::string path,
               size_t batch_every_n = 256,
               std::chrono::milliseconds batch_interval = std::chrono::milliseconds(50))
        : ring_(ring), fills_(fills), consumer_id_(ring.register_consumer()), path_(std::move(path)),
          batch_every_n_(batch_every_n), batch_interval_(batch_interval) {}

    ~DbConsumer() { close(); }

    // Opens the DB, creates the schema, and begins the first batch
    // transaction. Split from start() so tests can drive append_*()
    // directly on the calling thread with no background thread running
    // — same reasoning as WalConsumer::open()/append().
    bool open() {
        std::filesystem::path p(path_);
        if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
        if (sqlite3_open(path_.c_str(), &db_) != SQLITE_OK) { close(); return false; }

        exec("PRAGMA journal_mode=WAL;");
        exec("PRAGMA synchronous=NORMAL;");
        exec(
            "CREATE TABLE IF NOT EXISTS orders ("
            "  seq_num INTEGER PRIMARY KEY,"
            "  ts_ns INTEGER NOT NULL,"
            "  event_type INTEGER NOT NULL,"
            "  session_id INTEGER NOT NULL,"
            "  client_order_id INTEGER NOT NULL,"
            "  side INTEGER NOT NULL,"
            "  price INTEGER NOT NULL,"
            "  quantity INTEGER NOT NULL"
            ");");
        exec(
            "CREATE TABLE IF NOT EXISTS execution_reports ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  ts_ns INTEGER NOT NULL,"
            "  session_id INTEGER NOT NULL,"
            "  client_order_id INTEGER NOT NULL,"
            "  exchange_order_id INTEGER NOT NULL,"
            "  status INTEGER NOT NULL,"
            "  fill_price INTEGER NOT NULL,"
            "  fill_quantity INTEGER NOT NULL,"
            "  leaves_quantity INTEGER NOT NULL"
            ");");
        exec("CREATE INDEX IF NOT EXISTS idx_orders_client ON orders(session_id, client_order_id);");
        exec("CREATE INDEX IF NOT EXISTS idx_exec_client ON execution_reports(session_id, client_order_id);");

        prepare(&insert_order_stmt_,
            "INSERT INTO orders(seq_num, ts_ns, event_type, session_id, client_order_id, side, price, quantity) "
            "VALUES (?,?,?,?,?,?,?,?);");
        prepare(&insert_exec_stmt_,
            "INSERT INTO execution_reports(ts_ns, session_id, client_order_id, exchange_order_id, status, "
            "fill_price, fill_quantity, leaves_quantity) VALUES (?,?,?,?,?,?,?,?);");

        last_flush_ = std::chrono::steady_clock::now();
        exec("BEGIN;");
        in_txn_ = true;
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

    // Directly callable, no thread required — for tests. Requires open()
    // (not start()) to have been called first.
    void append_order(const SequencedEvent& ev) {
        sqlite3_reset(insert_order_stmt_);
        sqlite3_bind_int64(insert_order_stmt_, 1, static_cast<sqlite3_int64>(ev.seq_num));
        sqlite3_bind_int64(insert_order_stmt_, 2, static_cast<sqlite3_int64>(ev.sequence_ts_ns));
        sqlite3_bind_int(insert_order_stmt_, 3, static_cast<int>(ev.type));
        sqlite3_bind_int64(insert_order_stmt_, 4, static_cast<sqlite3_int64>(ev.session_id));
        sqlite3_bind_int64(insert_order_stmt_, 5, static_cast<sqlite3_int64>(ev.client_order_id));
        sqlite3_bind_int(insert_order_stmt_, 6, static_cast<int>(ev.side));
        sqlite3_bind_int64(insert_order_stmt_, 7, static_cast<sqlite3_int64>(ev.price));
        sqlite3_bind_int64(insert_order_stmt_, 8, static_cast<sqlite3_int64>(ev.quantity));
        step_checked(insert_order_stmt_, "insert orders");
        ++orders_written_;
        ++pending_;
        maybe_flush(/*force=*/false);
    }

    void append_execution_report(const OutboundAck& ack) {
        sqlite3_reset(insert_exec_stmt_);
        sqlite3_bind_int64(insert_exec_stmt_, 1, static_cast<sqlite3_int64>(wall_clock_ns()));
        sqlite3_bind_int64(insert_exec_stmt_, 2, static_cast<sqlite3_int64>(ack.session_id));
        sqlite3_bind_int64(insert_exec_stmt_, 3, static_cast<sqlite3_int64>(ack.client_order_id));
        sqlite3_bind_int64(insert_exec_stmt_, 4, static_cast<sqlite3_int64>(ack.exchange_order_id));
        sqlite3_bind_int(insert_exec_stmt_, 5, static_cast<int>(ack.status));
        sqlite3_bind_int64(insert_exec_stmt_, 6, static_cast<sqlite3_int64>(ack.fill_price));
        sqlite3_bind_int64(insert_exec_stmt_, 7, static_cast<sqlite3_int64>(ack.fill_quantity));
        sqlite3_bind_int64(insert_exec_stmt_, 8, static_cast<sqlite3_int64>(ack.leaves_quantity));
        step_checked(insert_exec_stmt_, "insert execution_reports");
        ++execs_written_;
        ++pending_;
        maybe_flush(/*force=*/false);
    }

    uint64_t orders_written() const { return orders_written_; }
    uint64_t execs_written() const { return execs_written_; }

    // Commits any open transaction and closes the DB. Only safe to call
    // when no background thread is running (either stop() already
    // joined it, or open() — not start() — was used, e.g. in tests).
    void close() {
        if (!db_) return;
        maybe_flush(/*force=*/true);
        if (insert_order_stmt_) { sqlite3_finalize(insert_order_stmt_); insert_order_stmt_ = nullptr; }
        if (insert_exec_stmt_) { sqlite3_finalize(insert_exec_stmt_); insert_exec_stmt_ = nullptr; }
        sqlite3_close(db_);
        db_ = nullptr;
    }

private:
    void run() {
        while (running_.load(std::memory_order_acquire)) {
            bool did_work = false;
            SequencedEvent ev;
            for (int i = 0; i < 32 && ring_.try_consume(consumer_id_, ev); ++i) {
                if (ev.type == EventType::NewOrder || ev.type == EventType::CancelOrder) append_order(ev);
                did_work = true;
            }
            OutboundAck ack;
            for (int i = 0; i < 32 && fills_.pop(ack); ++i) {
                append_execution_report(ack);
                did_work = true;
            }
            maybe_flush(/*force=*/false);
            if (!did_work) cpu_pause();
        }
        SequencedEvent ev;
        while (ring_.try_consume(consumer_id_, ev)) {
            if (ev.type == EventType::NewOrder || ev.type == EventType::CancelOrder) append_order(ev);
        }
        OutboundAck ack;
        while (fills_.pop(ack)) append_execution_report(ack);
        close();
    }

    void maybe_flush(bool force) {
        if (pending_ == 0 && !force) return;
        if (!force && pending_ < batch_every_n_ &&
            std::chrono::steady_clock::now() - last_flush_ < batch_interval_) return;
        if (in_txn_) { exec("COMMIT;"); in_txn_ = false; }
        pending_ = 0;
        last_flush_ = std::chrono::steady_clock::now();
        if (!force) { exec("BEGIN;"); in_txn_ = true; }
    }

    void step_checked(sqlite3_stmt* stmt, const char* what) {
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            std::fprintf(stderr, "DbConsumer: %s failed: %s\n", what, sqlite3_errmsg(db_));
        }
    }

    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown sqlite error";
            sqlite3_free(err);
            throw std::runtime_error("DbConsumer: sqlite exec failed: " + msg + " (" + sql + ")");
        }
    }

    void prepare(sqlite3_stmt** stmt, const char* sql) {
        if (sqlite3_prepare_v2(db_, sql, -1, stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("DbConsumer: sqlite prepare failed: ") + sqlite3_errmsg(db_));
        }
    }

    static uint64_t wall_clock_ns() {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }

    static void cpu_pause() {
    #if defined(__x86_64__)
        __builtin_ia32_pause();
    #elif defined(__aarch64__)
        asm volatile("yield");
    #endif
    }

    OutputRing& ring_;
    AckQueue&   fills_;
    size_t      consumer_id_;
    std::string path_;
    size_t      batch_every_n_;
    std::chrono::milliseconds batch_interval_;

    sqlite3*      db_ = nullptr;
    sqlite3_stmt* insert_order_stmt_ = nullptr;
    sqlite3_stmt* insert_exec_stmt_ = nullptr;
    bool        in_txn_ = false;
    size_t      pending_ = 0;
    std::chrono::steady_clock::time_point last_flush_;
    uint64_t    orders_written_ = 0;
    uint64_t    execs_written_ = 0;
    std::atomic<bool> running_{false};
    std::thread thread_;
};
