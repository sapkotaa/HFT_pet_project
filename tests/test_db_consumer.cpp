#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sqlite3.h>
#include "db_consumer.hpp"
#include "event.hpp"
#include "outbound_ack.hpp"
#include "sequencer.hpp"

namespace {

SequencedEvent make_new_order(uint64_t seq_num, uint32_t session_id, uint64_t client_order_id) {
    SequencedEvent ev{};
    ev.seq_num = seq_num;
    ev.sequence_ts_ns = seq_num * 10;
    ev.type = EventType::NewOrder;
    ev.session_id = session_id;
    ev.client_order_id = client_order_id;
    ev.side = Side::Buy;
    ev.price = 100;
    ev.quantity = 10;
    return ev;
}

int64_t query_count(sqlite3* db, const char* sql) {
    sqlite3_stmt* stmt;
    assert(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
    assert(sqlite3_step(stmt) == SQLITE_ROW);
    int64_t n = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return n;
}

}  // namespace

void test_orders_and_execution_reports_persist() {
    std::string path = (std::filesystem::temp_directory_path() / "hft_lob_db_test.sqlite").string();
    std::filesystem::remove(path);
    std::filesystem::remove(path + "-wal");
    std::filesystem::remove(path + "-shm");

    auto ring = std::make_unique<OutputRing>();
    auto fills = std::make_unique<AckQueue>();
    DbConsumer db(*ring, *fills, path, /*batch_every_n=*/2, std::chrono::milliseconds(1000));
    assert(db.open());   // no background thread — driven directly below

    db.append_order(make_new_order(1, /*session*/1, /*client*/100));
    db.append_order(make_new_order(2, /*session*/2, /*client*/100));   // same client_order_id, different session

    OutboundAck ack{};
    ack.session_id = 1;
    ack.client_order_id = 100;
    ack.exchange_order_id = 1;
    ack.status = ExecStatus::Accepted;
    ack.fill_price = 0;
    ack.fill_quantity = 0;
    ack.leaves_quantity = 10;
    db.append_execution_report(ack);

    assert(db.orders_written() == 2);
    assert(db.execs_written() == 1);
    db.close();

    sqlite3* verify;
    assert(sqlite3_open_v2(path.c_str(), &verify, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);

    assert(query_count(verify, "SELECT COUNT(*) FROM orders;") == 2);
    assert(query_count(verify, "SELECT COUNT(*) FROM execution_reports;") == 1);

    // Rows for two different sessions that happen to reuse the same
    // client_order_id must both be present and distinguishable by
    // (session_id, client_order_id) — same isolation property the
    // matching engine relies on.
    assert(query_count(verify,
        "SELECT COUNT(*) FROM orders WHERE session_id=1 AND client_order_id=100;") == 1);
    assert(query_count(verify,
        "SELECT COUNT(*) FROM orders WHERE session_id=2 AND client_order_id=100;") == 1);

    sqlite3_stmt* stmt;
    assert(sqlite3_prepare_v2(verify, "SELECT seq_num, price, quantity FROM orders WHERE session_id=1;",
                               -1, &stmt, nullptr) == SQLITE_OK);
    assert(sqlite3_step(stmt) == SQLITE_ROW);
    assert(sqlite3_column_int64(stmt, 0) == 1);
    assert(sqlite3_column_int64(stmt, 1) == 100);
    assert(sqlite3_column_int64(stmt, 2) == 10);
    sqlite3_finalize(stmt);

    sqlite3_close(verify);
    std::filesystem::remove(path);
    std::filesystem::remove(path + "-wal");
    std::filesystem::remove(path + "-shm");
    std::cout << "test_orders_and_execution_reports_persist passed\n";
}

void test_batched_commit_is_visible_after_close() {
    // batch_every_n is set high enough that nothing auto-flushes mid-test
    // — close() must still force a final commit so all rows are visible.
    std::string path = (std::filesystem::temp_directory_path() / "hft_lob_db_test2.sqlite").string();
    std::filesystem::remove(path);
    std::filesystem::remove(path + "-wal");
    std::filesystem::remove(path + "-shm");

    auto ring = std::make_unique<OutputRing>();
    auto fills = std::make_unique<AckQueue>();
    DbConsumer db(*ring, *fills, path, /*batch_every_n=*/1000, std::chrono::milliseconds(60'000));
    assert(db.open());
    for (uint64_t i = 1; i <= 5; ++i) db.append_order(make_new_order(i, 1, i));
    db.close();

    sqlite3* verify;
    assert(sqlite3_open_v2(path.c_str(), &verify, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    assert(query_count(verify, "SELECT COUNT(*) FROM orders;") == 5);
    sqlite3_close(verify);

    std::filesystem::remove(path);
    std::filesystem::remove(path + "-wal");
    std::filesystem::remove(path + "-shm");
    std::cout << "test_batched_commit_is_visible_after_close passed\n";
}

int main() {
    test_orders_and_execution_reports_persist();
    test_batched_commit_is_visible_after_close();
    std::cout << "All db_consumer tests passed.\n";
    return 0;
}
