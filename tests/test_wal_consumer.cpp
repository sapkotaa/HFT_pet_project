#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>
#include "event.hpp"
#include "sequencer.hpp"
#include "wal_consumer.hpp"

namespace {

SequencedEvent make_event(uint64_t seq_num) {
    SequencedEvent ev{};
    ev.seq_num = seq_num;
    ev.ingress_ts_ns = seq_num * 10;
    ev.sequence_ts_ns = seq_num * 20;
    ev.type = EventType::NewOrder;
    ev.session_id = 1;
    ev.client_order_id = seq_num * 100;
    ev.side = (seq_num % 2 == 0) ? Side::Buy : Side::Sell;
    ev.price = static_cast<uint32_t>(seq_num) + 1000;
    ev.quantity = static_cast<uint32_t>(seq_num) + 1;
    return ev;
}

}  // namespace

void test_append_round_trips_byte_exact() {
    std::string path = (std::filesystem::temp_directory_path() / "hft_lob_wal_test.bin").string();
    std::filesystem::remove(path);

    auto ring = std::make_unique<OutputRing>();
    WalConsumer wal(*ring, path, /*fsync_every_n=*/2, std::chrono::milliseconds(1000));
    assert(wal.open());   // no background thread — append() driven directly below

    std::vector<SequencedEvent> written;
    for (uint64_t i = 1; i <= 5; ++i) {
        SequencedEvent ev = make_event(i);
        written.push_back(ev);
        wal.append(ev);
    }
    assert(wal.records_written() == 5);
    wal.close();   // forced fsync + close

    std::uintmax_t size = std::filesystem::file_size(path);
    assert(size == 5 * sizeof(SequencedEvent));

    std::FILE* f = std::fopen(path.c_str(), "rb");
    assert(f != nullptr);
    for (uint64_t i = 1; i <= 5; ++i) {
        SequencedEvent readback{};
        size_t n = std::fread(&readback, sizeof(SequencedEvent), 1, f);
        assert(n == 1);
        assert(std::memcmp(&readback, &written[i - 1], sizeof(SequencedEvent)) == 0);
    }
    std::fclose(f);
    std::filesystem::remove(path);
    std::cout << "test_append_round_trips_byte_exact passed\n";
}

int main() {
    test_append_round_trips_byte_exact();
    std::cout << "All wal_consumer tests passed.\n";
    return 0;
}
