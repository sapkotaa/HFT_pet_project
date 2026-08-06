#include <cassert>
#include <cstring>
#include <iostream>
#include "frame_reader.hpp"

// Helper: build a raw Logon frame as bytes
std::vector<uint8_t> make_logon_frame(uint32_t seq_num, uint64_t client_id) {
    LogonMsg body{client_id, /*heartbeat_ms=*/1000, /*starting_seq=*/1};
    MsgHeader header{sizeof(LogonMsg), MsgType::Logon, seq_num};

    std::vector<uint8_t> frame(sizeof(MsgHeader) + sizeof(LogonMsg));
    std::memcpy(frame.data(), &header, sizeof(MsgHeader));
    std::memcpy(frame.data() + sizeof(MsgHeader), &body, sizeof(LogonMsg));
    return frame;
}

void test_single_complete_frame() {
    FrameReader reader;
    auto frame = make_logon_frame(1, 42);
    reader.feed(frame.data(), frame.size());

    auto result = reader.next_frame();
    assert(result.has_value());
    assert(result->size() == frame.size());
    assert(reader.next_frame() == std::nullopt);   // nothing left
    std::cout << "test_single_complete_frame passed\n";
}

void test_frame_split_across_feeds() {
    FrameReader reader;
    auto frame = make_logon_frame(1, 42);

    // Feed it 1 byte, 2 bytes, then the rest — deliberately awkward splits
    reader.feed(frame.data(), 1);
    assert(reader.next_frame() == std::nullopt);   // not even a full header

    reader.feed(frame.data() + 1, 2);
    assert(reader.next_frame() == std::nullopt);   // header done, body not

    reader.feed(frame.data() + 3, frame.size() - 3);
    auto result = reader.next_frame();
    assert(result.has_value());
    assert(result->size() == frame.size());
    std::cout << "test_frame_split_across_feeds passed\n";
}

void test_multiple_frames_in_one_feed() {
    FrameReader reader;
    auto f1 = make_logon_frame(1, 42);
    auto f2 = make_logon_frame(2, 43);

    std::vector<uint8_t> combined = f1;
    combined.insert(combined.end(), f2.begin(), f2.end());
    reader.feed(combined.data(), combined.size());

    auto r1 = reader.next_frame();
    auto r2 = reader.next_frame();
    assert(r1.has_value() && r1->size() == f1.size());
    assert(r2.has_value() && r2->size() == f2.size());
    assert(reader.next_frame() == std::nullopt);
    std::cout << "test_multiple_frames_in_one_feed passed\n";
}

void test_partial_frame_plus_next_frame_split() {
    // The nastiest realistic case: one feed() ends mid-header of the
    // *second* message, after a complete first message.
    FrameReader reader;
    auto f1 = make_logon_frame(1, 42);
    auto f2 = make_logon_frame(2, 43);

    std::vector<uint8_t> combined = f1;
    combined.insert(combined.end(), f2.begin(), f2.begin() + 3);  // f2's header, partial
    reader.feed(combined.data(), combined.size());

    auto r1 = reader.next_frame();
    assert(r1.has_value() && r1->size() == f1.size());
    assert(reader.next_frame() == std::nullopt);   // f2 still incomplete

    reader.feed(f2.data() + 3, f2.size() - 3);
    auto r2 = reader.next_frame();
    assert(r2.has_value() && r2->size() == f2.size());
    std::cout << "test_partial_frame_plus_next_frame_split passed\n";
}

void test_empty_feed() {
    FrameReader reader;
    reader.feed(nullptr, 0);   // shouldn't crash
    assert(reader.next_frame() == std::nullopt);
    std::cout << "test_empty_feed passed\n";
}
#include <cstdlib>
#include <ctime>

void test_fuzz_random_chunking() {
    srand(42);   // fixed seed — reproducible failures, not flaky ones

    constexpr int kNumFrames = 200;
    FrameReader reader;

    // Build N frames and concatenate them into one big byte stream
    std::vector<std::vector<uint8_t>> original_frames;
    std::vector<uint8_t> stream;
    for (int i = 0; i < kNumFrames; ++i) {
        auto frame = make_logon_frame(i, 1000 + i);
        original_frames.push_back(frame);
        stream.insert(stream.end(), frame.begin(), frame.end());
    }

    // Feed it back in random-sized chunks — simulates however TCP
    // actually decides to hand bytes back on a given day
    size_t offset = 0;
    std::vector<std::vector<uint8_t>> received_frames;
    while (offset < stream.size()) {
        size_t chunk_size = (rand() % 7) + 1;   // 1-7 bytes at a time
        chunk_size = std::min(chunk_size, stream.size() - offset);

        reader.feed(stream.data() + offset, chunk_size);
        offset += chunk_size;

        while (auto frame = reader.next_frame()) {
            received_frames.push_back(*frame);
        }
    }

    assert(received_frames.size() == original_frames.size());
    for (size_t i = 0; i < original_frames.size(); ++i) {
        assert(received_frames[i] == original_frames[i]);
    }
    std::cout << "test_fuzz_random_chunking passed (" 
              << kNumFrames << " frames, random 1-7 byte chunks)\n";
}
int main() {
    test_single_complete_frame();
    test_frame_split_across_feeds();
    test_multiple_frames_in_one_feed();
    test_partial_frame_plus_next_frame_split();
    test_empty_feed();
    test_fuzz_random_chunking();
    std::cout << "All frame_reader tests passed.\n";
    return 0;
}