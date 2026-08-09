#include <cassert>
#include <cstdint>
#include <iostream>
#include "broadcast_ring.hpp"

using Ring = BroadcastRing<uint64_t, 8>;

void test_single_consumer_basic_pub_sub() {
    Ring ring;
    size_t c = ring.register_consumer();
    assert(c == 0);

    for (uint64_t i = 0; i < 5; ++i) assert(ring.try_publish(i));

    uint64_t out;
    for (uint64_t i = 0; i < 5; ++i) {
        assert(ring.try_consume(c, out));
        assert(out == i);
    }
    assert(!ring.try_consume(c, out));   // drained
    std::cout << "test_single_consumer_basic_pub_sub passed\n";
}

void test_multi_consumer_independent_cursors() {
    Ring ring;
    size_t a = ring.register_consumer();
    size_t b = ring.register_consumer();

    for (uint64_t i = 0; i < 4; ++i) assert(ring.try_publish(i));

    uint64_t out;
    for (uint64_t i = 0; i < 4; ++i) {
        assert(ring.try_consume(a, out));
        assert(out == i);
    }
    assert(!ring.try_consume(a, out));

    // b hasn't consumed anything yet — must still be able to read from the start.
    for (uint64_t i = 0; i < 4; ++i) {
        assert(ring.try_consume(b, out));
        assert(out == i);
    }
    std::cout << "test_multi_consumer_independent_cursors passed\n";
}

void test_lapping_detection_blocks_publish() {
    Ring ring;   // Capacity = 8
    size_t fast = ring.register_consumer();
    size_t slow = ring.register_consumer();

    // Neither consumer's state affects whether these 8 initial publishes
    // succeed (the gate is lazy — it only rescans once the writer would
    // lap the cached minimum, and 8 - 0 first trips exactly at count 8).
    uint64_t out;
    for (uint64_t i = 0; i < 8; ++i) assert(ring.try_publish(i));

    // Drain fully via the fast consumer; slow never consumes at all.
    for (int i = 0; i < 8; ++i) assert(ring.try_consume(fast, out));

    // slow's cursor is the bottleneck now (still at 0) — publish blocks
    // even though the ring is otherwise fully drained by fast.
    assert(!ring.try_publish(999));

    // Advance the slow consumer partway; publishing should recover by
    // exactly that much headroom, then block again.
    for (int i = 0; i < 3; ++i) assert(ring.try_consume(slow, out));
    assert(ring.try_publish(100));
    assert(ring.try_publish(101));
    assert(ring.try_publish(102));
    assert(!ring.try_publish(103));   // lapping-distance behind again
    std::cout << "test_lapping_detection_blocks_publish passed\n";
}

void test_register_consumer_ids_monotonic() {
    Ring ring;
    assert(ring.register_consumer() == 0);
    assert(ring.register_consumer() == 1);
    assert(ring.register_consumer() == 2);
    std::cout << "test_register_consumer_ids_monotonic passed\n";
}

void test_late_registration_can_see_stale_data() {
    // Documents the startup-ordering hazard: a consumer that registers
    // after the ring has already wrapped can read overwritten data
    // without try_consume ever signaling anything is wrong. This is why
    // gateway_main.cpp registers every consumer before starting the
    // Sequencer thread.
    Ring ring;   // Capacity = 8
    size_t a = ring.register_consumer();

    // Publish past one full wrap, draining via `a` as we go so try_publish
    // never blocks on lapping.
    uint64_t out;
    for (uint64_t i = 0; i < 9; ++i) {
        assert(ring.try_publish(i));
        assert(ring.try_consume(a, out));
        assert(out == i);
    }
    // write_cursor_ is now 9; slot 0 has been overwritten by item 8 (9 & mask...
    // 8 & 7 == 0, so item index 8 landed in slot 0, clobbering item 0).

    size_t b = ring.register_consumer();   // registers late, cursor starts at 0
    assert(ring.try_consume(b, out));      // "succeeds" — but reads stale data
    assert(out != 0);                      // NOT the original first item
    assert(out == 8);                      // it's whatever last overwrote slot 0
    std::cout << "test_late_registration_can_see_stale_data passed\n";
}

int main() {
    test_single_consumer_basic_pub_sub();
    test_multi_consumer_independent_cursors();
    test_lapping_detection_blocks_publish();
    test_register_consumer_ids_monotonic();
    test_late_registration_can_see_stale_data();
    std::cout << "All broadcast_ring tests passed.\n";
    return 0;
}
