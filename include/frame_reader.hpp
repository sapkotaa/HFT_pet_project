#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <optional>
#include "gateway_protocol.hpp"

// Turns a raw byte stream into discrete, complete messages.
// No sockets, no I/O — just a buffer and a state machine, so it's
// unit-testable by calling feed() with arbitrary byte chunks.
class FrameReader {
public:
    void feed(const uint8_t* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
    }

    // Returns one complete frame (header + body) if available.
    // Call in a loop until nullopt — a single feed() can contain
    // multiple complete messages, or a partial one, or both.
    std::optional<std::vector<uint8_t>> next_frame() {
        if (buffer_.size() < sizeof(MsgHeader)) {
            return std::nullopt;   // haven't even got a full header yet
        }

        MsgHeader header;
        // memcpy, not reinterpret_cast: buffer_.data() has no alignment
        // guarantee, and casting a misaligned pointer to a packed struct
        // pointer is UB even though it "usually works" on x86.
        std::memcpy(&header, buffer_.data(), sizeof(MsgHeader));

        const size_t total_len = sizeof(MsgHeader) + header.body_length;
        if (buffer_.size() < total_len) {
            return std::nullopt;   // header's here, body isn't fully in yet
        }

        std::vector<uint8_t> frame(buffer_.begin(), buffer_.begin() + total_len);
        buffer_.erase(buffer_.begin(), buffer_.begin() + total_len);
        return frame;
    }

private:
    std::vector<uint8_t> buffer_;
};