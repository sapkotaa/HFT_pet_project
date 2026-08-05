#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>

// Fixed-capacity object pool with an intrusive free list.
// acquire()/release() are O(1) and never call malloc/new after the pool
// itself is constructed — this is what keeps the matching engine's hot
// path free of dynamic allocation.
template <typename T, size_t Capacity>
class ObjectPool {
public:
    ObjectPool() {
        for (size_t i = 0; i + 1 < Capacity; ++i) next_free_[i] = static_cast<int32_t>(i + 1);
        if constexpr (Capacity > 0) next_free_[Capacity - 1] = -1;
        free_head_ = Capacity > 0 ? 0 : -1;
    }

    template <typename... Args>
    T* acquire(Args&&... args) {
        if (free_head_ == -1) return nullptr; // pool exhausted — caller must handle
        int32_t idx = free_head_;
        free_head_ = next_free_[idx];
        T* obj = reinterpret_cast<T*>(&storage_[idx]);
        return new (obj) T(std::forward<Args>(args)...);
    }

    void release(T* obj) {
        auto* byte_ptr = reinterpret_cast<unsigned char*>(obj);
        auto* base_ptr = reinterpret_cast<unsigned char*>(storage_.data());
        int32_t idx = static_cast<int32_t>((byte_ptr - base_ptr) / sizeof(Slot));
        obj->~T();
        next_free_[idx] = free_head_;
        free_head_ = idx;
    }

    size_t capacity() const { return Capacity; }

private:
    struct alignas(alignof(T)) Slot { unsigned char bytes[sizeof(T)]; };
    std::array<Slot, Capacity> storage_;
    std::array<int32_t, Capacity> next_free_{};
    int32_t free_head_ = -1;
};
