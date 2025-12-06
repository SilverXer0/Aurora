#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <vector>

namespace aurora {

template<typename T>
class RingBuffer {
public:
    explicit RingBuffer(std::size_t capacity)
        : capacity_(normalize_capacity(capacity)),
          mask_(capacity_ - 1),
          buffer_(capacity_) {

        for (std::size_t i = 0; i < capacity_; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    template<typename U>
    bool push(U&& value) {
        static_assert(std::is_same_v<std::decay_t<U>, T>,
                      "value type mismatch");

        Cell* cell;
        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            std::size_t seq =
                cell->sequence.load(std::memory_order_acquire);

            intptr_t diff =
                static_cast<intptr_t>(seq) -
                static_cast<intptr_t>(pos);

            if (diff == 0) {
                if (head_.compare_exchange_weak(
                        pos, pos + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }

        cell->storage = std::forward<U>(value);
        cell->sequence.store(pos + 1,
                             std::memory_order_release);
        return true;
    }

    bool pop(T& out) {
        Cell* cell;
        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            std::size_t seq =
                cell->sequence.load(std::memory_order_acquire);

            intptr_t diff =
                static_cast<intptr_t>(seq) -
                static_cast<intptr_t>(pos + 1);

            if (diff == 0) {
                if (tail_.compare_exchange_weak(
                        pos, pos + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }

        out = std::move(cell->storage);
        cell->sequence.store(
            pos + mask_ + 1,
            std::memory_order_release);
        return true;
    }

private:
    struct Cell {
        std::atomic<std::size_t> sequence;
        T storage;
    };

    static std::size_t normalize_capacity(std::size_t n) {
        if (n < 2) n = 2;
        std::size_t p = 1;
        while (p < n) p <<= 1;
        return p;
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<Cell> buffer_;
    std::atomic<std::size_t> head_;
    std::atomic<std::size_t> tail_;
};

} // namespace aurora