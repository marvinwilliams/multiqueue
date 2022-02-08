/**
******************************************************************************
* @file:   guarded_fifo.hpp
*
* @author: Marvin Williams
* @date:   2021/11/09 18:05
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef GUARDED_FIFO
#define GUARDED_FIFO

#include <atomic>
#include <cassert>
#include <chrono>
#include <deque>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multififo {

template <typename T, typename Allocator = std::allocator<T>>
class alignas(2 * L1_CACHE_LINESIZE) GuardedFifo {
   public:
    using tick_type = std::chrono::time_point<std::chrono::steady_clock>;
    using value_type = T;
    using node_type = std::pair<tick_type, value_type>;
    using fifo_type =
        std::deque<node_type, typename std::allocator_traits<Allocator>::template rebind_alloc<node_type>>;
    using reference = typename fifo_type::reference;
    using const_reference = typename fifo_type::const_reference;
    using size_type = typename fifo_type::size_type;
    using allocator_type = Allocator;

   private:
    alignas(L1_CACHE_LINESIZE) std::atomic<tick_type> top_tick;
    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock;
    alignas(L1_CACHE_LINESIZE) fifo_type queue_;

   public:
    explicit GuardedFifo(allocator_type const& alloc) : top_tick(get_sentinel()), lock(false), queue_(alloc) {
    }

    static constexpr tick_type get_sentinel() noexcept {
        return tick_type{};
    }

    bool is_locked() const noexcept {
        return lock.load(std::memory_order_relaxed);
    }

    bool try_lock() noexcept {
        // Maybe do not to test?
        return !is_locked() && !lock.exchange(true, std::memory_order_acquire);
    }

    tick_type get_tick() const noexcept {
        return top_tick.load(std::memory_order_relaxed);
    }

    void unlock() noexcept {
        assert(is_locked());
        auto current_tick = queue_.empty() ? get_sentinel() : queue_.top().first;
        if (top_tick.load(std::memory_order_relaxed) != current_tick) {
            top_tick.store(current_tick, std::memory_order_relaxed);
        }
        lock.store(false, std::memory_order_release);
    }

    bool try_lock_if_tick(tick_type const& tick) noexcept {
        if (!try_lock()) {
            return false;
        }

        tick_type current_tick = top_tick.load(std::memory_order_relaxed);
        // One could just check for the tick not to be the sentinel to allow inaccuracies
        if (current_tick == tick) {
            return true;
        } else {
            lock.store(false, std::memory_order_release);
            return false;
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return get_tick() == get_sentinel();
    }

    void pop() {
        assert(is_locked());
        assert(!unsafe_empty());
        queue_.pop_front();
    }

    void extract_top(reference retval) {
        assert(is_locked());
        assert(!empty());
        retval = queue_.front().second;
        queue_.pop_front();
    };

    void push(const_reference value) {
        assert(is_locked());
        tick_type tick = std::chrono::steady_clock::now();
        queue_.push_back({tick, value});
    }

    void push(value_type&& value) {
        assert(is_locked());
        tick_type tick = std::chrono::steady_clock::now();
        queue_.push_back({tick, std::move(value)});
    }

    void clear() noexcept {
        assert(is_locked());
        queue_.clear();
    }

    [[nodiscard]] bool unsafe_empty() const noexcept {
        return queue_.empty();
    }

    size_type unsafe_size() const noexcept {
        return queue_.size();
    }

    const_reference unsafe_top() const {
        assert(!unsafe_empty());
        return queue_.front();
    }
};

}  // namespace multififo

#endif  //! GUARDED_FIFO
