/******************************************************************************
 * File:             rsm_pq.hpp
 *
 * Author:           Marvin Williams
 * Created:          12/22/20
 * Description:      This header implements a relaxed shared-memory priority
 *                   queue.  With `p` threads and `c >= 1`, there are `c*p`
 *                   priority queues, and queries as well as modifications
 *                   operate on randomly selected queues only.
 *****************************************************************************/

#ifndef RSM_PQ_HPP_LTZXQCH1
#define RSM_PQ_HPP_LTZXQCH1

#include "multiqueue/pq.hpp"

#include <atomic>
#include <cassert>
#include <random>
#include <vector>

namespace multiqueue {
namespace rsm {

struct DefaultConfiguration {
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // Number of retries for finding any nonempty queue
    static constexpr unsigned int Tries = 20;
    // Number of local queues to test for finding the smallest
    static constexpr unsigned int Peek = 4;
};

template <typename T>
struct Sentinel {
    static constexpr T get() noexcept {
        return T();
    }
    static constexpr bool is(T const &v) noexcept {
        return v == get();
    }
};

template <typename T, typename Configuration = DefaultConfiguration,
          typename Queue = multiqueue::local_nonaddressable::pq<int>>
class priority_queue {
   private:
    struct alignas(64) aligned_pq {
        Queue queue = Queue();
        T top_value = Sentinel<T>::get();
        mutable std::atomic_flag in_use = ATOMIC_FLAG_INIT;
    };

   public:
    using value_type = T;
    using pop_return_type = value_type;

   private:
    std::vector<aligned_pq> queue_list_;
    unsigned int const num_queues_;
    typename Queue::value_comparator comp_;

    inline size_t random_queue_index() const {
        static thread_local std::mt19937 gen;
        std::uniform_int_distribution<size_t> dist{0, num_queues_ - 1};
        return dist(gen);
    }

    inline bool try_lock(size_t const index) const noexcept {
        if (!queue_list_[index].in_use.test_and_set(std::memory_order_relaxed)) {
            std::atomic_thread_fence(std::memory_order_acquire);
            return true;
        }
        return false;
    }

    inline void unlock(size_t const index) const noexcept {
        queue_list_[index].in_use.clear(std::memory_order_release);
    }

   public:
    explicit priority_queue(unsigned int const num_threads)
        : queue_list_(num_threads * Configuration::C),
          num_queues_{static_cast<unsigned int>(queue_list_.size())},
          comp_{} {
        assert(num_threads >= 1);
        assert(num_queues_ >= Configuration::Peek);
    }

    inline value_type top() const {
        for (unsigned int try_count = 0; try_count < Configuration::Tries; ++try_count) {
            size_t peek_count;
            size_t queue_index;
            for (peek_count = 0; peek_count < Configuration::Peek; ++peek_count) {
                queue_index = random_queue_index();
                while (!try_lock(queue_index)) {
                    queue_index = (queue_index + 1) % num_queues_;
                }
                if (!Sentinel<T>::is(queue_list_[queue_index].top_value)) {
                    break;
                }
                unlock(queue_index);
            }
            if (peek_count == Configuration::Peek) {
                // already unlocked
                continue;
            }
            assert(!Sentinel<T>::is(queue_list_[queue_index].top_value));
            T min_value = queue_list_[queue_index].top_value;
            size_t min_index = queue_index;
            for (; peek_count < Configuration::Peek; ++peek_count) {
                queue_index = random_queue_index();
                while (!try_lock(queue_index)) {
                    queue_index = (queue_index + 1) % num_queues_;
                }
                if (!Sentinel<T>::is(queue_list_[queue_index].top_value) &&
                    comp_(queue_list_[queue_index].top_value, min_value)) {
                    unlock(min_index);
                    min_value = queue_list_[queue_index].top_value;
                    min_index = queue_index;
                } else {
                    unlock(queue_index);
                }
            }
            unlock(min_index);
            return min_value;
        }
        return Sentinel<T>::get();
    }

    void push(value_type value) {
        assert(!Sentinel<T>::is(value));
        size_t queue_index = random_queue_index();
        while (!try_lock(queue_index)) {
            queue_index = (queue_index + 1) % num_queues_;
        }
        if (queue_list_[queue_index].top_value == Sentinel<T>::get()) {
            queue_list_[queue_index].top_value = std::move(value);
        } else if (comp_(value, queue_list_[queue_index].top_value)) {
            queue_list_[queue_index].queue.push(std::move(queue_list_[queue_index].top_value));
            queue_list_[queue_index].top_value = std::move(value);
        } else {
            queue_list_[queue_index].queue.push(std::move(value));
        }
        unlock(queue_index);
    }

    inline value_type extract_top() {
        for (unsigned int try_count = 0; try_count < Configuration::Tries; ++try_count) {
            size_t peek_count;
            size_t queue_index;
            for (peek_count = 0; peek_count < Configuration::Peek; ++peek_count) {
                queue_index = random_queue_index();
                while (!try_lock(queue_index)) {
                    queue_index = (queue_index + 1) % num_queues_;
                }
                if (!Sentinel<T>::is(queue_list_[queue_index].top_value)) {
                    break;
                }
                unlock(queue_index);
            }
            if (peek_count == Configuration::Peek) {
                // already unlocked
                continue;
            }
            assert(!Sentinel<T>::is(queue_list_[queue_index].top_value));
            T min_value = queue_list_[queue_index].top_value;
            size_t min_index = queue_index;
            for (; peek_count < Configuration::Peek; ++peek_count) {
                queue_index = random_queue_index();
                while (!try_lock(queue_index)) {
                    queue_index = (queue_index + 1) % num_queues_;
                }
                if (!Sentinel<T>::is(queue_list_[queue_index].top_value) &&
                    comp_(queue_list_[queue_index].top_value, min_value)) {
                    unlock(min_index);
                    min_value = queue_list_[queue_index].top_value;
                    min_index = queue_index;
                } else {
                    unlock(queue_index);
                }
            }
            if (queue_list_[min_index].queue.empty()) {
                queue_list_[min_index].top_value = Sentinel<T>::get();
            } else {
                queue_list_[min_index].top_value = queue_list_[min_index].queue.top();
                queue_list_[min_index].queue.pop();
            }
            unlock(min_index);
            return min_value;
        }
        return Sentinel<T>::get();
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif /* end of include guard: RSM_PQ_HPP_LTZXQCH1 */
