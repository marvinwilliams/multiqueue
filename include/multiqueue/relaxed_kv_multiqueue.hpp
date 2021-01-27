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

#ifndef RSM_KV_PQ_HPP_LTZXQCH1
#define RSM_KV_PQ_HPP_LTZXQCH1

#include "multiqueue/kv_pq.hpp"
#include "multiqueue/util/range_iterator.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <random>
#include <vector>

namespace multiqueue {
namespace rsm {

struct DefaultKVConfiguration {
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // Number of local queues to test for finding the smallest
    static constexpr unsigned int Peek = 4;
};

template <typename T>
struct Sentinel;

template <typename Key, typename Value, typename Configuration = DefaultKVConfiguration,
          typename Queue = multiqueue::local_nonaddressable::kv_pq<Key, Value>,
          typename Allocator = std::allocator<Key>>
class kv_priority_queue {
   public:
    using key_type = Key;
    using mapped_type = Value;
    using value_type = typename Queue::value_type;
    using pop_return_type = value_type;

   private:
    struct alignas(64) aligned_pq {
        Queue queue = Queue();
        std::pair<std::atomic<Key>, std::atomic<Value>> top = {Sentinel<Key>::get(), mapped_type{}};
        mutable std::atomic_flag in_use = ATOMIC_FLAG_INIT;
    };
    struct is_nonempty {
        constexpr bool operator()(size_t index) {
            return !Sentinel<Key>::is_sentinel(pq->queue_list_[index].top.first.load(std::memory_order_relaxed));
        }
        kv_priority_queue const *pq;
    };
    using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<aligned_pq>;

   private:
    std::vector<aligned_pq, allocator_type> queue_list_;
    size_t num_queues_;
    typename Queue::key_comparator comp_;

    inline std::mt19937 &get_rng() const {
        static thread_local std::mt19937 gen;
        return gen;
    }

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
    explicit kv_priority_queue(unsigned int const num_threads)
        : queue_list_(num_threads * Configuration::C),
          num_queues_{static_cast<unsigned int>(queue_list_.size())},
          comp_{} {
        assert(num_threads >= 1);
        assert(num_queues_ >= Configuration::Peek);
    }

    explicit kv_priority_queue(std::allocator_arg_t, Allocator const &a, unsigned int const num_threads)
        : queue_list_(num_threads * Configuration::C, a),
          num_queues_{static_cast<unsigned int>(queue_list_.size())},
          comp_{} {
    }

    value_type top() const {
        std::array<size_t, Configuration::Peek> sample;
        while (true) {
            auto queue_it = util::predicate_iterator<size_t, is_nonempty>{0, num_queues_, is_nonempty{this}};
            auto sample_end = std::sample(queue_it, queue_it.end(), std::begin(sample), Configuration::Peek, get_rng());
            if (std::begin(sample) == sample_end) {
                return {Sentinel<key_type>::get(), mapped_type{}};
            }
            auto min_key = Sentinel<key_type>{};
            auto min_value = mapped_type{};
            auto it = std::begin(sample);
            for (; it != sample_end; ++it) {
                min_key = queue_list_[*it].top.first.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(min_key)) {
                    min_value = queue_list_[*it].top.second.load(std::memory_order_relaxed);
                    if (min_key == queue_list_[*it].top.first.load(std::memory_order_relaxed)) {
                        break;
                    }
                }
            }
            for (++it; it < sample_end; ++it) {
                auto current_key = queue_list_[*it].top.first.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(current_key) && comp_(current_key, min_key)) {
                    auto current_val = queue_list_[*it].top.second.load(std::memory_order_relaxed);
                    if (current_key == queue_list_[*it].top.first.load(std::memory_order_relaxed)) {
                        min_key = std::move(current_key);
                        min_value = std::move(current_val);
                    }
                }
            }
            if (!Sentinel<Key>::is_sentinel(min_key)) {
                return {min_key, min_value};
            }
        }
    }

    void push(value_type value) {
        assert(!Sentinel<key_type>::is_sentinel(value.first));
        size_t queue_index = random_queue_index();
        while (!try_lock(queue_index)) {
            queue_index = (queue_index + 1) % num_queues_;
        }
        if (queue_list_[queue_index].top.first == Sentinel<key_type>::get()) {
            queue_list_[queue_index].top = std::move(value);
        } else if (comp_(value.first, queue_list_[queue_index].top.first)) {
            queue_list_[queue_index].queue.push(std::move(queue_list_[queue_index].top));
            queue_list_[queue_index].top = std::move(value);
        } else {
            queue_list_[queue_index].queue.push(std::move(value));
        }
        unlock(queue_index);
    }

    inline value_type extract_top() {
        std::array<size_t, Configuration::Peek> sample;
        while (true) {
            auto queue_it = util::predicate_iterator<size_t, is_nonempty>{0, num_queues_, is_nonempty{this}};
            auto sample_end = std::sample(queue_it, queue_it.end(), std::begin(sample), Configuration::Peek, get_rng());
            if (std::begin(sample) == sample_end) {
                return {Sentinel<key_type>::get(), mapped_type{}};
            }
            auto min_key = Sentinel<key_type>::get();
            auto it = std::begin(sample);
            for (; it != sample_end; ++it) {
                min_key = queue_list_[*it].top.first.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(min_key)) {
                    break;
                }
            }
            auto min_it = it;
            for (++it; it < sample_end; ++it) {
                auto current_key = queue_list_[*it].top.first.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(current_key) && comp_(current_key, min_key)) {
                    min_key = std::move(current_key);
                    min_it = it;
                }
            }
            if (min_it < sample_end) {
                if (!try_lock(*min_it)) {
                    continue;
                }
                auto result_key = queue_list_[*it].top.first.load(std::memory_order_relaxed);
                if (Sentinel<key_type>::is_sentinel(result_key)) {
                    continue;
                }
                return {result_key, queue_list_[*it].top.first.load(std::memory_order_relaxed)};
            }
        }
        return {Sentinel<key_type>::get(), mapped_type{}};
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif /* end of include guard: RSM_PQ_HPP_LTZXQCH1 */
