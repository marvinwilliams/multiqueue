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

template <typename Key, typename Value>
struct DefaultKVConfiguration {
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // Number of local queues to test for finding the smallest
    static constexpr unsigned int Peek = 4;
    using Queue = multiqueue::local_nonaddressable::kv_pq<Key, Value>;
};

template <typename T>
struct Sentinel;

template <typename Key, typename Value, template <typename, typename> typename Configuration = DefaultKVConfiguration,
          typename Allocator = std::allocator<Key>>
class kv_priority_queue {
   public:
    using key_type = Key;
    using mapped_type = Value;
    using queue_type = typename Configuration<key_type, mapped_type>::Queue;
    using value_type = typename queue_type::value_type;
    static constexpr auto C = static_cast<unsigned int>(Configuration<key_type, mapped_type>::C);
    static constexpr auto Peek = static_cast<unsigned int>(Configuration<key_type, mapped_type>::Peek);

   private:
    struct alignas(64) aligned_pq {
        queue_type queue = queue_type();
        std::atomic<Key> top_key = Sentinel<Key>::get();
        mutable std::atomic_flag in_use = ATOMIC_FLAG_INIT;
    };

    struct is_nonempty {
        constexpr bool operator()(size_t index) {
            return !Sentinel<Key>::is_sentinel(pq->queue_list_[index].top_key.load(std::memory_order_relaxed));
        }
        kv_priority_queue const *pq;
    };

    using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<aligned_pq>;

   private:
    std::vector<aligned_pq, allocator_type> queue_list_;
    size_t num_queues_;
    typename queue_type::key_comparator comp_;

    inline std::mt19937 &get_rng() const {
        static thread_local std::mt19937 gen;
        return gen;
    }

    inline size_t random_queue_index() const {
        std::uniform_int_distribution<size_t> dist{0, num_queues_ - 1};
        return dist(get_rng());
    }

    inline bool try_lock(size_t const index) const noexcept {
        // TODO: MEASURE
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
        : queue_list_(num_threads * C), num_queues_{static_cast<unsigned int>(queue_list_.size())}, comp_{} {
        assert(num_threads >= 1);
        assert(num_queues_ >= Configuration::Peek);
    }

    explicit kv_priority_queue(std::allocator_arg_t, Allocator const &a, unsigned int const num_threads)
        : queue_list_(num_threads * C, {queue_type(a)}),
          num_queues_{static_cast<unsigned int>(queue_list_.size())},
          comp_{} {
        assert(num_threads >= 1);
        assert(num_queues_ >= Configuration::Peek);
    }

    bool top(value_type &retval) const {
        while (true) {
            auto const start_index = random_queue_index();
            auto index = start_index;
            auto min_index = index;
            /* auto nonempty_it = util::predicate_iterator<size_t, is_nonempty>{index, num_queues_, is_nonempty{this}};
             */
            auto min_key = Sentinel<key_type>{};
            unsigned int num_nonempty = 0;
            while (true) {
                auto current_key = queue_list_[index].top_key.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(current_key)) {
                    if ((num_nonempty == 0 || comp_(current_key, min_key))) {
                        min_key = std::move(current_key);
                        min_index = index;
                    }
                    if (++num_nonempty == Peek) {
                        break;
                    }
                }
                index = (index + 1) % num_queues_;
                if (index == start_index) {
                    break;
                }
            }
            if (num_nonempty == 0) {
                return false;
            }
            if (!try_lock(min_index)) {
                continue;
            }
            if (!queue_list_[min_index].queue.empty()) {
                retval = queue_list_[min_index].queue.top();
                unlock(min_index);
                return true;
            }
            unlock(min_index);
        }
        return false;
    }

    void push(value_type value) {
        assert(!Sentinel<key_type>::is_sentinel(value.first));
        size_t queue_index = random_queue_index();
        while (!try_lock(queue_index)) {
            queue_index = (queue_index + 1) % num_queues_;
        }
        queue_list_[queue_index].queue.push(std::move(value));
        queue_list_[queue_index].top_key.store(queue_list_[queue_index].queue.top().first, std::memory_order_relaxed);
        unlock(queue_index);
    }

    bool extract_top(value_type &retval) {
        while (true) {
            auto const start_index = random_queue_index();
            auto index = start_index;
            auto min_index = index;
            /* auto nonempty_it = util::predicate_iterator<size_t, is_nonempty>{index, num_queues_, is_nonempty{this}};
             */
            auto min_key = Sentinel<key_type>::get();
            unsigned int num_nonempty = 0;
            while (true) {
                auto current_key = queue_list_[index].top_key.load(std::memory_order_relaxed);
                if (!Sentinel<Key>::is_sentinel(current_key)) {
                    if ((num_nonempty == 0 || comp_(current_key, min_key))) {
                        min_key = std::move(current_key);
                        min_index = index;
                    }
                    if (++num_nonempty == Peek) {
                        break;
                    }
                }
                index = (index + 1) % num_queues_;
                if (index == start_index) {
                    break;
                }
            }
            if (num_nonempty == 0) {
                return false;
            }
            if (!try_lock(min_index)) {
                continue;
            }
            if (!queue_list_[min_index].queue.empty()) {
                queue_list_[min_index].queue.extract_top(retval);
                if (queue_list_[min_index].queue.empty()) {
                    queue_list_[min_index].top_key.store(Sentinel<Key>::get(), std::memory_order_relaxed);
                } else {
                    queue_list_[min_index].top_key.store(queue_list_[min_index].queue.top().first,
                                                         std::memory_order_relaxed);
                }
                unlock(min_index);
                return true;
            }
            unlock(min_index);
        }
        return false;
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif /* end of include guard: RSM_PQ_HPP_LTZXQCH1 */
