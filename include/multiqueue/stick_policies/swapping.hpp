#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <random>

namespace multiqueue {

template <typename ImplData>
struct Swapping : public ImplData {
    using key_type = typename ImplData::key_type;
    using size_type = typename ImplData::size_type;

    class Handle {
        friend Swapping;

        pcg32 rng_;
        Swapping &impl_;
        std::size_t permutation_index_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;
#ifdef MULTIQUEUE_COUNT_STATS
        std::size_t num_resets_{0};
#endif
        explicit Handle(unsigned int id, Swapping &impl) noexcept
            : rng_{std::seed_seq{impl.seed, id}},
              impl_{impl},
              permutation_index_{static_cast<std::size_t>(impl_.handle_count * 2)},
              stick_index_{impl_.permutation[permutation_index_].i.load(std::memory_order_relaxed),
                           impl_.permutation[permutation_index_ + 1].i.load(std::memory_order_relaxed)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        void swap_assignment(std::size_t pq) noexcept {
            assert(pq <= 1);
            if (!impl_.permutation[permutation_index_ + pq].i.compare_exchange_strong(stick_index_[pq], impl_.num_pqs,
                                                                                      std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(stick_index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index = 0;
            size_type target_assigned;
            do {
                target_index = impl_.random_pq_index(rng_);
                target_assigned = impl_.permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !impl_.permutation[target_index].i.compare_exchange_strong(target_assigned, stick_index_[pq],
                                                                                std::memory_order_relaxed));
            impl_.permutation[permutation_index_ + pq].i.store(target_assigned, std::memory_order_relaxed);
            stick_index_[pq] = target_assigned;
        }

        void refresh_pq(std::size_t pq) noexcept {
            if (use_count_[pq] != 0) {
                auto current_index = impl_.permutation[permutation_index_ + pq].i.load(std::memory_order_relaxed);
                if (current_index == stick_index_[pq]) {
                    return;
                }
                stick_index_[pq] = current_index;
            } else {
                swap_assignment(pq);
            }
            use_count_[pq] = impl_.stickiness;
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

        void push(typename ImplData::const_reference value) {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            refresh_pq(push_pq);
            if (!impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    swap_assignment(push_pq);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
#ifdef MULTIQUEUE_COUNT_STATS
            if (use_count_[push_pq] == impl_.stickiness) {
                ++num_resets_;
            }
#endif
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(typename ImplData::reference retval) {
            refresh_pq(0);
            refresh_pq(1);
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.sentinel_aware_comp()(key[0], key[1]) ? 1 : 0;
                if (ImplData::is_sentinel(key[select_pq])) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        --use_count_[0];
                        --use_count_[1];
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
#ifdef MULTIQUEUE_COUNT_STATS
                    if (use_count_[select_pq] == impl_.stickiness) {
                        ++num_resets_;
                    }
#endif
                }
                swap_assignment(select_pq);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
            } while (true);
        }

        bool try_pop_from(size_type idx, typename ImplData::reference retval) {
            return impl_.try_pop_from(idx, retval);
        }
    };

    using handle_type = Handle;

    struct alignas(BuildConfiguration::L1CacheLinesize) AlignedIndex {
        std::atomic<size_type> i;
    };

    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    int stickiness;

    Swapping(std::size_t n, Config const &config, typename ImplData::key_compare const &compare)
        : ImplData(n, config.seed, compare),
          permutation(this->num_pqs),
          stickiness{static_cast<int>(config.stickiness)} {
        for (std::size_t i = 0; i < this->num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    handle_type get_handle(unsigned int id) noexcept {
        return handle_type{id, *this};
    }
};

}  // namespace multiqueue
