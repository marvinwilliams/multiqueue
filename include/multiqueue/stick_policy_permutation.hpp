#pragma once

#include "multiqueue/config.hpp"
#include "multiqueue/stick_policy.hpp"
#include "multiqueue/third_party/pcg_random.hpp"

#include <array>
#include <atomic>
#include <random>

namespace multiqueue {

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
template <typename Data>
struct StickPolicyImpl : public Data {
    using key_type = typename Data::key_type;
    using size_type = typename Data::size_type;

    class Handle {
        friend StickPolicyImpl;

        pcg32 rng_;
        StickPolicyImpl &impl_;

        std::size_t permutation_index_;
        std::uint64_t current_permutation_;
        int use_count_{};

        explicit Handle(std::uint32_t seed, StickPolicyImpl &impl) noexcept
            : rng_{std::seed_seq{seed}},
              impl_{impl},
              permutation_index_{impl_.handle_count * 2},
              current_permutation_{impl_.permutation.load(std::memory_order_relaxed)} {
            ++impl_.handle_count;
        }

        void update_permutation() {
            std::uint64_t new_permutation = rng_() | 1;
            if (impl_.permutation.compare_exchange_strong(current_permutation_, new_permutation,
                                                          std::memory_order_relaxed)) {
                current_permutation_ = new_permutation;
            }
        }

        size_type get_index(std::size_t pq) const noexcept {
            static constexpr int shift_width = 32;
            size_type a = current_permutation_ & Mask;
            size_type b = current_permutation_ >> shift_width;
            assert((a & 1) == 1);
            return ((permutation_index_ + pq) * a + b) & (impl_.num_pqs - 1);
        }

        void refresh_permutation() {
            if (use_count_ >= 2 * impl_.stickiness) {
                auto p = impl_.permutation.load(std::memory_order_relaxed);
                if (p == current_permutation_) {
                    return;
                }
                current_permutation_ = p;
            } else {
                update_permutation();
            }
            use_count_ = 0;
        }

       public:
        void push(typename Data::const_reference value) noexcept {
            refresh_permutation();
            bool push_pq = std::bernoulli_distribution{}(rng_);
            size_type pq_index = get_index(push_pq);
            while (!impl_.pq_list[pq_index].try_lock()) {
                auto p = impl_.permutation.load(std::memory_order_relaxed);
                if (p != current_permutation_) {
                    current_permutation_ = p;
                    use_count_ = 0;
                }
            }
            impl_.pq_list[pq_index].unsafe_push(value);
            impl_.pq_list[pq_index].unlock();
            ++use_count_;
        }

        bool try_pop(typename Data::reference retval) noexcept {
            refresh_permutation();
            assert(use_count_ <= 2 * impl_.stickiness);
            std::array<size_type, 2> pq_index = {get_index(0), get_index(1)};
            std::array<key_type, 2> key = {impl_.pq_list[pq_index[0]].concurrent_top_key(),
                                           impl_.pq_list[pq_index[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (Data::is_sentinel(key[select_pq])) {
                    use_count_ = 2 * impl_.stickiness;
                    return false;
                }
                size_type select_index = pq_index[select_pq];
                while (true) {
                    if (impl_.pq_list[select_index].try_lock()) {
                        if (!impl_.pq_list[select_index].unsafe_empty()) {
                            retval = impl_.pq_list[select_index].unsafe_top();
                            impl_.pq_list[select_index].unsafe_pop();
                            impl_.pq_list[select_index].unlock();
                            return true;
                        }
                        key[select_index] = Sentinel::get();
                        impl_.pq_list[select_index].unlock();
                        break;
                    }
                    auto p = impl_.permutation.load(std::memory_order_relaxed);
                    if (p != current_permutation_) {
                        current_permutation_ = p;
                        use_count_ = 0;
                        pq_index[0] = get_index(0);
                        pq_index[1] = get_index(1);
                        key[0] = impl_.pq_list[pq_index[0]].concurrent_top_key();
                        key[1] = impl_.pq_list[pq_index[1]].concurrent_top_key();
                        break;
                    }
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    static constexpr std::uint64_t Mask = 0xffffffff;

    alignas(BuildConfiguration::L1CacheLinesize) std::atomic_uint64_t permutation;
    int stickiness;
    int handle_count = 0;

    static constexpr std::size_t next_power_of_two(std::size_t n) {
        return std::size_t{1} << static_cast<unsigned int>(std::ceil(std::log2(n)));
    }

    StickPolicyImpl(int num_threads, Config const &config, typename Data::key_compare const &compare)
        : Data(next_power_of_two(static_cast<std::size_t>(num_threads) * config.c), config.seed, compare),
          stickiness{config.stickiness},
          permutation{1} {
    }

    handle_type get_handle() noexcept {
        return handle_type{this->rng(), *this};
    }
};

}  // namespace multiqueue
