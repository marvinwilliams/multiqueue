#pragma once

#include "multiqueue/config.hpp"
#include "multiqueue/stick_policy.hpp"
#include "multiqueue/third_party/pcg_random.hpp"

#include <array>
#include <random>

namespace multiqueue {

template <typename ImplData>
struct Random<ImplData> : public ImplData {
    using key_type = typename ImplData::key_type;
    using size_type = typename ImplData::size_type;

    class Handle {
        friend Random;

        pcg32 rng_;
        Random &impl_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;

        explicit Handle(std::uint32_t seed, Random &impl) noexcept
            : rng_{std::seed_seq{seed}},
              impl_{impl},
              stick_index_{impl_.random_pq_index(rng_), impl_.random_pq_index(rng_)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

        void push(typename ImplData::const_reference value) {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            if (use_count_[push_pq] == 0 || !impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    stick_index_[push_pq] = impl_.random_pq_index(rng_);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(typename ImplData::reference retval) {
            if (use_count_[0] == 0) {
                stick_index_[0] = impl_.random_pq_index(rng_);
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                stick_index_[1] = impl_.random_pq_index(rng_);
                use_count_[1] = impl_.stickiness;
            }
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
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
                }
                stick_index_[select_pq] = impl_.random_pq_index(rng_);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    int stickiness;

    Random(std::size_t num_pqs, Config const &config, typename ImplData::key_compare const &compare)
        : ImplData(num_pqs, config.seed, compare), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        return handle_type{this->rng(), *this};
    }
};

}  // namespace multiqueue
