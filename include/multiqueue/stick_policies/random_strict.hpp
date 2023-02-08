#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <random>

namespace multiqueue {

template <typename ImplData>
struct RandomStrict : public ImplData {
    using key_type = typename ImplData::key_type;
    using size_type = typename ImplData::size_type;

    class Handle {
        friend RandomStrict;

        pcg32 rng_;
        RandomStrict &impl_;
        std::array<size_type, 2> stick_index_;
        int use_count_;

        explicit Handle(unsigned int id, RandomStrict &impl) noexcept
            : rng_{std::seed_seq{impl.seed, id}},
              impl_{impl},
              stick_index_{impl_.random_pq_index(rng_), impl_.random_pq_index(rng_)},
              use_count_{2 * impl_.stickiness} {
            while (stick_index_[0] == stick_index_[1]) {
                stick_index_[1] = impl_.random_pq_index(rng_);
            }
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

        void push(typename ImplData::const_reference value) noexcept {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            if (use_count_ <= 0 || !impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    stick_index_[push_pq] = impl_.random_pq_index(rng_);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                do {
                    stick_index_[1 - push_pq] = impl_.random_pq_index(rng_);
                } while (stick_index_[0] == stick_index_[1]);
                use_count_ = 2 * impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_ > 0);
            --use_count_;
        }

        bool try_pop(typename ImplData::reference retval) noexcept {
            if (use_count_ > 0) {
                std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                               impl_.pq_list[stick_index_[1]].concurrent_top_key()};
                std::size_t select_pq = impl_.sentinel_aware_comp()(key[0], key[1]) ? 1 : 0;
                if (ImplData::is_sentinel(key[select_pq])) {
                    // Both pqs are empty
                    use_count_ = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        use_count_ -= 2;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            }
            do {
                stick_index_[0] = impl_.random_pq_index(rng_);
                do {
                    stick_index_[1] = impl_.random_pq_index(rng_);
                } while (stick_index_[0] == stick_index_[1]);
                std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                               impl_.pq_list[stick_index_[1]].concurrent_top_key()};
                std::size_t const select_pq = impl_.sentinel_aware_comp()(key[0], key[1]) ? 1 : 0;
                if (ImplData::is_sentinel(key[select_pq])) {
                    // Both pqs are empty
                    use_count_ = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        use_count_ = 2 * impl_.stickiness - 2;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    int stickiness;

    RandomStrict(std::size_t n, Config const &config, typename ImplData::key_compare const &compare)
        : ImplData(n, config.seed, compare), stickiness{static_cast<int>(config.stickiness)} {
    }

    handle_type get_handle(unsigned int id) noexcept {
        return handle_type{id, *this};
    }
};

}  // namespace multiqueue
