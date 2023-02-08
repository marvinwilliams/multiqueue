#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <cstdint>
#include <random>

namespace multiqueue {

template <typename ImplData>
struct NoSticking : ImplData {
    using key_type = typename ImplData::key_type;
    using size_type = typename ImplData::size_type;

    class Handle {
        friend NoSticking;

        pcg32 rng_;
        NoSticking &impl_;

        explicit Handle(unsigned int id, NoSticking &impl) noexcept : rng_{std::seed_seq{impl.seed, id}}, impl_{impl} {
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

        void push(typename ImplData::const_reference value) {
            size_type index;
            do {
                index = impl_.random_pq_index(rng_);
            } while (!impl_.pq_list[index].try_lock());
            impl_.pq_list[index].unsafe_push(value);
            impl_.pq_list[index].unlock();
        }

        bool try_pop(typename ImplData::reference retval) {
            do {
                std::array<size_type, 2> index = {impl_.random_pq_index(rng_), impl_.random_pq_index(rng_)};
                while (index[0] == index[1]) {
                    index[1] = impl_.random_pq_index(rng_);
                }
                std::array<key_type, 2> key = {impl_.pq_list[index[0]].concurrent_top_key(),
                                               impl_.pq_list[index[1]].concurrent_top_key()};
                std::size_t select_pq = (impl_.sentinel_aware_comp()(key[0], key[1])) ? 1 : 0;
                if (ImplData::is_sentinel(key[select_pq])) {
                    return false;
                }
                size_type select_index = index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        bool try_pop_from(size_type idx, typename ImplData::reference retval) {
            return impl_.try_pop_from(idx, retval);
        }
    };

    using handle_type = Handle;

    NoSticking(std::size_t n, Config const &config, typename ImplData::key_compare const &compare)
        : ImplData(n, config.seed, compare) {
    }

    handle_type get_handle(unsigned int id) noexcept {
        return handle_type{id, *this};
    }
};

}  // namespace multiqueue
