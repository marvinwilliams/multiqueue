#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <cstdint>
#include <random>

namespace multiqueue {

template <typename ImplData>
struct NoStickingStrict : ImplData {
    using key_type = typename ImplData::key_type;
    using size_type = typename ImplData::size_type;

    class Handle {
        friend NoStickingStrict;

        pcg32 rng_;
        NoStickingStrict &impl_;

        explicit Handle(unsigned int id, NoStickingStrict &impl) noexcept
            : rng_{std::seed_seq{impl.seed, id}}, impl_{impl} {
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
            std::array<size_type, 2> index;
            do {
                index[0] = impl_.random_pq_index(rng_);
            } while (!impl_.pq_list[index[0]].try_lock());
            if (impl_.pq_list[index[0]].unsafe_empty()) {
                impl_.pq_list[index[0]].unlock();
                do {
                    index[1] = impl_.random_pq_index(rng_);
                } while (index[1] == index[0] || !impl_.pq_list[index[1]].try_lock());
                if (impl_.pq_list[index[1]].unsafe_empty()) {
                    impl_.pq_list[index[1]].unlock();
                    return false;
                }
                retval = impl_.pq_list[index[1]].unsafe_top();
                impl_.pq_list[index[1]].unsafe_pop();
                impl_.pq_list[index[1]].unlock();
                return true;
            }
            do {
                index[1] = impl_.random_pq_index(rng_);
            } while (index[1] == index[0] || !impl_.pq_list[index[1]].try_lock());
            if (impl_.pq_list[index[1]].unsafe_empty()) {
                impl_.pq_list[index[1]].unlock();
                retval = impl_.pq_list[index[0]].unsafe_top();
                impl_.pq_list[index[0]].unsafe_pop();
                impl_.pq_list[index[0]].unlock();
                return true;
            }
            std::size_t select_pq =
                (impl_.value_comp()(impl_.pq_list[index[0]].unsafe_top(), impl_.pq_list[index[1]].unsafe_top())) ? 1
                                                                                                                 : 0;
            impl_.pq_list[index[1 - select_pq]].unlock();
            retval = impl_.pq_list[index[select_pq]].unsafe_top();
            impl_.pq_list[index[select_pq]].unsafe_pop();
            impl_.pq_list[index[select_pq]].unlock();
            return true;
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;
ndle(std::uint32_t seed,/Handle(unsigned int id,/g
    NoStickingStrict(std::size_t n, Config const &config, typename ImplData::key_compare const &compare)
        : ImplData(n, config.seed, compare) {
    }

    handle_type get_handle(unsigned int id) noexcept {
        return handle_type{id, *this};
    }
};

}  // namespace multiqueue
