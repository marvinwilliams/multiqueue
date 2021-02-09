#ifndef KLSM_HPP
#define KLSM_HPP

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace multiqueue {
namespace wrapper {

template <typename LsmVariant>
class klsm {
    LsmVariant pq_;

   public:
    klsm(unsigned int) : pq_{} {
    }

    void push(std::pair<uint32_t, uint32_t> const& value) {
        pq_.insert(value.first, value.second);
    }
    bool extract_top(std::pair<uint32_t, uint32_t>& retval) {
        return pq_.delete_min(retval.first, retval.second);
    }
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
