#ifndef WRAPPER_DLSM_HPP_INCLUDED
#define WRAPPER_DLSM_HPP_INCLUDED

// Adapted from klsm

#include "dist_lsm/dist_lsm.h"

#include <string>
#include <utility>

namespace multiqueue {
namespace wrapper {

template <typename KeyType, typename ValueType>
class dlsm {
    kpq::dist_lsm<KeyType, ValueType, 256> pq_;

   public:
    struct Handle{};

    dlsm(unsigned int = 0) : pq_{} {
    }

    constexpr Handle get_handle(unsigned int) {
        return Handle{};
    }

    void push(Handle, std::pair<KeyType, ValueType> const& value) {
        pq_.insert(value.first, value.second);
    }

    bool extract_top(Handle, std::pair<KeyType, ValueType>& retval) {
        return pq_.delete_min(retval.first, retval.second);
    }

    static std::string description() {
        return "dlsm";
    }
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
