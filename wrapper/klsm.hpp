#ifndef WRAPPER_KLSM_HPP_INCLUDED
#define WRAPPER_KLSM_HPP_INCLUDED

// Adapted from klsm

#include "k_lsm/k_lsm.h"

#include <string>
#include <utility>

namespace wrapper {

template <typename KeyType, typename ValueType, int Relaxation = 256>
class klsm {
    kpq::k_lsm<KeyType, ValueType, Relaxation> pq_;

   public:
    struct Handle{};

    klsm(unsigned int = 0) : pq_{} {
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
        return "klsm" + std::to_string(Relaxation);
    }
};

}  // namespace wrapper

#endif
