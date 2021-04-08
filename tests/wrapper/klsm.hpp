#ifndef WRAPPER_KLSM_HPP
#define WRAPPER_KLSM_HPP

// Adapted from klsm

#include "k_lsm/k_lsm.h"

#include <cstdint>
#include <string>
#include <utility>

namespace multiqueue {
namespace wrapper {

template <typename KeyType, typename ValueType>
class klsm {
    kpq::k_lsm<KeyType, ValueType, 256> pq_;

   public:
    klsm(unsigned int = 0) : pq_{} {
    }

    void push(std::pair<KeyType, ValueType> const& value) {
        pq_.insert(value.first, value.second);
    }
    bool extract_top(std::pair<KeyType, ValueType>& retval) {
        return pq_.delete_min(retval.first, retval.second);
    }

    static std::string description() {
        return "klsm";
    }
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
