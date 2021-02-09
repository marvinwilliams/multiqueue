#ifndef CAPQ_HPP
#define CAPQ_HPP

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <utility>

namespace multiqueue {
namespace wrapper {

struct capq_wrapper_type;

template <bool remove_min_relax = true, bool put_relax = true, bool catree_adapt = true>
class capq {
    capq_wrapper_type* pq_;

   public:
    capq(unsigned int = 0);

    void push(std::pair<uint32_t, uint32_t> const& value);
    bool extract_top(std::pair<uint32_t, uint32_t>& retval);
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
