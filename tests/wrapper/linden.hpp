#ifndef WRAPPER_LINDEN_HPP_INCLUDED
#define WRAPPER_LINDEN_HPP_INCLUDED

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace multiqueue {
namespace wrapper {

struct linden_pq_wrapper;

class linden {
    linden_pq_wrapper* pq_;

   public:
    static constexpr int DEFAULT_OFFSET = 32;

    linden(unsigned int num_threads = 0, int const max_offset = DEFAULT_OFFSET);

    ~linden();

    void push(std::pair<uint32_t, uint32_t> const& value);

    bool extract_top(std::pair<uint32_t, uint32_t>& retval);

    static std::string description() {
        return "linden";
    }
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
