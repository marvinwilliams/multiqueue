#ifndef WRAPPER_LINDEN_HPP_INCLUDED
#define WRAPPER_LINDEN_HPP_INCLUDED

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace wrapper {

struct linden_pq_wrapper;

class linden {
    linden_pq_wrapper* pq_;

   public:
    struct Handle{};
    static constexpr int DEFAULT_OFFSET = 32;

    linden(unsigned int num_threads = 0, int const max_offset = DEFAULT_OFFSET);

    constexpr Handle get_handle(unsigned int) {
        return Handle{};
    }

    ~linden();

    void push(Handle, std::pair<uint32_t, uint32_t> const& value);

    bool extract_top(Handle, std::pair<uint32_t, uint32_t>& retval);

    static std::string description() {
        return "linden";
    }
};

}  // namespace wrapper

#endif
