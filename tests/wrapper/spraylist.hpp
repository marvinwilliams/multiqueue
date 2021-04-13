#ifndef WRAPPER_SPRAYLIST_HPP_INCLUDED
#define WRAPPER_SPRAYLIST_HPP_INCLUDED

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

struct sl_intset;

namespace multiqueue {
namespace wrapper {

class spraylist {
    using pq_t = sl_intset;

    pq_t* pq_;

   public:
    struct Handle{};
    spraylist(size_t const num_threads);
    virtual ~spraylist();

    constexpr Handle get_handle(unsigned int) {
        return Handle{};
    }

    void init_thread(size_t const num_threads);

    void push(Handle, std::pair<uint32_t, uint32_t> const& value);
    bool extract_top(Handle, std::pair<uint32_t, uint32_t>& retval);

    static std::string description() {
        return "spraylist";
    }
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
