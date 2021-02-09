#ifndef SPRAYLIST_HPP
#define SPRAYLIST_HPP

// Adapted from klsm

#include <cstddef>
#include <cstdint>
#include <utility>

struct sl_intset;

namespace multiqueue {
namespace wrapper {

class spraylist {
    using pq_t = sl_intset;

    pq_t* pq_;

   public:
    spraylist(size_t const num_threads);
    virtual ~spraylist();

    void init_thread(size_t const num_threads);

    void push(std::pair<uint32_t, uint32_t> const& value);
    bool extract_top(std::pair<uint32_t, uint32_t>& retval);
};

}  // namespace wrapper
}  // namespace multiqueue

#endif
