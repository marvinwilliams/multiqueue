#include "linden.hpp"

#include <cstddef>
#include <utility>

extern "C" {
#include "spraylist_linden/gc/gc.h"
#include "spraylist_linden/linden.h"
}

namespace wrapper {

struct linden_pq_wrapper {
    pq_t* pq;
};

linden::linden(unsigned int, int const max_offset) {
    _init_gc_subsystem();
    pq_ = new linden_pq_wrapper;
    pq_->pq = pq_init(max_offset);
}

linden::~linden() {
    // Avoid segfault
    push(Handle{}, {0u, 0u});
    pq_destroy(pq_->pq);
    delete pq_;
    _destroy_gc_subsystem();
}

void linden::push(Handle, std::pair<std::uint32_t, std::uint32_t> const& value) {
    ::insert(pq_->pq, value.first + 1, value.second);
}

bool linden::extract_top(Handle, std::pair<std::uint32_t, std::uint32_t>& retval) {
    unsigned long k_ret;
    retval.second = deletemin_key(pq_->pq, &k_ret);
    retval.first = k_ret - 1;
    return k_ret != -1;
}

}  // namespace wrapper
