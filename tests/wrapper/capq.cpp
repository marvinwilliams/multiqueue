#include "capq.hpp"

extern "C" {
#include "capq/capq.h"
#include "capq/gc/gc.h"
}

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

__thread ptst_t* ptst;

namespace multiqueue {
namespace wrapper {

struct capq_wrapper_type {
    char pad1[64 - sizeof(CAPQ*)];
    CAPQ* pq;
    char pad2[64];
};

template <bool remove_min_relax, bool put_relax, bool catree_adapt>
capq<remove_min_relax, put_relax, catree_adapt>::capq(unsigned int) {
    _init_gc_subsystem();
    /* init_thread(1); */
    pq_ = new capq_wrapper_type;
    pq_->pq = capq_new();
}

template <bool remove_min_relax, bool put_relax, bool catree_adapt>
void capq<remove_min_relax, put_relax, catree_adapt>::push(Handle, std::pair<uint32_t, uint32_t> const& value) {
    capq_put_param(pq_->pq, static_cast<unsigned long>(value.first), static_cast<unsigned long>(value.second),
                   catree_adapt);
}

template <bool remove_min_relax, bool put_relax, bool catree_adapt>
bool capq<remove_min_relax, put_relax, catree_adapt>::extract_top(Handle, std::pair<uint32_t, uint32_t>& retval) {
    unsigned long key_write_back;
    retval.second = static_cast<uint32_t>(
        capq_remove_min_param(pq_->pq, &key_write_back, remove_min_relax, put_relax, catree_adapt));
    retval.first = static_cast<uint32_t>(key_write_back);
    return key_write_back != std::numeric_limits<unsigned long>::max();
}

template class capq<true, true, true>;
template class capq<true, false, true>;
template class capq<false, true, true>;
template class capq<false, false, true>;

}  // namespace wrapper
}  // namespace multiqueue
