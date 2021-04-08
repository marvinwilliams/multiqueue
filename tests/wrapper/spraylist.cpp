// Adapted from klsm

#include "spraylist.hpp"

#include <cstddef>
#include <iostream>
#include <utility>

extern "C" {
#include "spraylist_linden/include/random.h"
#include "spraylist_linden/intset.h"
#include "spraylist_linden/linden.h"
#include "spraylist_linden/pqueue.h"
#include "ssalloc.h"
}

__thread unsigned long *seeds;

namespace multiqueue {
namespace wrapper {

constexpr unsigned int INITIAL_SIZE = 1000000;

/** See documentation of --elasticity in spraylist/test.c. */
#define READ_ADD_REM_ELASTIC_TX (4)

static thread_local bool initialized = false;
static thread_local thread_data_t *d;

spraylist::spraylist(size_t const num_threads) {
    init_thread(num_threads);
    *levelmax = floor_log_2(INITIAL_SIZE);
    pq_ = sl_set_new();
}

spraylist::~spraylist() {
    sl_set_delete(pq_);
    delete d;
}

void spraylist::init_thread(size_t const num_threads) {
    if (!initialized) {
        ssalloc_init(num_threads);
        seeds = seed_rand();

        d = new thread_data_t;
        d->seed = rand();
        d->seed2 = rand();

        initialized = true;
    }

    d->nb_threads = num_threads;
}

void spraylist::push(std::pair<uint32_t, uint32_t> const &value) {
    sl_add_val(pq_, value.first, value.second, TRANSACTIONAL);
}

bool spraylist::extract_top(std::pair<uint32_t, uint32_t> &retval) {
    slkey_t k_ret;
    val_t v_ret;
    int ret;
    do {
        ret = spray_delete_min_key(pq_, &k_ret, &v_ret, d);
    } while (ret == 0 && k_ret != -1);
    retval.first = static_cast<uint32_t>(k_ret);
    retval.second = static_cast<uint32_t>(v_ret);
    return k_ret != -1;
}

}  // namespace wrapper
}  // namespace multiqueue
