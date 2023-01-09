#pragma once

#include <cstddef>

#ifdef MULTIQUEUE_COUNT_STATS

struct Stats {
    std::size_t num_locking_failed{0};
    std::size_t num_resets{0};
    std::size_t use_counts{0};
};

#define INCREMENT_STAT(stat) ++stats.stat
#define INCREMENT_STAT_BY(stat, n) stats.stat += static_cast<std::size_t>(n)
#define INCREMENT_STAT_IF(exp, stat) \
    do {                             \
        if (exp)                     \
            ++stats.stat;            \
    } while (0)
#define INCREMENT_STAT_BY_IF(exp, stat, n)             \
    do {                                               \
        if (exp)                                       \
            stats.stat += static_cast<std::size_t>(n); \
    } while (0)
#define INJECT_STATS_MEMBER Stats stats;

#else

#define INCREMENT_STAT(stat) \
    do {                     \
    } while (false)
#define INCREMENT_STAT_BY(stat, n) \
    do {                           \
    } while (false)
#define INCREMENT_STAT_IF(exp, stat) \
    do {                             \
    } while (false)
#define INCREMENT_STAT_BY_IF(exp, stat, n) \
    do {                                   \
    } while (false)

#define INJECT_STATS_MEMBER
#endif
