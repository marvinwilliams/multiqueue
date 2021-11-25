#include "multiqueue/default_configuration.hpp"
#include "multiqueue/multiqueue.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

using mq_t = multiqueue::Multiqueue<unsigned, unsigned, std::less<>, std::allocator<unsigned>,
                                    multiqueue::StickySelectionConfiguration>;

static const unsigned int n_threads = 16;
static const size_t iterations_per_thread = 1'000'000;

void work(mq_t& pq) {
    auto handle = pq.get_handle();
    mq_t::value_type ret;
    for (unsigned int i = 1; i <= iterations_per_thread; ++i) {
        handle.push({i, i});
        handle.try_extract_top(ret);
    }
}

int main() {
    std::vector<std::thread> threads;
    mq_t pq(n_threads);
    std::generate_n(std::back_inserter(threads), n_threads, [&pq]() { return std::thread{work, std::ref(pq)}; });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    return 0;
}
