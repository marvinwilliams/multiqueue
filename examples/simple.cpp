#include "multiqueue/multiqueue.hpp"

#include <functional>
#include <utility>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

using mq_type = multiqueue::MultiQueue<unsigned, std::pair<unsigned, unsigned>>;

static const unsigned int n_threads = 4;
static const std::size_t iterations_per_thread = 1'000;

static void work(mq_type& pq) {
    auto handle = pq.get_handle();
    for (unsigned i = 1; i <= iterations_per_thread; ++i) {
        handle.push({i, i});
        handle.try_pop();
    }
}

int main() {
    std::vector<std::thread> threads;
    mq_type pq(4 * n_threads);
    std::generate_n(std::back_inserter(threads), n_threads, [&pq]() { return std::thread{work, std::ref(pq)}; });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
}
