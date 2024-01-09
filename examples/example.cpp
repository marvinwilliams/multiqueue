#include "multiqueue/multiqueue.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

using mq_type = multiqueue::ValueMultiQueue<unsigned, std::less<>>;

static const unsigned int n_threads = 4;
static const std::size_t iterations_per_thread = 1'000;

static void work(mq_type& pq) {
    auto handle = pq.get_handle();
    for (unsigned i = 1; i <= iterations_per_thread; ++i) {
        handle.push(i);
        handle.try_pop();
    }
}

int main() {
    std::vector<std::thread> threads;
    mq_type pq(static_cast<std::size_t>(4 * n_threads));
    std::generate_n(std::back_inserter(threads), n_threads, [&pq]() { return std::thread{work, std::ref(pq)}; });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
}
