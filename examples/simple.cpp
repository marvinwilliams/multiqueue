#include "multiqueue/multiqueue.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

using mq_t = multiqueue::Multiqueue<unsigned, unsigned>;

static const unsigned int n_threads = 4;
static const size_t iterations_per_thread = 1'000;

void work(mq_t& pq) {
    auto handle = pq.get_handle();
    mq_t::value_type ret;
    for (unsigned int i = 0; i < iterations_per_thread; ++i) {
        pq.push(handle, {i, i});
        pq.try_delete_min(handle, ret);
    }
}

int main() {
    std::vector<std::thread> threads;
    mq_t pq(n_threads, 4, 8u);
    std::generate_n(std::back_inserter(threads), n_threads, [&pq]() { return std::thread{work, std::ref(pq)}; });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    return 0;
}
