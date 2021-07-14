#include "multiqueue/multiqueue.hpp"

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

using PriorityQueue = multiqueue::multiqueue<unsigned, unsigned>;

static constexpr unsigned int n_threads = 4;
static constexpr size_t iterations_per_thread = 1'000;

std::atomic_size_t unsuccessful = 0;

void work(PriorityQueue& pq, unsigned int id) {
    auto handle = pq.get_handle(id);
    PriorityQueue::value_type ret;
    std::size_t local_unsuccessful = 0;
    for (std::size_t i = 0; i < iterations_per_thread; ++i) {
        pq.push(handle, {i, i});
        auto success = pq.extract_top(handle, ret);
        if (!success) {
            ++local_unsuccessful;
        }
    }
    unsuccessful.fetch_add(local_unsuccessful, std::memory_order_relaxed);
}

int main() {
    std::vector<std::thread> threads;
    PriorityQueue pq(n_threads);
    std::generate_n(std::back_inserter(threads), n_threads, [&pq, i = 0]() mutable {
        return std::thread{work, std::ref(pq), i++};
    });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
    std::clog << "Unsuccessful extracts: " << unsuccessful.load() << '\n';

    return 0;
}
