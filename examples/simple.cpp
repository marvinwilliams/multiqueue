#include "multiqueue/default_configuration.hpp"
#include "multiqueue/multiqueue.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

using mq_t = typename multiqueue::MultiqueueFactory<unsigned int, unsigned int>::type;

static const unsigned int n_threads = 4;
static const size_t iterations_per_thread = 20;

std::mutex m;
std::size_t num_updates = 0;
std::size_t num_extracts = 0;

void work(mq_t& pq, unsigned int id) {
    auto handle = pq.get_handle();
    for (unsigned int i = 0; i < iterations_per_thread; ++i) {
        mq_t::value_type val = {i, id * iterations_per_thread + i};
        handle.push(val);
        {
            auto l = std::scoped_lock{m};
            std::clog << '[' << id << "] Insert (" << val.first << ", " << val.second << ")\n";
        }
        val.first = n_threads * iterations_per_thread;
        if (handle.try_update(val)) {
            auto l = std::scoped_lock{m};
            ++num_updates;
            std::clog << '[' << id << "] Update (" << val.first << ", " << val.second << ")\n";
        }
        if (handle.try_extract_top(val)) {
            auto l = std::scoped_lock{m};
            ++num_extracts;
            std::clog << '[' << id << "] Extract (" << val.first << ", " << val.second << ")\n";
        }
    }
}

int main() {
    std::vector<std::thread> threads;
    mq_t pq(n_threads, n_threads * iterations_per_thread);
    std::generate_n(std::back_inserter(threads), n_threads, [&pq, i = 0]() mutable {
        return std::thread{work, std::ref(pq), i++};
    });
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
    std::clog << "----\n";
    std::clog << "Num inserts: " << iterations_per_thread * n_threads << '\n';
    std::clog << "Num updates: " << num_updates << '\n';
    std::clog << "Num extracts: " << num_extracts << '\n';
    return 0;
}
