#include <x86intrin.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cxxopts.hpp"
#include "priority_queue_factory.hpp"
#include "system_config.hpp"
#include "thread_coordination.hpp"
#include "threading.hpp"

using namespace std::chrono_literals;
using clock_type = std::chrono::steady_clock;

using weight_type = std::uint32_t;
using value_type = std::uint32_t;

using PriorityQueue = typename util::PriorityQueueFactory<value_type, weight_type>::type;

static constexpr auto retries = 400;

struct Settings {
    std::filesystem::path instance_file;
    std::filesystem::path solution_file;
    unsigned int num_threads = 4;
};

struct KnapsackInstance {
    struct Item {
        weight_type weight;
        value_type value;
    };
    std::vector<Item> items;
    weight_type max_capacity;
};

std::vector<std::vector<KnapsackInstance::Item>> prefix_sums;

static value_type get_upper_bound(KnapsackInstance const& instance, unsigned int index, weight_type used_capacity) {
    assert(index < items.size());
    auto last_to_include =
        std::lower_bound(prefix_sums[index].begin(), prefix_sums[index].end(),
                         KnapsackInstance::Item{instance.max_capacity - used_capacity, 0},
                         [&](auto const& lhs, auto const& rhs) { return lhs.weight < rhs.weight; });
    return last_to_include == prefix_sums[index].end() ? prefix_sums[index].back().value
                                                       : last_to_include->value;
}

alignas(2 * L1_CACHE_LINESIZE) static std::atomic<value_type> lower_bound;

static constexpr unsigned int bits_for_index = 10;
static constexpr weight_type weight_mask =
    (static_cast<weight_type>(1) << (std::numeric_limits<weight_type>::digits - bits_for_index)) - 1;

static constexpr weight_type to_node(unsigned int element_index, weight_type weight) noexcept {
    return (static_cast<value_type>(element_index) << (std::numeric_limits<weight_type>::digits - bits_for_index)) |
        (weight & weight_mask);
}

static constexpr unsigned int get_element_index(weight_type node) noexcept {
    return static_cast<unsigned int>(node >> (std::numeric_limits<weight_type>::digits - bits_for_index));
}

static constexpr value_type get_weight(weight_type node) noexcept {
    return node & weight_mask;
}

static std::atomic_size_t num_processed_nodes;

struct IdleState {
    alignas(2 * L1_CACHE_LINESIZE) std::atomic_uint state;
};

alignas(2 * L1_CACHE_LINESIZE) static std::atomic_size_t idle_counter;
static IdleState* idle_state;

std::atomic_bool start_flag;

static inline bool idle(unsigned int id, unsigned int num_threads) {
    idle_state[id].state.store(2, std::memory_order_relaxed);
    idle_counter.fetch_add(1, std::memory_order_release);
    while (true) {
        if (idle_counter.load(std::memory_order_relaxed) == 2 * num_threads) {
            return true;
        }
        if (idle_state[id].state.load(std::memory_order_relaxed) == 0) {
            return false;
        }
        std::this_thread::yield();
    }
}

struct Task {
    static void run(thread_coordination::Context ctx, PriorityQueue& pq, KnapsackInstance const& instance) {
#ifdef PQ_SPRAYLIST
        pq.init_thread(ctx.get_num_threads());
#endif
        unsigned int stage = 0;

        auto handle = pq.get_handle(ctx.get_id());

        if (ctx.is_main()) {
            pq.push(handle, {std::numeric_limits<value_type>::max(), to_node(0, 0)});
        }

        std::size_t num_local_processed_nodes = 0;

        ctx.synchronize(stage++, [&ctx]() {
            std::clog << "Calculating knapsack value...\n" << std::flush;
            ctx.notify_coordinator();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
            _mm_pause();
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        std::pair<value_type, weight_type> retval;
        while (true) {
            if (pq.extract_top(handle, retval)) {
            found:
                auto const index = get_element_index(retval.second);
                auto const used_capacity = get_weight(retval.second);
                auto const value = std::numeric_limits<value_type>::max() - retval.first;
                if (value + get_upper_bound(instance, index, used_capacity) <=
                    lower_bound.load(std::memory_order_relaxed)) {
                    continue;
                }
                ++num_local_processed_nodes;
                bool inserted = false;
                if (instance.items[index].weight <= instance.max_capacity - used_capacity) {
                    auto const value_with_next = value + instance.items[index].value;
                    auto const capacity_with_next = used_capacity + instance.items[index].weight;
                    auto current_lower_bound = lower_bound.load(std::memory_order_relaxed);
                    while (value_with_next > current_lower_bound &&
                           !lower_bound.compare_exchange_weak(current_lower_bound, value_with_next)) {
                    }
                    if (index + 1 < instance.items.size() && value_with_next + get_upper_bound(instance, index + 1, capacity_with_next) >
                        lower_bound.load(std::memory_order_relaxed)) {
                        pq.push(handle,
                                {std::numeric_limits<value_type>::max() - value_with_next,
                                 to_node(index + 1, capacity_with_next)});
                        inserted = true;
                    }
                }
                if (index + 1 < instance.items.size() && value + get_upper_bound(instance, index + 1, used_capacity) >
                    lower_bound.load(std::memory_order_relaxed)) {
                    pq.push(handle, {std::numeric_limits<value_type>::max() - value, to_node(index + 1, used_capacity)});
                    inserted = true;
                }
                if (inserted) {
                    if (idle_counter.load(std::memory_order_acquire) > 0) {
                        for (std::size_t i = 0; i < ctx.get_num_threads(); ++i) {
                            if (i == ctx.get_id()) {
                                continue;
                            }
                            unsigned int thread_state = 2;
                            while (!idle_state[i].state.compare_exchange_weak(
                                       thread_state, 3, std::memory_order_acq_rel, std::memory_order_acquire) &&
                                   thread_state != 0 && thread_state != 3) {
                                thread_state = 2;
                                std::this_thread::yield();
                            }
                            if (thread_state == 2) {
                                idle_counter.fetch_sub(2, std::memory_order_relaxed);
                                idle_state[i].state.store(0, std::memory_order_release);
                            }
                        }
                    }
                }
            } else {
                for (std::size_t i = 0; i < retries; ++i) {
                    if (pq.extract_top(handle, retval)) {
                        goto found;
                    }
                    std::this_thread::yield();
                }
                idle_state[ctx.get_id()].state.store(1, std::memory_order_relaxed);
                idle_counter.fetch_add(1, std::memory_order_release);
#ifdef PQ_IS_WRAPPER
                if (pq.extract_top(handle, retval)) {
#else
                if (pq.extract_from_partition(handle, retval)) {
#endif
                    idle_counter.fetch_sub(1, std::memory_order_relaxed);
                    idle_state[ctx.get_id()].state.store(0, std::memory_order_release);
                    goto found;
                }

                if (idle(ctx.get_id(), ctx.get_num_threads())) {
                    break;
                } else {
                    continue;
                }
            }
        }
        num_processed_nodes += num_local_processed_nodes;
    }

    static threading::thread_config get_config(thread_coordination::Context const& ctx) {
        threading::thread_config config;
        config.cpu_set.reset();
        config.cpu_set.set(ctx.get_id());
        return config;
    }
};

static KnapsackInstance read_problem(Settings const& settings) {
    std::ifstream file_stream{settings.instance_file};
    if (!file_stream) {
        throw std::runtime_error{"Could not open knapsack file"};
    }
    size_t n;
    file_stream >> n;
    if (!file_stream || file_stream.eof()) {
        throw std::runtime_error{"Error reading knapsack file"};
    }

    KnapsackInstance instance;
    instance.items.reserve(n);
    file_stream >> instance.max_capacity;
    if (!file_stream || (n > 0 && file_stream.eof())) {
        throw std::runtime_error{"Error reading knapsack file"};
    }

    for (size_t i = 0; i < n; ++i) {
        if (!file_stream || file_stream.eof()) {
            throw std::runtime_error{"Error reading knapsack file"};
        }
        auto item = KnapsackInstance::Item{};
        file_stream >> item.value;
        if (!file_stream || file_stream.eof()) {
            throw std::runtime_error{"Error reading knapsack file"};
        }
        file_stream >> item.weight;
        if (!file_stream) {
            throw std::runtime_error{"Error reading knapsack file"};
        }
        instance.items.push_back(item);
    }
    return instance;
}

int main(int argc, char* argv[]) {
    Settings settings{};

    cxxopts::Options options(
        "Knapsack benchmark",
        "This executable measures and records the performance of relaxed priority queues in the knapsack problem");
    // clang-format off
    options.add_options()
      ("j,threads", "Specify the number of threads "
       "(default: 4)", cxxopts::value<unsigned int>(), "NUMBER")
      ("f,file", "The input graph", cxxopts::value<std::filesystem::path>(settings.instance_file)->default_value("knapsack.kp"), "PATH")
      ("h,help", "Print this help");
    // clang-format on

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            return 0;
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

#ifndef NDEBUG
    std::clog << "Using debug build!\n\n";
#endif
    std::clog << "Settings: \n\t"
              << "Threads: " << settings.num_threads << "\n\t"
              << "Instance file: " << settings.instance_file.string() << "\n\t";
    std::clog << "\n\n";

    std::clog << "Using priority queue: " << PriorityQueue::description() << '\n';
    KnapsackInstance instance;
    std::clog << "Reading problem..." << std::flush;
    try {
        instance = read_problem(settings);
    } catch (std::runtime_error const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    std::sort(instance.items.begin(), instance.items.end(), [](auto const& lhs, auto const& rhs) {
        return (static_cast<double>(lhs.value) / static_cast<double>(lhs.weight)) >
            (static_cast<double>(rhs.value) / static_cast<double>(rhs.weight));
    });
    prefix_sums.resize(instance.items.size());
    for (std::size_t i = 0; i < instance.items.size(); ++i) {
        std::partial_sum(
            instance.items.begin() + static_cast<long>(i), instance.items.end(), std::back_inserter(prefix_sums[i]),
            [](auto const& lhs, auto const& rhs) {
                return KnapsackInstance::Item{lhs.weight + rhs.weight, lhs.value + rhs.value};
            });
    }
    lower_bound = 0;
    std::clog << "done\n";

    std::clog << instance.items.size() << ' ' << instance.max_capacity << '\n';
    idle_counter = 0;
    idle_state = new IdleState[settings.num_threads]();
    num_processed_nodes = 0;
    PriorityQueue pq{settings.num_threads};
    start_flag.store(false, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task>(std::ref(pq), instance);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    auto start_tick = clock_type::now();
    __asm__ __volatile__("" ::: "memory");
    coordinator.join();
    __asm__ __volatile__("" ::: "memory");
    auto end_tick = clock_type::now();
    std::clog << "Done\n";
    std::clog << "Solution value: " << lower_bound << '\n';
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end_tick - start_tick).count() << ' '
              << num_processed_nodes << '\n';

    delete[] idle_state;
    return 0;
}
