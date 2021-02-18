#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "cxxopts.hpp"
#include "inserter.hpp"
#include "key_generator.hpp"
#include "thread_coordination.hpp"
#include "threading.hpp"

#if defined PQ_CAPQ1 || defined PQ_CAPQ2 || defined PQ_CAPQ3 || defined PQ_CAPQ4
#include "capq.hpp"
#elif defined PQ_LINDEN
#include "linden.hpp"
#elif defined PQ_SPRAYLIST
#include "spraylist.hpp"
#elif defined PQ_KLSM
#include "k_lsm/k_lsm.h"
#include "klsm.hpp"
#elif defined PQ_DLSM
#include "dist_lsm/dist_lsm.h"
#include "klsm.hpp"
#elif defined PQ_NBMQ
#include "multiqueue/no_buffer_pq.hpp"
#else
#error No supported priority queue defined!
#endif

using key_type = std::uint32_t;
using value_type = std::uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");

using clk = std::chrono::steady_clock;
using UniformInserter = util::Inserter<util::InsertPolicy::Uniform>;
using UniformKeyGenerator = util::KeyGenerator<key_type, util::KeyDistribution::Uniform>;

static constexpr size_t default_prefill_size = 10'000;
static constexpr unsigned int default_num_operations = 10'000u;
static constexpr unsigned int default_num_threads = 1u;
static constexpr unsigned int bits_represent_thread_id = 8;
static constexpr value_type thread_id_mask =
    (static_cast<value_type>(1u) << (std::numeric_limits<value_type>::digits - bits_represent_thread_id)) - 1u;

static constexpr value_type to_value(unsigned int thread_id, value_type elem_id) noexcept {
    return (static_cast<value_type>(thread_id)
            << (std::numeric_limits<value_type>::digits - bits_represent_thread_id)) |
        (elem_id & thread_id_mask);
}

static constexpr unsigned int get_thread_id(value_type value) noexcept {
    return static_cast<unsigned int>(value >> (std::numeric_limits<value_type>::digits - bits_represent_thread_id));
}

static constexpr value_type get_elem_id(value_type value) noexcept {
    return value & thread_id_mask;
}

struct Settings {
    size_t prefill_size = default_prefill_size;
    unsigned int num_operations = default_num_operations;
    unsigned int num_threads = default_num_threads;
};

struct log_entry {
    clk::rep tick;
    key_type key;
    value_type value;
};

std::atomic_bool start_flag;

// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

template <typename T, typename = void>
struct has_thread_init : std::false_type {};

template <typename T>
struct has_thread_init<T, std::void_t<decltype(std::declval<T>().init_thread(static_cast<size_t>(0)))>>
    : std::true_type {};

template <typename pq_t>
struct Task {
    static void run(thread_coordination::Context context, pq_t& pq, Settings const& settings,
                    std::vector<std::vector<log_entry>>& global_insertions,
                    std::vector<std::vector<log_entry>>& global_deletions) {
        UniformInserter inserter{context.get_id()};
        UniformKeyGenerator key_generator{context.get_id(), 0, thread_id_mask};

        std::vector<log_entry> insertions;
        std::vector<log_entry> deletions;

        if constexpr (has_thread_init<pq_t>::value) {
            pq.init_thread(context.get_num_threads());
        }

        unsigned int stage = 0u;

        if (settings.prefill_size > 0u) {
            context.synchronize(stage++, []() { std::clog << "Prefilling the queue..." << std::flush; });
            size_t num_insertions = settings.prefill_size / context.get_num_threads();
            if (context.is_main()) {
                num_insertions += settings.prefill_size -
                    (settings.prefill_size / context.get_num_threads()) * context.get_num_threads();
            }
            for (size_t i = 0u; i < num_insertions; ++i) {
                key_type key = key_generator();
                value_type value = to_value(context.get_id(), static_cast<value_type>(insertions.size()));
                auto now = std::chrono::steady_clock::now();
                __asm__ __volatile__("" ::: "memory");
                pq.push({key, value});
                insertions.push_back(log_entry{now.time_since_epoch().count(), key, value});
            }
            context.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        }
        context.synchronize(stage++, [&context]() {
            std::clog << "Starting the workload..." << std::flush;
            context.notify_coordinator();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        for (unsigned int i = 0; i < settings.num_operations; ++i) {
            if (inserter()) {
                key_type key = key_generator();
                value_type value = to_value(context.get_id(), static_cast<value_type>(insertions.size()));
                auto now = std::chrono::steady_clock::now();
                __asm__ __volatile__("" ::: "memory");
                pq.push({key, value});
                insertions.push_back(log_entry{now.time_since_epoch().count(), key, value});
            } else if (std::pair<key_type, value_type> retval; pq.extract_top(retval)) {
                __asm__ __volatile__("" ::: "memory");
                auto now = std::chrono::steady_clock::now();
                deletions.push_back(log_entry{now.time_since_epoch().count(), retval.first, retval.second});
            }
        }
        context.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        global_insertions[context.get_id()] = std::move(insertions);
        global_deletions[context.get_id()] = std::move(deletions);
    }

    static threading::thread_config get_config(unsigned int i) {
        threading::thread_config config;
        config.cpu_set.reset();
        config.cpu_set.set(i);
        return config;
    }
};

int main(int argc, char* argv[]) {
#if defined PQ_CAPQ1
    using pq_t = multiqueue::wrapper::capq<true, true, true>;
#elif defined PQ_CAPQ2
    using pq_t = multiqueue::wrapper::capq<true, false, true>;
#elif defined PQ_CAPQ3
    using pq_t = multiqueue::wrapper::capq<false, true, true>;
#elif defined PQ_CAPQ4
    using pq_t = multiqueue::wrapper::capq<false, false, true>;
#elif defined PQ_LINDEN
    using pq_t = multiqueue::wrapper::linden;
#elif defined PQ_SPRAYLIST
    using pq_t = multiqueue::wrapper::spraylist;
#elif defined PQ_KLSM
    using pq_t = multiqueue::wrapper::klsm<kpq::k_lsm<key_type, value_type, 256>>;
#elif defined PQ_DLSM
    using pq_t = multiqueue::wrapper::klsm<kpq::dist_lsm<key_type, value_type, 256>>;
#elif defined PQ_NBMQ
    using pq_t = multiqueue::rsm::no_buffer_pq<key_type, value_type>;
#endif

    cxxopts::Options options("Consistency test", "This executable tests the consistency of concurrent priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: " + std::to_string(default_prefill_size) + ")", cxxopts::value<size_t>(), "NUMBER")
      ("j,threads", "Specify the number of threads "
       "(default: " + std::to_string(default_num_threads) + ")", cxxopts::value<unsigned int>(), "NUMBER")
      ("o,operations", "Specify the number of operations done by each thread "
       "(default: " + std::to_string(default_num_operations) + ")", cxxopts::value<unsigned int>(), "NUMBER")
      ("h,help", "Print this help");
    // clang-format on

    Settings settings{};

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            exit(0);
        }
        if (result.count("prefill") > 0) {
            settings.prefill_size = result["prefill"].as<size_t>();
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
        if (result.count("operations") > 0) {
            settings.num_operations = result["operations"].as<unsigned int>();
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    if (settings.num_threads > (1u << bits_represent_thread_id) - 1) {
        std::cerr << "Too many threads!" << std::endl;
        return 1;
    }
    start_flag.store(false, std::memory_order_relaxed);
    pq_t pq(settings.num_threads);

    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    std::vector<std::vector<log_entry>> global_insertions(settings.num_threads);
    std::vector<std::vector<log_entry>> global_deletions(settings.num_threads);
    coordinator.run<Task<pq_t>>(std::ref(pq), settings, std::ref(global_insertions), std::ref(global_deletions));
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::clog << "Writing logs..." << std::flush;
    std::cout << settings.num_threads << '\n';
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, val] : global_insertions[t]) {
            std::cout << "i " << t << ' ' << tick << ' ' << key << ' ' << val.thread_id() << ' ' << val.elem_id()
                      << '\n';
        }
    }
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, val] : global_deletions[t]) {
            std::cout << "d " << t << ' ' << tick << ' ' << key << ' ' << val.thread_id() << ' ' << val.elem_id()
                      << '\n';
        }
    }
    std::cout << std::flush;
    std::clog << "done" << std::endl;
#ifdef PQ_LINDEN
    // Avoid segfault
    pq.push({0, 0});
#endif
    return 0;
}
