#include <x86intrin.h>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
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
#include "system_config.hpp"
#include "utils/priority_queue_factory.hpp"
#include "utils/thread_coordination.hpp"
#include "utils/threading.hpp"

using key_type = std::uint32_t;
using value_type = std::uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");
using PriorityQueue = typename multiqueue::util::PriorityQueueFactory<key_type, value_type>::type;

using namespace std::chrono_literals;
using clock_type = std::chrono::steady_clock;
using tick_type = clock_type::rep;

static inline tick_type get_tick() noexcept {
    return clock_type::now().time_since_epoch().count();
}

struct Settings;

struct Inserter {
    enum class Policy : std::size_t { Uniform = 0, Split, Producer, Alternating };
    static inline std::array policy_names = {"uniform", "split", "producer", "alternating"};

    Policy policy;

    std::mt19937 gen;
    std::uniform_int_distribution<std::uint64_t> dist;
    std::uint64_t random_bits;
    std::uint8_t bit_pos : 6;

    bool insert;

    explicit Inserter(unsigned int id, Settings const& settings);

    inline bool operator()() {
        switch (policy) {
            case Policy::Uniform: {
                if (bit_pos == 0) {
                    random_bits = dist(gen);
                }
                return random_bits & (1 << bit_pos++);
            }
            case Policy::Split:
            case Policy::Producer:
                return insert;
            case Policy::Alternating:
                return insert = !insert;
            default:
                assert(false);
                return false;
        }
    }
};

struct KeyGenerator {
    enum class Distribution : std::size_t { Uniform, Dijkstra, Ascending, Descending };
    static inline std::array distribution_names = {"uniform", "dijkstra", "ascending", "descending"};

    Distribution distribution;

    std::mt19937 gen;
    std::uniform_int_distribution<key_type> dist;

    key_type current;

    explicit KeyGenerator(unsigned int id, Settings const& settings);

    inline key_type operator()() {
        switch (distribution) {
            case Distribution::Uniform:
                return dist(gen);
            case Distribution::Dijkstra:
                return current++ + dist(gen);
            case Distribution::Ascending:
                return current++;
            case Distribution::Descending:
                return current--;
            default:
                assert(false);
                return key_type{};
        }
    }
};

struct Settings {
    std::size_t prefill_size = 1'000'000;
    std::chrono::milliseconds test_duration = 3s;
    unsigned int num_threads = 4;
    Inserter::Policy insert_policy = Inserter::Policy::Uniform;
    KeyGenerator::Distribution key_distribution = KeyGenerator::Distribution::Uniform;
    value_type min_key = std::numeric_limits<value_type>::min();
    value_type max_key = std::numeric_limits<value_type>::max();
    value_type dijkstra_min_increase = 1;
    value_type dijkstra_max_increase = 100;
};

Inserter::Inserter(unsigned int id, Settings const& settings)
    : policy{settings.insert_policy}, gen{id}, random_bits{0}, bit_pos{0} {
    switch (policy) {
        case Policy::Split:
        case Policy::Alternating:
            insert = id % 2 == 0;
            break;
        case Policy::Producer:
            insert = id == 0;
            break;
        default:;
    }
}

KeyGenerator::KeyGenerator(unsigned int id, Settings const& settings)
    : distribution{settings.key_distribution}, gen(id) {
    switch (distribution) {
        case Distribution::Uniform:
            dist = std::uniform_int_distribution<key_type>(settings.min_key, settings.max_key);
            break;
        case Distribution::Ascending:
            current = settings.min_key;
            break;
        case Distribution::Descending:
            current = settings.max_key;
            break;
        case Distribution::Dijkstra:
            current = settings.min_key;
            dist =
                std::uniform_int_distribution<key_type>(settings.dijkstra_min_increase, settings.dijkstra_max_increase);
            break;
    }
}

#ifndef THROUGHPUT_ONLY

static constexpr unsigned int bits_for_thread_id = 6;
static constexpr value_type thread_id_mask =
    (static_cast<value_type>(1) << (std::numeric_limits<value_type>::digits - bits_for_thread_id)) - 1;

static constexpr value_type to_value(unsigned int thread_id, value_type elem_id) noexcept {
    return (static_cast<value_type>(thread_id) << (std::numeric_limits<value_type>::digits - bits_for_thread_id)) |
        (elem_id & thread_id_mask);
}

static constexpr unsigned int get_thread_id(value_type value) noexcept {
    return static_cast<unsigned int>(value >> (std::numeric_limits<value_type>::digits - bits_for_thread_id));
}

static constexpr value_type get_elem_id(value_type value) noexcept {
    return value & thread_id_mask;
}

struct LogEntry {
    tick_type tick;
    key_type key;
    value_type value;
};

std::vector<LogEntry>* insertions;
std::vector<LogEntry>* deletions;
std::vector<tick_type>* failed_deletions;

#else

// Used to guarantee writing of result so it can't be optimized out;
struct alignas(L1_CACHE_LINESIZE) DummyResult {
    volatile key_type key;
    volatile value_type value;
};

DummyResult* dummy_result;

#endif

std::atomic_uint64_t num_insertions;
std::atomic_uint64_t num_deletions;
std::atomic_uint64_t num_failed_deletions;

std::atomic_bool start_flag;
std::atomic_bool stop_flag;

// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

struct Task {
    static void run(thread_coordination::Context ctx, PriorityQueue& pq, Settings const& settings) {
#ifndef THROUGHPUT_ONLY
        std::vector<LogEntry> local_insertions;
        local_insertions.reserve(settings.prefill_size + 1'000'000);
        std::vector<LogEntry> local_deletions;
        local_deletions.reserve(1'000'000);
        std::vector<tick_type> local_failed_deletions;
        local_failed_deletions.reserve(1'000'000);
#endif
        uint64_t num_local_insertions = 0;
        uint64_t num_local_deletions = 0;
        uint64_t num_local_failed_deletions = 0;

#ifdef PQ_SPRAYLIST
        pq.init_thread(ctx.get_num_threads());
#endif

        unsigned int stage = 0;

        auto handle = pq.get_handle(ctx.get_id());

        auto inserter = Inserter{ctx.get_id(), settings};
        auto key_generator = KeyGenerator{ctx.get_id(), settings};

        if (settings.prefill_size > 0) {
            ctx.synchronize(stage++, []() { std::clog << "Prefilling..." << std::flush; });
            size_t thread_prefill_size = settings.prefill_size / ctx.get_num_threads();
            if (ctx.is_main()) {
                thread_prefill_size += settings.prefill_size - thread_prefill_size * ctx.get_num_threads();
            }
            for (size_t i = 0; i < thread_prefill_size; ++i) {
                key_type const key = key_generator();
#ifndef THROUGHPUT_ONLY
                value_type const value = to_value(ctx.get_id(), static_cast<value_type>(local_insertions.size()));
                auto tick = get_tick();
                // Compiler memory barrier (might flush registers, so has performance implications, see
                // https://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html)
                __asm__ __volatile__("" ::: "memory");
                pq.push(handle, {key, value});
                local_insertions.push_back(LogEntry{tick, key, value});
#else
                pq.push(handle, {key, key});
#endif
            }
            ctx.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        }
        ctx.synchronize(stage++, [&ctx]() {
            std::clog << "Starting the workload..." << std::flush;
            ctx.notify_coordinator();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
            _mm_pause();
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        std::pair<key_type, value_type> retval;
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (inserter()) {
                key_type const key = key_generator();
#ifndef THROUGHPUT_ONLY
                value_type const value = to_value(ctx.get_id(), static_cast<value_type>(local_insertions.size()));
                auto tick = get_tick();
                __asm__ __volatile__("" ::: "memory");
                pq.push(handle, {key, value});
                local_insertions.push_back(LogEntry{tick, key, value});
#else
                pq.push(handle, {key, key});
#endif
                ++num_local_insertions;
            } else {
                bool success = pq.extract_top(handle, retval);
#ifndef THROUGHPUT_ONLY
                __asm__ __volatile__("" ::: "memory");
                auto tick = get_tick();
                if (success) {
                    local_deletions.push_back(LogEntry{tick, retval.first, retval.second});
                } else {
                    local_failed_deletions.push_back(tick);
                    ++num_local_failed_deletions;
                }
#else
                if (success) {
                    dummy_result[ctx.get_id()].key = retval.first;
                    dummy_result[ctx.get_id()].value = retval.second;
                } else {
                    ++num_local_failed_deletions;
                }
#endif
                ++num_local_deletions;
            }
        }
        ctx.synchronize(stage++, []() { std::clog << "done" << std::endl; });
#ifndef THROUGHPUT_ONLY
        insertions[ctx.get_id()] = std::move(local_insertions);
        deletions[ctx.get_id()] = std::move(local_deletions);
        failed_deletions[ctx.get_id()] = std::move(local_failed_deletions);
#endif

        num_insertions += num_local_insertions;
        num_deletions += num_local_deletions;
        num_failed_deletions += num_local_failed_deletions;
    }

    static threading::thread_config get_config(thread_coordination::Context const& ctx) {
        threading::thread_config config;
        config.cpu_set.reset();
        config.cpu_set.set(ctx.get_id());
        return config;
    }
};

int main(int argc, char* argv[]) {
    Settings settings{};

    cxxopts::Options options("performance test",
                             "This executable measures and records the performance of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: 1'000'000)", cxxopts::value<size_t>(), "NUMBER")
      ("i,insert", "Specify the insert policy as one of \"uniform\", \"split\", \"producer\", \"alternating\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 4)", cxxopts::value<unsigned int>(), "NUMBER")
      ("t,time", "Specify the test timeout in ms "
       "(default: 3000)", cxxopts::value<unsigned int>(), "NUMBER")
      ("d,distribution", "Specify the key distribution as one of \"uniform\", \"dijkstra\", \"ascending\", \"descending\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("h,help", "Print this help");
    // clang-format on

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            exit(0);
        }
        if (result.count("prefill") > 0) {
            settings.prefill_size = result["prefill"].as<size_t>();
        }
        if (result.count("policy") > 0) {
            std::string policy = result["policy"].as<std::string>();
            if (policy == "uniform") {
                settings.insert_policy = Inserter::Policy::Uniform;
            } else if (policy == "split") {
                settings.insert_policy = Inserter::Policy::Split;
            } else if (policy == "producer") {
                settings.insert_policy = Inserter::Policy::Producer;
            } else if (policy == "alternating") {
                settings.insert_policy = Inserter::Policy::Alternating;
            } else {
                std::cerr << "Unknown insert policy \"" << policy << "\"\n";
                return 1;
            }
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
        if (result.count("time") > 0) {
            settings.test_duration = std::chrono::milliseconds{result["time"].as<unsigned int>()};
        }
        if (result.count("key-distribution") > 0) {
            std::string dist = result["key-distribution"].as<std::string>();
            if (dist == "uniform") {
                settings.key_distribution = KeyGenerator::Distribution::Uniform;
            } else if (dist == "ascending") {
                settings.key_distribution = KeyGenerator::Distribution::Ascending;
            } else if (dist == "descending") {
                settings.key_distribution = KeyGenerator::Distribution::Descending;
            } else if (dist == "dijkstra") {
                settings.key_distribution = KeyGenerator::Distribution::Dijkstra;
            } else {
                std::cerr << "Unknown key distribution \"" << dist << "\"\n";
                return 1;
            }
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

#ifndef THROUGHPUT_ONLY
    if (settings.num_threads > (1 << bits_for_thread_id) - 1) {
        std::cerr << "Too many threads, increase the number of thread bits!" << std::endl;
        return 1;
    }
#endif
#ifndef NDEBUG
    std::clog << "Using debug build!\n\n";
#endif
#ifdef THROUGHPUT_ONLY
    std::clog << "Measuring performance only!\n\n";
#endif
    std::clog << "Settings: \n\t"
              << "Prefill size: " << settings.prefill_size << "\n\t"
              << "Test duration: " << settings.test_duration.count() << " ms\n\t"
              << "Threads: " << settings.num_threads << "\n\t"
              << "Insert policy: " << Inserter::policy_names[static_cast<std::size_t>(settings.insert_policy)] << "\n\t"
              << "Min key: " << settings.min_key << "\n\t"
              << "Max key: " << settings.max_key << "\n\t"
              << "Key distribution: "
              << KeyGenerator::distribution_names[static_cast<std::size_t>(settings.key_distribution)] << "\n\t"
              << "Dijkstra min increase: " << settings.dijkstra_min_increase << "\n\t"
              << "Dijkstra max increase: " << settings.dijkstra_max_increase;
    std::clog << "\n\n";

    std::clog << "Using priority queue: " << PriorityQueue::description() << '\n';
    PriorityQueue pq{settings.num_threads};

#ifndef THROUGHPUT_ONLY
    insertions = new std::vector<LogEntry>[settings.num_threads];
    deletions = new std::vector<LogEntry>[settings.num_threads];
    failed_deletions = new std::vector<tick_type>[settings.num_threads];
#else
    dummy_result = new DummyResult[settings.num_threads];
#endif
    num_insertions = 0;
    num_deletions = 0;
    num_failed_deletions = 0;
    start_flag.store(false, std::memory_order_relaxed);
    stop_flag.store(false, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task>(std::ref(pq), settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    std::this_thread::sleep_for(settings.test_duration);
    stop_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::clog << "Insertions: " << num_insertions << "\nDeletions: " << num_deletions
              << "\nFailed deletions: " << num_failed_deletions << "\nOps/s: " << std::fixed << std::setprecision(1)
              << (1000.0 * static_cast<double>(num_insertions + num_deletions)) /
            static_cast<double>(settings.test_duration.count())
              << std::endl;
#ifndef THROUGHPUT_ONLY
    std::cout << settings.num_threads << '\n';
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, value] : insertions[t]) {
            std::cout << "i " << t << ' ' << tick << ' ' << key << ' ' << get_thread_id(value) << ' '
                      << get_elem_id(value) << '\n';
        }
    }

    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, value] : deletions[t]) {
            std::cout << "d " << t << ' ' << tick << ' ' << key << ' ' << get_thread_id(value) << ' '
                      << get_elem_id(value) << '\n';
        }
    }

    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto tick : failed_deletions[t]) {
            std::cout << "f " << t << ' ' << tick << '\n';
        }
    }

    std::cout << std::flush;

    delete[] insertions;
    delete[] deletions;
    delete[] failed_deletions;
#else
    delete[] dummy_result;
#endif
    return 0;
}
