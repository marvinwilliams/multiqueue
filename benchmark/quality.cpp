#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>

#include "cxxopts.hpp"
#include "inserter.hpp"
#include "key_generator.hpp"
#include "select_queue.hpp"
#include "thread_coordination.hpp"
#include "threading.hpp"

#ifndef NDEBUG
#error "Benchmarks must not be compiled in debug build!"
#endif

using key_type = std::uint32_t;
using value_type = std::uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");

using util::Inserter;
using util::InsertPolicy;
using util::KeyDistribution;
using util::KeyGenerator;

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static constexpr size_t default_prefill_size = 1'000'000;
static constexpr unsigned int default_num_deletions = 100'000u;
static constexpr unsigned int default_num_threads = 1u;
static constexpr InsertPolicy default_policy = InsertPolicy::Uniform;
static constexpr KeyDistribution default_key_distribution = KeyDistribution::Uniform;
static constexpr value_type default_dijkstra_increase_min = 1u;
static constexpr value_type default_dijkstra_increase_max = 100u;
static constexpr unsigned int bits_represent_thread_id = 6;
static constexpr value_type thread_id_mask =
    (static_cast<value_type>(1u) << (std::numeric_limits<value_type>::digits - bits_represent_thread_id)) - 1u;

constexpr value_type to_value(unsigned int thread_id, value_type elem_id) noexcept {
    return (static_cast<value_type>(thread_id)
            << (std::numeric_limits<value_type>::digits - bits_represent_thread_id)) |
        (elem_id & thread_id_mask);
}

constexpr unsigned int get_thread_id(value_type value) noexcept {
    return static_cast<unsigned int>(value >> (std::numeric_limits<value_type>::digits - bits_represent_thread_id));
}

constexpr value_type get_elem_id(value_type value) noexcept {
    return value & thread_id_mask;
}

struct Settings {
    size_t prefill_size = default_prefill_size;
    unsigned int num_deletions = default_num_deletions;
    unsigned int num_threads = default_num_threads;
    InsertPolicy policy = default_policy;
    KeyDistribution key_distribution = default_key_distribution;
    value_type dijkstra_increase_min = default_dijkstra_increase_min;
    value_type dijkstra_increase_max = default_dijkstra_increase_max;
};

struct insertion_log {
    clk::rep tick;
    key_type key;
};

struct deletion_log {
    clk::rep tick;
    std::optional<value_type> value;
};

std::atomic_bool start_flag;

std::vector<std::vector<insertion_log>> global_insertions;
std::vector<std::vector<deletion_log>> global_deletions;
// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

template <typename Queue>
struct Task {
    static void run(thread_coordination::Context context, Queue& pq, Settings const& settings) {
        std::variant<Inserter<InsertPolicy::Uniform>, Inserter<InsertPolicy::Split>, Inserter<InsertPolicy::Producer>,
                     Inserter<InsertPolicy::Alternating>>
            inserter;
        switch (settings.policy) {
            case (InsertPolicy::Uniform):
                inserter = Inserter<InsertPolicy::Uniform>{context.get_id()};
                break;
            case (InsertPolicy::Split):
                inserter = Inserter<InsertPolicy::Split>{context.get_id() % 2 == 0};
                break;
            case (InsertPolicy::Producer):
                inserter = Inserter<InsertPolicy::Producer>{context.get_id() == 0};
                break;
            case (InsertPolicy::Alternating):
                inserter = Inserter<InsertPolicy::Alternating>{};
                break;
        }
        std::variant<
            KeyGenerator<key_type, KeyDistribution::Uniform>, KeyGenerator<key_type, KeyDistribution::Ascending>,
            KeyGenerator<key_type, KeyDistribution::Descending>, KeyGenerator<key_type, KeyDistribution::Dijkstra>>
            key_generator;
        switch (settings.key_distribution) {
            case (KeyDistribution::Uniform):
                key_generator = KeyGenerator<key_type, KeyDistribution::Uniform>{context.get_id()};
                break;
            case (KeyDistribution::Ascending):
                key_generator = KeyGenerator<key_type, KeyDistribution::Ascending>{
                    (std::numeric_limits<key_type>::max() / context.get_num_threads()) * context.get_id()};
                break;
            case (KeyDistribution::Descending):
                key_generator = KeyGenerator<key_type, KeyDistribution::Descending>{
                    ((std::numeric_limits<key_type>::max() - 1) / context.get_num_threads()) * (context.get_id() + 1)};
                break;
            case (KeyDistribution::Dijkstra):
                key_generator = KeyGenerator<key_type, KeyDistribution::Dijkstra>{
                    settings.dijkstra_increase_min, settings.dijkstra_increase_max, context.get_id()};
                break;
        }
        std::vector<insertion_log> insertions;
        insertions.reserve(settings.prefill_size + settings.num_deletions);
        std::vector<deletion_log> deletions;
        deletions.reserve(settings.num_deletions);

        if constexpr (util::QueueTraits<Queue>::has_thread_init) {
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
                key_type const key = std::visit([](auto& g) noexcept { return g(); }, key_generator);
                value_type const value = to_value(context.get_id(), static_cast<value_type>(insertions.size()));
                auto now = std::chrono::steady_clock::now();
                // Compiler memory barrier (might flush registers, so has performance implications, see
                // https://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html)
                __asm__ __volatile__("" ::: "memory");
                pq.push({key, value});
                insertions.push_back(insertion_log{now.time_since_epoch().count(), key});
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
        std::pair<key_type, value_type> retval;
        unsigned int deletion_count = 0;
        while (deletion_count < settings.num_deletions) {
            if (std::visit([](auto& ins) noexcept { return ins(); }, inserter)) {
                key_type const key = std::visit([](auto& g) noexcept { return g(); }, key_generator);
                value_type const value = to_value(context.get_id(), static_cast<value_type>(insertions.size()));
                auto now = std::chrono::steady_clock::now();
                __asm__ __volatile__("" ::: "memory");
                pq.push({key, value});
                insertions.push_back(insertion_log{now.time_since_epoch().count(), key});
            } else {
                bool success = pq.extract_top(retval);
                __asm__ __volatile__("" ::: "memory");
                auto now = std::chrono::steady_clock::now();
                deletions.push_back(deletion_log{now.time_since_epoch().count(),
                                                 success ? std::optional<value_type>{retval.second} : std::nullopt});
                ++deletion_count;
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
    cxxopts::Options options("quality benchmark", "This executable measures the quality of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: " + std::to_string(default_prefill_size) + ")", cxxopts::value<size_t>(), "NUMBER")
      ("p,policy", "Specify the thread policy as one of \"uniform\", \"split\", \"producer\", \"alternating\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 1)", cxxopts::value<unsigned int>(), "NUMBER")
      ("o,deletions", "Specify the number of deletions done by each thread "
       "(default: " + std::to_string(default_num_deletions) + ")", cxxopts::value<unsigned int>(), "NUMBER")
      ("d,key-distribution", "Specify the key distribution as one of \"uniform\", \"dijkstra\", \"ascending\", \"descending\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
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
        if (result.count("policy") > 0) {
            std::string policy = result["policy"].as<std::string>();
            if (policy == "uniform") {
                settings.policy = InsertPolicy::Uniform;
            } else if (policy == "split") {
                settings.policy = InsertPolicy::Split;
            } else if (policy == "producer") {
                settings.policy = InsertPolicy::Producer;
            } else if (policy == "alternating") {
                settings.policy = InsertPolicy::Alternating;
            } else {
                std::cerr << "Unknown policy \"" << policy << "\"\n";
                return 1;
            }
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
        if (result.count("deletions") > 0) {
            settings.num_deletions = result["deletions"].as<unsigned int>();
        }
        if (result.count("key-distribution") > 0) {
            std::string dist = result["key-distribution"].as<std::string>();
            if (dist == "uniform") {
                settings.key_distribution = KeyDistribution::Uniform;
            } else if (dist == "ascending") {
                settings.key_distribution = KeyDistribution::Ascending;
            } else if (dist == "descending") {
                settings.key_distribution = KeyDistribution::Descending;
            } else if (dist == "dijkstra") {
                settings.key_distribution = KeyDistribution::Dijkstra;
            } else {
                std::cerr << "Unknown key distribution \"" << dist << "\"\n";
                return 1;
            }
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
    if (settings.num_threads > (1u << bits_represent_thread_id) - 1) {
        std::cerr << "Too many threads!" << std::endl;
        return 1;
    }

    using Queue = util::QueueSelector<key_type, value_type>::pq_t;
    Queue pq{settings.num_threads};

    start_flag.store(false, std::memory_order_relaxed);
    global_insertions.resize(settings.num_threads);
    global_deletions.resize(settings.num_threads);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task<Queue>>(std::ref(pq), settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::cout << settings.num_threads << '\n';
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key] : global_insertions[t]) {
            std::cout << "i " << t << ' ' << tick << ' ' << key << '\n';
        }
    }
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, val] : global_deletions[t]) {
            if (val) {
                std::cout << "d " << t << ' ' << tick << ' ' << static_cast<int>(get_thread_id(*val)) << ' '
                          << static_cast<int64_t>(get_elem_id(*val)) << '\n';
            } else {
                std::cout << "f " << t << ' ' << tick << '\n';
            }
        }
    }
    std::cout << std::flush;
    std::clog << "done" << std::endl;
    return 0;
}
