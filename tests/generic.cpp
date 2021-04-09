#include <x86intrin.h>
#include <array>
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
#include <vector>

#include "cxxopts.hpp"
#include "utils/priority_queue_factory.hpp"
#include "utils/thread_coordination.hpp"
#include "utils/threading.hpp"

using key_type = std::uint32_t;
using value_type = std::uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");
using PriorityQueue = typename multiqueue::util::PriorityQueueFactory<key_type, value_type>::type;

using namespace std::chrono_literals;

class Timer {
   public:
    using clock = std::chrono::steady_clock;

   private:
    clock::time_point start_;

   public:
    Timer() noexcept : start_{clock::now()} {
    }

    void reset() noexcept {
        start_ = clock::now();
    }

    clock::duration elapsed_time() noexcept {
        return clock::now() - start_;
    }

    static clock::rep ticks_since_epoch() noexcept {
        return clock::now().time_since_epoch().count();
    }
};

struct Settings {
    std::size_t prefill_size = 1'000'000;
    std::chrono::milliseconds test_duration = 1s;
    unsigned int num_threads = 4;
    value_type key_min = std::numeric_limits<value_type>::min();
    value_type key_max = std::numeric_limits<value_type>::max();
    value_type dijkstra_increase_min = 1;
    value_type dijkstra_increase_max = 100;
};

enum class InsertPolicy { Uniform, Split, Producer, Alternating };
std::array policy_names = {"uniform", "split", "producer", "alternating"};

template <InsertPolicy>
struct Inserter;

template <>
struct Inserter<InsertPolicy::Uniform> {
   private:
    std::default_random_engine gen_;
    std::uniform_int_distribution<std::uint64_t> dist_;
    std::uint64_t rand_num_;
    std::uint8_t bit_pos_ : 6;

   public:
    explicit Inserter(thread_coordination::Context const& ctx, Settings const&)
        : gen_{ctx.get_id()}, rand_num_{0}, bit_pos_{0} {
    }

    inline bool operator()() {
        if (bit_pos_ == 0) {
            rand_num_ = dist_(gen_);
        }
        return rand_num_ & (1 << bit_pos_++);
    }
};

template <>
struct Inserter<InsertPolicy::Split> {
   private:
    bool insert_;

   public:
    explicit Inserter(thread_coordination::Context const& ctx, Settings const&) : insert_{ctx.get_id() % 2 == 0} {
    }

    inline bool operator()() {
        return insert_;
    }
};

template <>
struct Inserter<InsertPolicy::Producer> {
   private:
    bool insert_;

   public:
    explicit Inserter(thread_coordination::Context const& ctx, Settings const&) : insert_{ctx.get_id() == 0} {
    }

    inline bool operator()() {
        return insert_;
    }
};

template <>
struct Inserter<InsertPolicy::Alternating> {
   private:
    bool insert_;

   public:
    explicit Inserter(thread_coordination::Context const& ctx, Settings const&) : insert_{ctx.get_id() % 2 == 0} {
    }

    inline bool operator()() noexcept {
        return insert_ = !insert_;
    }
};

enum class KeyDistribution : std::size_t { Uniform = 0, Dijkstra, Ascending, Descending };
std::array distribution_names = {"uniform", "dijkstra", "ascending", "descending"};

template <typename T, KeyDistribution>
struct KeyGenerator;

template <typename T>
struct KeyGenerator<T, KeyDistribution::Uniform> {
   private:
    std::default_random_engine gen_;
    std::uniform_int_distribution<T> dist_;

   public:
    explicit KeyGenerator(thread_coordination::Context const& ctx, Settings const& settings)
        : gen_{ctx.get_id()}, dist_{settings.key_min, settings.key_max} {
    }

    inline T operator()() {
        return dist_(gen_);
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Ascending> {
   private:
    T current_;

   public:
    explicit KeyGenerator(thread_coordination::Context const&, Settings const& settings) : current_{settings.key_min} {
    }

    inline T operator()() noexcept {
        return current_++;
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Descending> {
   private:
    T current_;

   public:
    explicit KeyGenerator(thread_coordination::Context const&, Settings const& settings) : current_{settings.key_max} {
    }

    inline T operator()() {
        return current_ > 0 ? current_-- : 0;
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Dijkstra> {
   private:
    std::default_random_engine gen_;
    std::uniform_int_distribution<T> dist_;
    T current_ = 0;

   public:
    explicit KeyGenerator(thread_coordination::Context const& ctx, Settings const& settings)
        : gen_{ctx.get_id()}, dist_{settings.dijkstra_increase_min, settings.dijkstra_increase_max} {
    }

    inline T operator()() {
        return current_++ + dist_(gen_);
    }
};

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
    Timer::clock::rep tick;
    key_type key;
    value_type value;
};

std::vector<std::vector<LogEntry>> insertions;
std::vector<std::vector<LogEntry>> deletions;
std::vector<std::vector<Timer::clock::rep>> failed_deletions;

std::atomic_bool start_flag;
std::atomic_bool stop_flag;

// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

template <InsertPolicy insert_policy, KeyDistribution key_distribution>
struct Task {
  template <typename PQ = PriorityQueue>
    static void run(thread_coordination::Context ctx, PriorityQueue& pq, Settings const& settings) {
        std::vector<LogEntry> local_insertions;
        local_insertions.reserve(settings.prefill_size + 1'000'000);
        std::vector<LogEntry> local_deletions;
        local_deletions.reserve(1'000'000);
        std::vector<Timer::clock::rep> local_failed_deletions;
        local_failed_deletions.reserve(1'000'000);

        /* if constexpr (util::PriorityQueueTraits<PQ>::has_thread_init) { */
#ifdef PQ_SPRAYLIST
            pq.init_thread(ctx.get_num_threads());
#endif
        /* } */

        unsigned int stage = 0;

#ifndef PQ_IS_WRAPPER
        auto handle = pq.get_handle(ctx.get_id());
#endif

        auto inserter = Inserter<insert_policy>{ctx, settings};
        auto key_generator = KeyGenerator<key_type, key_distribution>{ctx, settings};

        if (settings.prefill_size > 0) {
            ctx.synchronize(stage++, []() { std::clog << "Prefilling..." << std::flush; });
            size_t num_insertions = settings.prefill_size / ctx.get_num_threads();
            if (ctx.is_main()) {
                num_insertions += settings.prefill_size - num_insertions * ctx.get_num_threads();
            }
            for (size_t i = 0; i < num_insertions; ++i) {
                key_type const key = key_generator();
                value_type const value = to_value(ctx.get_id(), static_cast<value_type>(local_insertions.size()));
                auto tick = Timer::ticks_since_epoch();
                // Compiler memory barrier (might flush registers, so has performance implications, see
                // https://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html)
                __asm__ __volatile__("" ::: "memory");
#ifdef PQ_IS_WRAPPER
                pq.push({key, value});
#else
                pq.push(handle, {key, value});
#endif
                local_insertions.push_back(LogEntry{tick, key, value});
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
                value_type const value = to_value(ctx.get_id(), static_cast<value_type>(local_insertions.size()));
                auto tick = Timer::ticks_since_epoch();
                __asm__ __volatile__("" ::: "memory");
#ifdef PQ_IS_WRAPPER
                pq.push({key, value});
#else
                pq.push(handle, {key, value});
#endif
                local_insertions.push_back(LogEntry{tick, key, value});
            } else {
#ifdef PQ_IS_WRAPPER
                bool success = pq.extract_top(retval);
#else
                bool success = pq.extract_top(handle, retval);
#endif
                __asm__ __volatile__("" ::: "memory");
                auto tick = Timer::ticks_since_epoch();
                if (success) {
                    local_deletions.push_back(LogEntry{tick, retval.first, retval.second});
                } else {
                    local_failed_deletions.push_back(tick);
                }
            }
        }
        ctx.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        insertions[ctx.get_id()] = std::move(local_insertions);
        deletions[ctx.get_id()] = std::move(local_deletions);
        failed_deletions[ctx.get_id()] = std::move(local_failed_deletions);
    }

    static threading::thread_config get_config(thread_coordination::Context const& ctx) {
        threading::thread_config config;
        config.cpu_set.reset();
        config.cpu_set.set(ctx.get_id());
        return config;
    }
};

template <InsertPolicy insert_policy>
void dispatch_key_distribution(KeyDistribution key_distribution, thread_coordination::ThreadCoordinator& coordinator,
                               PriorityQueue& pq, Settings const& settings) {
    switch (key_distribution) {
        case KeyDistribution::Uniform:
            coordinator.run<Task<insert_policy, KeyDistribution::Uniform>>(std::ref(pq), settings);
            break;
        case KeyDistribution::Ascending:
            coordinator.run<Task<insert_policy, KeyDistribution::Ascending>>(std::ref(pq), settings);
            break;
        case KeyDistribution::Descending:
            coordinator.run<Task<insert_policy, KeyDistribution::Descending>>(std::ref(pq), settings);
            break;
        case KeyDistribution::Dijkstra:
            coordinator.run<Task<insert_policy, KeyDistribution::Dijkstra>>(std::ref(pq), settings);
            break;
    }
}

void dispatch_inserter(InsertPolicy insert_policy, KeyDistribution key_distribution,
                       thread_coordination::ThreadCoordinator& coordinator, PriorityQueue& pq,
                       Settings const& settings) {
    switch (insert_policy) {
        case InsertPolicy::Uniform:
            dispatch_key_distribution<InsertPolicy::Uniform>(key_distribution, coordinator, pq, settings);
            break;
        case InsertPolicy::Split:
            dispatch_key_distribution<InsertPolicy::Split>(key_distribution, coordinator, pq, settings);
            break;
        case InsertPolicy::Alternating:
            dispatch_key_distribution<InsertPolicy::Alternating>(key_distribution, coordinator, pq, settings);
            break;
        case InsertPolicy::Producer:
            dispatch_key_distribution<InsertPolicy::Producer>(key_distribution, coordinator, pq, settings);
            break;
    }
}

int main(int argc, char* argv[]) {
    Settings settings{};

    InsertPolicy insert_policy = InsertPolicy::Uniform;
    KeyDistribution key_distribution = KeyDistribution::Uniform;

    cxxopts::Options options("quality benchmark", "This executable records the behaviour of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: 1'000'000)", cxxopts::value<size_t>(), "NUMBER")
      ("p,policy", "Specify the thread policy as one of \"uniform\", \"split\", \"producer\", \"alternating\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 4)", cxxopts::value<unsigned int>(), "NUMBER")
      ("t,time", "Specify the test timeout in ms "
       "(default: 1000)", cxxopts::value<unsigned int>(), "NUMBER")
      ("d,key-distribution", "Specify the key distribution as one of \"uniform\", \"dijkstra\", \"ascending\", \"descending\" "
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
                insert_policy = InsertPolicy::Uniform;
            } else if (policy == "split") {
                insert_policy = InsertPolicy::Split;
            } else if (policy == "producer") {
                insert_policy = InsertPolicy::Producer;
            } else if (policy == "alternating") {
                insert_policy = InsertPolicy::Alternating;
            } else {
                std::cerr << "Unknown policy \"" << policy << "\"\n";
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
                key_distribution = KeyDistribution::Uniform;
            } else if (dist == "ascending") {
                key_distribution = KeyDistribution::Ascending;
            } else if (dist == "descending") {
                key_distribution = KeyDistribution::Descending;
            } else if (dist == "dijkstra") {
                key_distribution = KeyDistribution::Dijkstra;
            } else {
                std::cerr << "Unknown key distribution \"" << dist << "\"\n";
                return 1;
            }
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
    if (settings.num_threads > (1 << bits_for_thread_id) - 1) {
        std::cerr << "Too many threads!" << std::endl;
        return 1;
    }
    std::clog << "Settings: \n\t"
              << "Prefill size: " << settings.prefill_size << "\n\t"
              << "Test duration: " << settings.test_duration.count() << " ms\n\t"
              << "Threads: " << settings.num_threads << "\n\t"
              << "Insert policy: " << policy_names[static_cast<std::size_t>(insert_policy)] << "\n\t"
              << "Key distribution: " << distribution_names[static_cast<std::size_t>(key_distribution)] << "\n\t"
              << "Dijkstra min/max: " << settings.dijkstra_increase_min << '/' << settings.dijkstra_increase_max
              << '\n';
    std::clog << '\n';

    std::clog << "Using priority queue: " << PriorityQueue::description() << '\n';
    PriorityQueue pq{settings.num_threads};

    start_flag.store(false, std::memory_order_relaxed);
    insertions.resize(settings.num_threads);
    deletions.resize(settings.num_threads);
    failed_deletions.resize(settings.num_threads);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    dispatch_inserter(insert_policy, key_distribution, coordinator, pq, settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    std::this_thread::sleep_for(settings.test_duration);
    stop_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::cout << settings.num_threads << '\n';
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, value] : insertions[t]) {
            std::cout << "i " << t << ' ' << tick << ' ' << key << ' ' << value << '\n';
        }
    }

    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [tick, key, value] : deletions[t]) {
            std::cout << "d " << t << ' ' << tick << ' ' << key << ' ' << value << ' ' << get_thread_id(value) << ' '
                      << get_elem_id(value) << '\n';
        }
    }

    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto tick : failed_deletions[t]) {
            std::cout << "f " << t << ' ' << tick << '\n';
        }
    }

    std::cout << std::flush;
    std::clog << "done" << std::endl;
    return 0;
}
