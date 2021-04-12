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

#include "cxxopts.hpp"
#include "../utils/priority_queue_factory.hpp"
#include "../utils/thread_coordination.hpp"
#include "../utils/threading.hpp"

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
    std::chrono::milliseconds test_duration = 3s;
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

std::atomic_bool start_flag;
std::atomic_bool stop_flag;

std::atomic_uint64_t global_insertions = 0;
std::atomic_uint64_t global_deletions = 0;
std::atomic_uint64_t global_failed_deletions = 0;

// Used to guarantee writing of result so it can't be optimized out;
volatile key_type result_key;
volatile value_type result_value;

// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

template <InsertPolicy insert_policy, KeyDistribution key_distribution>
struct Task {
    static void run(thread_coordination::Context ctx, PriorityQueue& pq, Settings const& settings) {
#ifdef PQ_SPRAYLIST
        pq.init_thread(ctx.get_num_threads());
#endif

        unsigned int stage = 0;
        uint64_t insertions = 0;
        uint64_t deletions = 0;
        uint64_t failed_deletions = 0;

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
#ifdef PQ_IS_WRAPPER
                pq.push({key_generator(), 0});
#else
                pq.push(handle, {key_generator(), 0});
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
#ifdef PQ_IS_WRAPPER
                pq.push({key, key});
#else
                pq.push(handle, {key, key});
#endif
                ++insertions;
            } else {
#ifdef PQ_IS_WRAPPER
                if (pq.extract_top(retval)) {
#else
                if (pq.extract_top(handle, retval)) {
#endif
                    // Assign result to make sure the return value is not optimized away
                    result_key = retval.first;
                    result_value = retval.second;
                } else {
                    ++failed_deletions;
                }
                ++deletions;
            }
        }
        ctx.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        global_insertions += insertions;
        global_deletions += deletions;
        global_failed_deletions += failed_deletions;
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

    cxxopts::Options options("quality benchmark", "This executable measures the throughput of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: 1'000'000)", cxxopts::value<size_t>(), "NUMBER")
      ("p,policy", "Specify the thread policy as one of \"uniform\", \"split\", \"producer\", \"alternating\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 4)", cxxopts::value<unsigned int>(), "NUMBER")
      ("t,time", "Specify the test timeout in ms "
       "(default: 3000)", cxxopts::value<unsigned int>(), "NUMBER")
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

#ifndef NDEBUG
    std::clog << "Using debug build!\n\n";
#endif
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
    stop_flag.store(false, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    dispatch_inserter(insert_policy, key_distribution, coordinator, pq, settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    std::this_thread::sleep_for(settings.test_duration);
    stop_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::cout << "Insertions: " << global_insertions << "\nDeletions: " << global_deletions
              << "\nFailed deletions: " << global_failed_deletions << "\nOps/s: " << std::fixed << std::setprecision(1)
              << (1000.0 * static_cast<double>(global_insertions + global_deletions)) /
            static_cast<double>(settings.test_duration.count())
              << std::endl;
    std::clog << "done" << std::endl;
    return 0;
}
