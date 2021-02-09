#include <x86intrin.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>

#include "cxxopts.hpp"
#include "thread_coordination.hpp"
#include "threading.hpp"

#define PQ_SPRAYLIST

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
#elif defined PQ_MLSM
#include "klsm.hpp"
#include "multi_lsm/multi_lsm.h"
#elif defined PQ_SLSM
#include "klsm.hpp"
#include "shared_lsm/shared_lsm.h"
#else
#include "multiqueue/kv_pq.hpp"
#include "multiqueue/relaxed_kv_multiqueue.hpp"
#ifndef PQ_MQ
#define PQ_MQ
#endif
#endif

using namespace std::chrono_literals;

enum class InsertPolicy { Uniform, Split, Producer, Alternating };
enum class KeyDistribution { Uniform, Dijkstra, Ascending, Descending };

static constexpr size_t default_prefill_size = 1'000'000;
static constexpr std::chrono::milliseconds default_working_time = 1s;
static constexpr unsigned int default_num_threads = 1u;
static constexpr InsertPolicy default_policy = InsertPolicy::Uniform;
static constexpr KeyDistribution default_key_distribution = KeyDistribution::Uniform;
static constexpr uint64_t default_dijkstra_increase_min = 1u;
static constexpr uint64_t default_dijkstra_increase_max = 100u;

static constexpr unsigned int bits_represent_thread_id = 8;
static constexpr uint64_t thread_id_mask = (static_cast<uint64_t>(1u) << (64u - bits_represent_thread_id)) - 1u;

using key_type = uint64_t;
using value_type = uint64_t;

struct log_value {
    uint64_t data;

    log_value(uint64_t from_uint) noexcept : data{from_uint} {
    }
    log_value(unsigned int thread_id, uint64_t elem_id) noexcept {
        data = (static_cast<uint64_t>(thread_id) << (64u - bits_represent_thread_id)) | (elem_id & thread_id_mask);
    }
    constexpr unsigned int thread_id() const noexcept {
        return static_cast<unsigned int>(data >> (64u - bits_represent_thread_id));
    }
    constexpr uint64_t elem_id() const noexcept {
        return data & thread_id_mask;
    }
    operator uint64_t() const noexcept {
        return data;
    }
};

#ifdef PQ_MQ
template <typename Key, typename Value>
struct KVConfiguration : multiqueue::rsm::DefaultKVConfiguration<Key, Value> {
    static constexpr unsigned int Peek = 2;
    static constexpr unsigned int C = 4;
};

namespace multiqueue {
namespace rsm {

template <>
struct Sentinel<key_type> {
    static constexpr key_type get() noexcept {
        return std::numeric_limits<key_type>::max();
    }

    static constexpr bool is_sentinel(key_type v) noexcept {
        return v == get();
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif

struct Settings {
    size_t prefill_size = default_prefill_size;
    std::chrono::milliseconds working_time = default_working_time;
    unsigned int num_threads = default_num_threads;
    InsertPolicy policy = default_policy;
    KeyDistribution key_distribution = default_key_distribution;
    uint64_t dijkstra_increase_min = default_dijkstra_increase_min;
    uint64_t dijkstra_increase_max = default_dijkstra_increase_max;
};

template <InsertPolicy>
struct Inserter {
    bool insert;

    bool operator()() {
        return insert;
    }
};

template <>
struct Inserter<InsertPolicy::Uniform> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<uint64_t> dist_;
    uint64_t rand_num_;
    uint8_t bit_pos_ : 6;

   public:
    explicit Inserter(unsigned int seed = 0u) : gen_{seed}, rand_num_{0u}, bit_pos_{0u} {
    }

    bool operator()() {
        if (bit_pos_ == 0) {
            rand_num_ = dist_(gen_);
        }
        return rand_num_ & (1 << bit_pos_++);
    }
};

template <>
struct Inserter<InsertPolicy::Alternating> {
   private:
    bool insert_ = false;

   public:
    bool operator()() {
        return insert_ = !insert_, insert_;
    }
};

template <KeyDistribution>
struct KeyGenerator;

template <>
struct KeyGenerator<KeyDistribution::Uniform> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<uint64_t> dist_;

   public:
    explicit KeyGenerator(unsigned int seed = 0u) : gen_{seed} {
    }

    uint64_t operator()() {
        return dist_(gen_);
    }
};

template <>
struct KeyGenerator<KeyDistribution::Ascending> {
   private:
    uint64_t current_;

   public:
    explicit KeyGenerator(uint64_t start = 0) : current_{start} {
    }

    uint64_t operator()() {
        return current_++;
    }
};

template <>
struct KeyGenerator<KeyDistribution::Descending> {
   private:
    uint64_t current_;

   public:
    explicit KeyGenerator(uint64_t start) : current_{start} {
    }

    uint64_t operator()() {
        assert(current_ != std::numeric_limits<uint64_t>::max());
        return current_--;
    }
};

template <>
struct KeyGenerator<KeyDistribution::Dijkstra> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<uint64_t> dist_;
    uint64_t current_ = 0;

   public:
    explicit KeyGenerator(uint64_t increase_min, uint64_t increase_max, unsigned int seed = 0u)
        : gen_{seed}, dist_{increase_min, increase_max} {
    }

    uint64_t operator()() {
        return current_++ + dist_(gen_);
    }
};

struct insertion_log {
    uint64_t tick;
    key_type key;
};

struct deletion_log {
    uint64_t tick;
    log_value value;
};

std::atomic_bool start_flag;
std::atomic_bool stop_flag;
std::mutex m;
std::condition_variable cv;
bool prefill_done;

// Assume rdtsc is thread-safe and synchronized on each CPU
// Assumption false

template <typename T, typename = void>
struct has_thread_init : std::false_type {};

template <typename T>
struct has_thread_init<T, std::void_t<decltype(std::declval<T>().init_thread(static_cast<size_t>(0)))>> : std::true_type {};

template <typename pq_t>
struct Task {
    static void run(thread_coordination::Context context, pq_t& pq, Settings const& settings,
                    std::vector<std::vector<insertion_log>>& global_insertions,
                    std::vector<std::vector<deletion_log>>& global_deletions) {
      std::cout << "Starting thread " << context.get_id() + 1 << "/" << context.get_num_threads() << '\n';
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
        std::variant<KeyGenerator<KeyDistribution::Uniform>, KeyGenerator<KeyDistribution::Ascending>,
                     KeyGenerator<KeyDistribution::Descending>, KeyGenerator<KeyDistribution::Dijkstra>>
            key_generator;
        switch (settings.key_distribution) {
            case (KeyDistribution::Uniform):
                key_generator = KeyGenerator<KeyDistribution::Uniform>{context.get_id()};
                break;
            case (KeyDistribution::Ascending):
                key_generator = KeyGenerator<KeyDistribution::Ascending>{
                    (std::numeric_limits<key_type>::max() / context.get_num_threads()) * context.get_id()};
                break;
            case (KeyDistribution::Descending):
                key_generator = KeyGenerator<KeyDistribution::Descending>{
                    ((std::numeric_limits<key_type>::max() - 1) / context.get_num_threads()) * (context.get_id() + 1)};
                break;
            case (KeyDistribution::Dijkstra):
                key_generator = KeyGenerator<KeyDistribution::Dijkstra>{
                    settings.dijkstra_increase_min, settings.dijkstra_increase_max, context.get_id()};
                break;
        }
        std::vector<insertion_log> insertions;
        insertions.reserve(1'000'000);
        std::vector<deletion_log> deletions;
        deletions.reserve(1'000'000);

        if constexpr (has_thread_init<pq_t>::value) {
            pq.init_thread(context.get_num_threads());
        }

        context.synchronize(0, []() { std::clog << "Prefilling the queue..." << std::flush; });
        size_t num_insertions = settings.prefill_size / context.get_num_threads();
        if (context.is_main()) {
            num_insertions +=
                settings.prefill_size - (settings.prefill_size / context.get_num_threads()) * context.get_num_threads();
        }
        context.synchronize(1);
        for (size_t i = 0u; i < num_insertions; ++i) {
            key_type key = std::visit([](auto& g) noexcept { return g(); }, key_generator);
            log_value value{context.get_id(), insertions.size()};
            auto now = std::chrono::steady_clock::now();
            insertions.push_back(insertion_log{static_cast<uint64_t>(now.time_since_epoch().count()), key});
            pq.push({key, value});
        }
        context.synchronize(2, []() {
            std::clog << "done\nStarting the workload..." << std::flush;
            {
                auto lock = std::unique_lock(m);
                prefill_done = true;
            }
            cv.notify_one();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        std::pair<key_type, value_type> retval;
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (std::visit([](auto& i) noexcept { return i(); }, inserter)) {
                key_type key = std::visit([](auto& g) noexcept { return g(); }, key_generator);
                log_value value{context.get_id(), insertions.size()};
                /* insertions.push_back(insertion_log{_rdtsc() - local_rdtsc, key}); */
                auto now = std::chrono::steady_clock::now();
                insertions.push_back(insertion_log{static_cast<uint64_t>(now.time_since_epoch().count()), key});
                pq.push({key, value});
            } else {
                if (pq.extract_top(retval)) {
                    /* deletions.push_back(deletion_log{_rdtsc() - local_rdtsc, retval.second}); */
                    auto now = std::chrono::steady_clock::now();
                    deletions.push_back(
                        deletion_log{static_cast<uint64_t>(now.time_since_epoch().count()), retval.second});
                } else {
                    auto now = std::chrono::steady_clock::now();
                    deletions.push_back(deletion_log{static_cast<uint64_t>(now.time_since_epoch().count()),
                                                     log_value{(1u << bits_represent_thread_id) - 1u, 0u}});
                }
            }
        }
        context.synchronize(3, []() { std::clog << "done" << std::endl; });
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
    using pq_t = multiqueue::wrapper::klsm<kpq::k_lsm<key_type, value_type, 256>, false>;
#elif defined PQ_DLSM
    using pq_t = multiqueue::wrapper::klsm<kpq::dist_lsm<key_type, value_type, 256>, false>;
#elif defined PQ_MLSM
    using pq_t = multiqueue::wrapper::klsm<kpq::multi_lsm<key_type, value_type>, true>;
#elif defined PQ_SLSM
    using pq_t = multiqueue::wrapper::klsm<kpq::shared_lsm<key_type, value_type, 256>, false>;
#else
    using pq_t = multiqueue::rsm::kv_priority_queue<key_type, value_type, KVConfiguration>;
#endif

    cxxopts::Options options("quality benchmark", "This executable measures the quality of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: " + std::to_string(default_prefill_size) + ")", cxxopts::value<size_t>(), "NUMBER")
      ("p,policy", "Specify the thread policy as one of \"uniform\", \"split\", \"producer\", \"alternating\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 1)", cxxopts::value<unsigned int>(), "NUMBER")
      ("t,time", "Specify the benchmark timeout (ms)"
       "(default: 1000)", cxxopts::value<unsigned int>(), "NUMBER")
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
        if (result.count("time") > 0) {
            settings.working_time = std::chrono::milliseconds{result["time"].as<unsigned int>()};
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
        std::cerr << e.what() << '\n';
        return 1;
    }
    start_flag.store(false, std::memory_order_relaxed);
    stop_flag.store(false, std::memory_order_relaxed);
    prefill_done = false;
    pq_t pq{settings.num_threads};

    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    std::vector<std::vector<insertion_log>> global_insertions(settings.num_threads);
    std::vector<std::vector<deletion_log>> global_deletions(settings.num_threads);
    coordinator.run<Task<pq_t>>(std::ref(pq), settings, std::ref(global_insertions), std::ref(global_deletions));
    {
        auto lock = std::unique_lock(m);
        cv.wait(lock, []() { return prefill_done; });
    }
    start_flag.store(true, std::memory_order_release);
    std::this_thread::sleep_for(settings.working_time);
    stop_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::clog << "Writing logs..." << std::flush;
    {
        std::ofstream out("insertions.txt");
        for (unsigned int t = 0; t < settings.num_threads; ++t) {
            for (auto const& [time, key] : global_insertions[t]) {
                out << t << " " << time << " " << key << '\n';
            }
        }
    }
    {
        std::ofstream out("deletions.txt");
        for (unsigned int t = 0; t < settings.num_threads; ++t) {
            for (auto const& [time, val] : global_deletions[t]) {
                out << t << " " << time << " " << val.thread_id() << " " << val.elem_id() << '\n';
            }
        }
    }
    std::clog << "done" << std::endl;
    return 0;
}
