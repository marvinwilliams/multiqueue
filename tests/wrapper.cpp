#include <x86intrin.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>

#include "cxxopts.hpp"
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
#else
#error No supported priority queue defined!
#endif

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static constexpr size_t default_prefill_size = 1'000'000;
static constexpr std::chrono::milliseconds default_working_time = 1s;
static constexpr unsigned int default_num_threads = 1u;
static constexpr unsigned int bits_represent_thread_id = 8;
static constexpr uint32_t thread_id_mask = (static_cast<uint32_t>(1u) << (32u - bits_represent_thread_id)) - 1u;

using key_type = uint32_t;
using value_type = uint32_t;

struct log_value {
    uint32_t data;

    log_value(uint32_t from_uint) noexcept : data{from_uint} {
    }
    log_value(unsigned int thread_id, uint32_t elem_id) noexcept {
        data = (static_cast<uint32_t>(thread_id) << (32u - bits_represent_thread_id)) | (elem_id & thread_id_mask);
    }
    constexpr unsigned int thread_id() const noexcept {
        return static_cast<unsigned int>(data >> (32u - bits_represent_thread_id));
    }
    constexpr uint32_t elem_id() const noexcept {
        return data & thread_id_mask;
    }
    operator uint32_t() const noexcept {
        return data;
    }
};

struct Settings {
    size_t prefill_size = default_prefill_size;
    std::chrono::milliseconds working_time = default_working_time;
    unsigned int num_threads = default_num_threads;
};

struct UniformInserter {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<uint64_t> dist_;
    uint64_t rand_num_;
    uint8_t bit_pos_ : 6;

   public:
    explicit UniformInserter(unsigned int seed = 0u) : gen_{seed}, rand_num_{0u}, bit_pos_{0u} {
    }

    bool operator()() {
        if (bit_pos_ == 0) {
            rand_num_ = dist_(gen_);
        }
        return rand_num_ & (1 << bit_pos_++);
    }
};

struct UniformKeyGenerator {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<uint32_t> dist_{0, thread_id_mask};

   public:
    explicit UniformKeyGenerator(unsigned int seed = 0u) : gen_{seed} {
    }

    uint32_t operator()() {
        return dist_(gen_);
    }
};

struct log_entry {
    clk::rep tick;
    key_type key;
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
struct has_thread_init<T, std::void_t<decltype(std::declval<T>().init_thread(static_cast<size_t>(0)))>>
    : std::true_type {};

template <typename pq_t>
struct Task {
    static void run(thread_coordination::Context context, pq_t& pq, Settings const& settings,
                    std::vector<std::vector<log_entry>>& global_insertions,
                    std::vector<std::vector<log_entry>>& global_deletions) {
        UniformInserter inserter{context.get_id()};
        UniformKeyGenerator key_generator{context.get_id()};
        unsigned int stage = 0u;

        std::vector<log_entry> insertions;
        std::vector<log_entry> deletions;

        if constexpr (has_thread_init<pq_t>::value) {
            pq.init_thread(context.get_num_threads());
        }

        if (settings.prefill_size > 0u) {
            context.synchronize(stage++, []() { std::clog << "Prefilling the queue..." << std::flush; });
            size_t num_insertions = settings.prefill_size / context.get_num_threads();
            if (context.is_main()) {
                num_insertions += settings.prefill_size -
                    (settings.prefill_size / context.get_num_threads()) * context.get_num_threads();
            }
            for (size_t i = 0u; i < num_insertions; ++i) {
                key_type key = key_generator();
                log_value value{context.get_id(), static_cast<uint32_t>(insertions.size())};
                auto now = std::chrono::steady_clock::now();
                insertions.push_back(log_entry{now.time_since_epoch().count(), key, value});
                pq.push({key, value});
            }
            context.synchronize(stage++, []() { std::clog << "done" << std::endl; });
        }
        context.synchronize(stage++, []() {
            std::clog << "Starting the workload..." << std::flush;
            {
                auto lock = std::unique_lock(m);
                prefill_done = true;
            }
            cv.notify_one();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (inserter()) {
                key_type key = key_generator();
                log_value value{context.get_id(), static_cast<uint32_t>(insertions.size())};
                auto now = std::chrono::steady_clock::now();
                insertions.push_back(log_entry{now.time_since_epoch().count(), key, value});
                pq.push({key, value});
            } else if (std::pair<key_type, value_type> retval; pq.extract_top(retval)) {
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
#endif

    cxxopts::Options options("quality benchmark", "This executable measures the quality of relaxed priority queues");
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: " + std::to_string(default_prefill_size) + ")", cxxopts::value<size_t>(), "NUMBER")
      ("j,threads", "Specify the number of threads "
       "(default: 1)", cxxopts::value<unsigned int>(), "NUMBER")
      ("t,time", "Specify the benchmark timeout (ms)"
       "(default: 1000)", cxxopts::value<unsigned int>(), "NUMBER")
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
        if (result.count("time") > 0) {
            settings.working_time = std::chrono::milliseconds{result["time"].as<unsigned int>()};
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    start_flag.store(false, std::memory_order_relaxed);
    stop_flag.store(false, std::memory_order_relaxed);
    prefill_done = false;
    pq_t pq(settings.num_threads);

    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    std::vector<std::vector<log_entry>> global_insertions(settings.num_threads);
    std::vector<std::vector<log_entry>> global_deletions(settings.num_threads);
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
    std::cout << settings.num_threads << '\n';
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [time, key, val] : global_insertions[t]) {
            std::cout << "i " << t << ' ' << time << ' ' << key << ' ' << val.thread_id() << ' ' << val.elem_id()
                      << '\n';
        }
    }
    for (unsigned int t = 0; t < settings.num_threads; ++t) {
        for (auto const& [time, key, val] : global_deletions[t]) {
            std::cout << "d " << t << ' ' << time << ' ' << key << ' ' << val.thread_id() << ' ' << val.elem_id()
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
