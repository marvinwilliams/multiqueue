#include <atomic>
#include <iostream>
#include <limits>
#include <type_traits>
#include <utility>

#include "cxxopts.hpp"
#include "inserter.hpp"
#include "key_generator.hpp"
#include "thread_coordination.hpp"
#include "threading.hpp"

using key_type = uint32_t;
using value_type = uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");

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
#error No supported priority queue wrapper defined!
#endif

static constexpr unsigned int default_num_operations = 1000u;
static constexpr unsigned int default_num_threads = 1u;

using UniformInserter = util::Inserter<util::InsertPolicy::Uniform>;
using UniformKeyGenerator = util::KeyGenerator<key_type, util::KeyDistribution::Uniform>;

struct Settings {
    unsigned int num_operations = default_num_operations;
    unsigned int num_threads = default_num_threads;
};

std::atomic_bool start_flag;

template <typename T, typename = void>
struct has_thread_init : std::false_type {};

template <typename T>
struct has_thread_init<T, std::void_t<decltype(std::declval<T>().init_thread(static_cast<size_t>(0)))>>
    : std::true_type {};

template <typename pq_t>
struct Task {
    static void run(thread_coordination::Context context, pq_t& pq, Settings const& settings) {
        UniformKeyGenerator inserter{context.get_id()};
        UniformKeyGenerator key_generator{context.get_id()};
        unsigned int stage = 0u;

        if constexpr (has_thread_init<pq_t>::value) {
            pq.init_thread(context.get_num_threads());
        }

        context.synchronize(stage++, [&context]() {
            std::clog << "Starting the test..." << std::flush;
            context.notify_coordinator();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        for (unsigned int i = 0; i < settings.num_operations; ++i) {
            if (inserter()) {
                key_type key = key_generator();
                pq.push({key, 0u});
            } else {
                std::pair<key_type, value_type> retval;
                pq.extract_top(retval);
            }
        }
        context.synchronize(stage++, []() { std::clog << "done" << std::endl; });
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

    cxxopts::Options options("wrapper test", "This executable tests the functionality of the wrappers");
    // clang-format off
    options.add_options()
      ("j,threads", "Specify the number of threads "
       "(default: 1)", cxxopts::value<unsigned int>(), "NUMBER")
      ("o,operations", "Specify the number of operations done by each thread "
       "(default: 1000u)", cxxopts::value<unsigned int>(), "NUMBER")
      ("h,help", "Print this help");
    // clang-format on

    Settings settings{};

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            exit(0);
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
    start_flag.store(false, std::memory_order_relaxed);
    pq_t pq(settings.num_threads);

    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task<pq_t>>(std::ref(pq), settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::clog << "All done" << std::endl;
#ifdef PQ_LINDEN
    // Avoid segfault
    pq.push({0, 0});
#endif
    return 0;
}
