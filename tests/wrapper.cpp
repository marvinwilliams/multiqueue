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
#include "select_queue.hpp"

using key_type = uint32_t;
using value_type = uint32_t;
static_assert(std::is_unsigned_v<value_type>, "Value type must be unsigned");

static constexpr unsigned int default_num_operations = 1000u;
static constexpr unsigned int default_num_threads = 1u;

using UniformInserter = util::Inserter<util::InsertPolicy::Uniform>;
using UniformKeyGenerator = util::KeyGenerator<key_type, util::KeyDistribution::Uniform>;

struct Settings {
    unsigned int num_operations = default_num_operations;
    unsigned int num_threads = default_num_threads;
};

std::atomic_bool start_flag;

template <typename Queue>
struct Task {
    static void run(thread_coordination::Context context, Queue& pq, Settings const& settings) {
        UniformKeyGenerator inserter{context.get_id()};
        UniformKeyGenerator key_generator{context.get_id()};
        unsigned int stage = 0u;

        if constexpr (util::QueueTraits<Queue>::has_thread_init) {
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

    using Queue = util::QueueSelector<key_type, value_type>::queue_type;
    Queue pq(settings.num_threads);
    start_flag.store(false, std::memory_order_relaxed);

    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task<Queue>>(std::ref(pq), settings);
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    coordinator.join();
    std::clog << "All done" << std::endl;
    return 0;
}
