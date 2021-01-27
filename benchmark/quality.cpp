#include <cxxopts.hpp>
#include <iostream>
#include <limits>
#include <new>
#include <type_traits>
#include "multiqueue/kv_pq.hpp"
#include "multiqueue/pq.hpp"
#include "multiqueue/relaxed_multiqueue.hpp"
#include "utils.hpp"

#include <chrono>
#include <memory>

using namespace std::chrono_literals;

enum class Policy { Uniform, Split, Producer };
enum class KeyDistribution { Uniform, Dijkstra, Ascending, Descending };

static constexpr size_t default_prefill_size = 1'000'000;
static constexpr std::chrono::milliseconds default_working_time = 10ms;
static constexpr unsigned int default_num_threads = 1u;

using local_pq_t = multiqueue::local_nonaddressable::kv_pq<int32_t, int32_t, std::less<int32_t>>;

namespace multiqueue {
namespace rsm {
template <>
struct Sentinel<int32_t> {
    static constexpr int32_t get() noexcept {
        return std::numeric_limits<int32_t>::max();
    }

    static constexpr bool is(int32_t v) noexcept {
        return v == get();
    }
};
}  // namespace rsm
}  // namespace multiqueue

using multiqueue_t = multiqueue::rsm::priority_queue<int32_t, int32_t>;

struct Settings {
    size_t prefill_size = default_prefill_size;
    std::chrono::milliseconds working_time = default_working_time;
    unsigned int num_threads = default_num_threads;
};

int main(int argc, char* argv[]) {
    cxxopts::Options options("quality benchmark", "This executable measures the quality of relaxed priority queues");
    options.positional_help("[test test2]");
    options.show_positional_help();
    // clang-format off
    options.add_options()
      ("n,prefill", "Specify the number of elements to prefill the queue with "
       "(default: " + std::to_string(default_prefill_size) + ")", cxxopts::value<size_t>(), "NUMBER")
      ("p,policy", "Specify the thread policy as one of \"uniform\", \"split\", \"producer\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("j,threads", "Specify the number of threads "
       "(default: 1)", cxxopts::value<unsigned int>(), "NUMBER")
      ("d,key-distribution", "Specify the key distribution as one of \"uniform\", \"dijkstra\", \"ascending\", \"descending\" "
       "(default: uniform)", cxxopts::value<std::string>(), "ARG")
      ("h,help", "Print this help");
    // clang-format on

    Settings settings{};

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cout << options.help() << std::endl;
            exit(0);
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    std::vector<std::thread> threads;
    barrier b(settings.num_threads + 1, [](unsigned int id) { std::cout << "Barrier broken " << id << std::endl; });
    for (unsigned int i = 0; i < settings.num_threads; ++i) {
        threads.emplace_back([&b, i]() { b.wait(i + 1); });
    }
    std::cout << "Im waiting too\n";
    b.wait(0);
    for (unsigned int i = 0; i < settings.num_threads; ++i) {
        threads[i].join();
    }
    std::cout << "All done!\n";
    return 0;
}
