#include <x86intrin.h>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
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

using PriorityQueue = typename multiqueue::util::PriorityQueueFactory<std::uint32_t, std::uint32_t>::type;

using namespace std::chrono_literals;
using clock_type = std::chrono::steady_clock;

static constexpr auto retries = 100;

struct Settings {
    std::filesystem::path graph_file;
    unsigned int num_threads = 4;
};

struct Graph {
    struct Edge {
        std::uint32_t target;
        std::uint32_t weight;
    };
    std::vector<std::uint32_t> nodes;
    std::vector<Edge> edges;
};

struct Distance {
    alignas(L1_CACHE_LINESIZE) std::atomic_uint32_t distance;
};

struct IdleState {
    alignas(L1_CACHE_LINESIZE) std::atomic_uint state;
};

alignas(L1_CACHE_LINESIZE) static std::atomic_size_t idle_counter;
static IdleState* idle_state;

static std::atomic_size_t num_processed_nodes;

std::atomic_bool start_flag;

#ifdef PQ_IS_WRAPPER
static bool idle(PriorityQueue::Handle handle, thread_coordination::Context ctx, PriorityQueue & pq,
                 std::pair<std::uint32_t, std::uint32_t>& retval) {
#else
static bool idle(PriorityQueue::Handle handle, thread_coordination::Context ctx, PriorityQueue const& pq) {
#endif
    idle_counter.fetch_add(1, std::memory_order_relaxed);
    idle_state[ctx.get_id()].state.store(1, std::memory_order_release);
    if (!pq.weak_empty()) {
        idle_counter.fetch_sub(1, std::memory_order_relaxed);
        idle_state[ctx.get_id()].state.store(0, std::memory_order_release);
        return false;
    }
    idle_counter.fetch_add(1, std::memory_order_relaxed);
    idle_state[ctx.get_id()].state.store(2, std::memory_order_release);
    while (true) {
        if (idle_counter.load(std::memory_order_acquire) == 2 * ctx.get_num_threads()) {
            return true;
        }
        if (idle_state[ctx.get_id()].state.load(std::memory_order_acquire) == 0) {
            return false;
        }
        std::this_thread::yield();
    }
}

struct Task {
    static void run(thread_coordination::Context ctx, PriorityQueue& pq, Graph const& graph,
                    std::vector<Distance>& distances) {
#ifdef PQ_SPRAYLIST
        pq.init_thread(ctx.get_num_threads());
#endif
        unsigned int stage = 0;

        auto handle = pq.get_handle(ctx.get_id());

        std::size_t num_local_processed_nodes = 0;

        if (ctx.is_main()) {
            distances[0].distance.store(0, std::memory_order_relaxed);
            pq.push(handle, {0, 0});
        }
        ctx.synchronize(stage++, [&ctx]() {
            std::clog << "Start benchmark..." << std::flush;
            ctx.notify_coordinator();
        });
        while (!start_flag.load(std::memory_order_relaxed)) {
            _mm_pause();
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        while (true) {
            std::pair<std::uint32_t, std::uint32_t> retval;
            if (pq.extract_top(handle, retval)) {
            found:
                std::uint32_t const current_distance =
                    distances[retval.second].distance.load(std::memory_order_relaxed);
                if (retval.first > current_distance) {
                    continue;
                }
                ++num_local_processed_nodes;
                bool pushed = false;
                for (std::size_t i = graph.nodes[retval.second]; i < graph.nodes[retval.second + 1]; ++i) {
                    std::uint32_t target = graph.edges[i].target;
                    std::uint32_t const new_target_distance = current_distance + graph.edges[i].weight;
                    std::uint32_t old_target_distance = distances[target].distance.load(std::memory_order_relaxed);
                    while (old_target_distance > new_target_distance &&
                           !distances[target].distance.compare_exchange_weak(old_target_distance, new_target_distance,
                                                                             std::memory_order_relaxed,
                                                                             std::memory_order_relaxed)) {
                    }
                    if (old_target_distance > new_target_distance) {
                        pq.push(handle, {new_target_distance, target});
                        pushed = true;
                    }
                }
                if (pushed) {
                    if (idle_counter.load(std::memory_order_acquire) > 0) {
                        for (std::size_t i = 0; i < ctx.get_num_threads(); ++i) {
                            if (i == ctx.get_id()) {
                                continue;
                            }
                            unsigned int thread_state = 2;
                            while (!idle_state[i].state.compare_exchange_weak(thread_state, 3,
                                                                              std::memory_order_acq_rel) &&
                                   thread_state != 0 && thread_state != 3) {
                                thread_state = 2;
                                std::this_thread::yield();
                            }
                            if (thread_state == 2) {
                                idle_counter.fetch_sub(2, std::memory_order_relaxed);
                                idle_state[i].state.store(0, std::memory_order_release);
                            }
                        }
                    }
                }
            } else {
                for (std::size_t i = 0; i < retries; ++i) {
                    if (pq.extract_top(handle, retval)) {
                        goto found;
                    }
                    std::this_thread::yield();
                }
#ifdef PQ_IS_WRAPPER
                if (idle(handle, ctx, pq, retval)) {
                    break;
                } else {
                    goto found;
                }
#else
                if (idle(handle, ctx, pq)) {
                    break;
                } else {
                    continue;
                }
            }
        }
        num_processed_nodes += num_local_processed_nodes;
    }

    static threading::thread_config get_config(thread_coordination::Context const& ctx) {
        threading::thread_config config;
        config.cpu_set.reset();
        config.cpu_set.set(ctx.get_id());
        return config;
    }
};

static Graph read_graph(Settings const& settings) {
    std::ifstream graph_stream{settings.graph_file};
    if (!graph_stream) {
        throw std::runtime_error{"Could not open graph file"};
    }
    Graph graph;
    std::vector<std::vector<Graph::Edge>> edges_per_node;
    std::uint32_t source;
    std::uint32_t target;
    std::uint32_t weight;
    std::string problem;
    std::string first;
    while (graph_stream >> first) {
        if (first == "c") {
            graph_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        } else if (first == "p") {
            graph_stream >> problem;
            std::size_t num_nodes;
            std::size_t num_edges;
            graph_stream >> num_nodes >> num_edges;
            graph.nodes.resize(num_nodes + 1, 0);
            edges_per_node.resize(num_nodes);
            graph.edges.reserve(num_edges);
        } else if (first == "a") {
            graph_stream >> source >> target >> weight;
            edges_per_node[source - 1].push_back(Graph::Edge{target - 1, weight});
        } else {
            throw std::runtime_error{"Error reading file"};
        }
    }
    for (std::size_t i = 0; i < edges_per_node.size(); ++i) {
        graph.nodes[i + 1] = graph.nodes[i] + static_cast<std::uint32_t>(edges_per_node[i].size());
        std::copy(edges_per_node[i].begin(), edges_per_node[i].end(), std::back_inserter(graph.edges));
    }
    return graph;
}

static std::vector<std::uint32_t> read_solution(Settings const& settings) {
    std::ifstream solution_stream{settings.solution_file};
    if (!solution_stream) {
        throw std::runtime_error{"Could not open solution file"};
    }
    std::vector<std::uint32_t> solution;
    std::uint32_t node;
    std::uint32_t distance;
    while (solution_stream >> node >> distance) {
        solution.push_back(distance);
    }
    return solution;
}

int main(int argc, char* argv[]) {
    Settings settings{};

    cxxopts::Options options(
        "Shortest path benchmark",
        "This executable measures and records the performance of relaxed priority queues in the SSSP problem");
    // clang-format off
    options.add_options()
      ("j,threads", "Specify the number of threads "
       "(default: 4)", cxxopts::value<unsigned int>(), "NUMBER")
      ("f,file", "The input graph", cxxopts::value<std::filesystem::path>(settings.graph_file)->default_value("graph.gr"), "PATH")
      ("h,help", "Print this help");
    // clang-format on

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            exit(0);
        }
        if (result.count("threads") > 0) {
            settings.num_threads = result["threads"].as<unsigned int>();
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

#ifndef NDEBUG
    std::clog << "Using debug build!\n\n";
#endif
    std::clog << "Settings: \n\t"
              << "Threads: " << settings.num_threads << "\n\t"
              << "Graph file: " << settings.graph_file.string() << "\n\t";
    std::clog << "\n\n";

    std::clog << "Using priority queue: " << PriorityQueue::description() << '\n';
    Graph graph;
    try {
        graph = read_graph(settings);
    } catch (std::runtime_error const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    std::vector<Distance> distances(graph.nodes.size());
    for (std::size_t i = 0; i + 1 < graph.nodes.size(); ++i) {
        distances[i].distance = std::numeric_limits<std::uint32_t>::max();
    }
    idle_state = new IdleState[settings.num_threads];
    num_processed_nodes = 0;
    PriorityQueue pq{settings.num_threads};
    start_flag.store(false, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    thread_coordination::ThreadCoordinator coordinator{settings.num_threads};
    coordinator.run<Task>(std::ref(pq), graph, std::ref(distances));
    coordinator.wait_until_notified();
    start_flag.store(true, std::memory_order_release);
    auto start_tick = clock_type::now();
    coordinator.join();
    auto end_tick = clock_type::now();
    std::clog << "Done\n";
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end_tick - start_tick).count() << ' '
              << num_processed_nodes << '\n';
    /* for (auto const& d : distances) { */
    /*   std::cout << d.distance << '\n'; */
    /* } */
    delete[] idle_state;
    return 0;
}
