#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "cxxopts.hpp"

struct Graph {
    struct Edge {
        std::size_t target;
        std::uint32_t weight;
    };
    std::vector<std::size_t> nodes;
    std::vector<Edge> edges;
};

static std::vector<std::uint32_t> distances;

static Graph read_graph(std::filesystem::path const& graph_file) {
    std::ifstream graph_stream{graph_file};
    if (!graph_stream) {
        throw std::runtime_error{"Could not open graph file"};
    }
    Graph graph;
    std::vector<std::vector<Graph::Edge>> edges_per_node;
    std::size_t source;
    std::size_t target;
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
        graph.nodes[i + 1] = graph.nodes[i] + edges_per_node[i].size();
        std::copy(edges_per_node[i].begin(), edges_per_node[i].end(), std::back_inserter(graph.edges));
    }
    return graph;
}

int main(int argc, char* argv[]) {
    std::filesystem::path graph_file;

    cxxopts::Options options("Sequential shortest path", "Generate shortest paths");
    // clang-format off
    options.add_options()
      ("f,file", "The input graph", cxxopts::value<std::filesystem::path>(graph_file)->default_value("graph.gr"), "PATH")
      ("n,start", "The starting node"
       "(default: 0)", cxxopts::value<size_t>(), "NUMBER")
      ("h,help", "Print this help");
    // clang-format on

    std::size_t starting_node = 0;

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help")) {
            std::cerr << options.help() << std::endl;
            return 0;
        }
        if (result.count("start") > 0) {
            starting_node = result["start"].as<unsigned int>();
        }
    } catch (cxxopts::OptionParseException const& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
    Graph graph;
    std::clog << "Reading graph..." << std::flush;
    try {
        graph = read_graph(graph_file);
    } catch (std::runtime_error const& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    distances.resize(graph.nodes.size() - 1);
    for (std::size_t i = 0; i + 1 < graph.nodes.size(); ++i) {
        distances[i] = std::numeric_limits<std::uint32_t>::max() - 1;
    }
    std::clog << "done\n";
    std::clog << "Calculating shortest paths..." << std::flush;
    std::priority_queue<std::pair<std::uint32_t, std::size_t>, std::vector<std::pair<std::uint32_t, std::size_t>>,
                        std::greater<std::pair<std::uint32_t, std::size_t>>>
        pq;
    distances[starting_node] = 0;
    pq.push({0, starting_node});
    while (!pq.empty()) {
        std::pair<std::uint32_t, std::size_t> p = pq.top();
        pq.pop();
        std::uint32_t const current_distance = distances[p.second];
        if (p.first > current_distance) {
            continue;
        }
        for (std::size_t i = graph.nodes[p.second]; i < graph.nodes[p.second + 1]; ++i) {
            std::uint32_t const new_target_distance = current_distance + graph.edges[i].weight;
            std::size_t const target = graph.edges[i].target;
            if (new_target_distance < distances[target]) {
                distances[target] = new_target_distance;
                pq.push({new_target_distance, target});
            }
        }
    }
    std::clog << "done\n";
    for (std::size_t i = 0; i < distances.size(); ++i) {
        std::cout << i << ' ' << distances[i] << '\n';
    }
}
