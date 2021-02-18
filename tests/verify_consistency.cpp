#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

struct log_entry {
    unsigned int thread_id;
    uint64_t tick;
    uint32_t key;
    unsigned int other_thread_id;
    uint32_t value;
    bool deleted;
};

std::istream& operator>>(std::istream& in, log_entry& line) {
    in >> line.thread_id >> line.tick >> line.key >> line.other_thread_id >> line.value;
    return in;
}

std::ostream& operator<<(std::ostream& out, log_entry const& line) {
    out << "Thread: " << line.thread_id << " Tick: " << line.tick << " Key: " << line.key
        << " Inserting thread: " << line.other_thread_id << " Value: " << line.value;
    return out;
}

struct pair_hash {
    template <typename First, typename Second>
    std::size_t operator()(std::pair<First, Second> const& pair) const noexcept {
        return std::hash<First>{}(pair.first) ^ std::hash<Second>{}(pair.second);
    }
};

int main() {
    std::vector<std::vector<log_entry>> insertions;
    std::clog << "Reading log file from stdin..." << std::endl;
    unsigned int num_threads;
    std::cin >> num_threads;
    if (!std::cin || num_threads == 0) {
      std::cerr << "Invalid number of threads" << std::endl;
      return 1;
    }
    insertions.resize(num_threads);
    char op;
    bool deleting = false;
    for (log_entry entry; std::cin >> op >> entry;) {
        if (entry.thread_id >= num_threads) {
            std::cerr << "Thread id " << entry.thread_id << " too high (Max: " << num_threads - 1 << ')' << std::endl;
            return 1;
        }
        if (entry.other_thread_id >= num_threads) {
            std::cerr << "Other thread id " << entry.other_thread_id << " too high (Max: " << num_threads - 1 << ')'
                      << std::endl;
            return 1;
        }
        if (op == 'i') {
            if (deleting) {
                std::cerr << "Insertion\n\t" << entry << " following a deletion in log" << std::endl;
                return 1;
            }
            if (entry.value != insertions[entry.thread_id].size() || entry.thread_id != entry.other_thread_id) {
                std::cerr << "Inconsistent insertion:\n\t" << entry << std::endl;
                return 1;
            }
            entry.deleted = false;
            insertions[entry.thread_id].push_back(entry);
        } else if (op == 'd') {
            deleting = true;
            if (entry.value >= insertions[entry.other_thread_id].size()) {
                std::cerr << "No insertion corresponding to deletion\n\t" << entry << std::endl;
                return 1;
            }
            if (entry.key != insertions[entry.other_thread_id][entry.value].key) {
                std::cerr << "Deletion \n\t" << entry << "\ninconsistent to insertion\n\t"
                          << insertions[entry.other_thread_id][entry.value] << std::endl;
                return 1;
            }
            if (entry.tick < insertions[entry.other_thread_id][entry.value].tick) {
                std::cerr << "Deletion of \n\t" << entry << "\nhappens before its insertion" << std::endl;
                return 1;
            }
            if (insertions[entry.other_thread_id][entry.value].deleted) {
                std::cerr << "Insertion\n\t" << insertions[entry.other_thread_id][entry.value].deleted
                          << "\n extracted twice" << std::endl;
                return 1;
            }
            insertions[entry.other_thread_id][entry.value].deleted = true;
        } else {
            std::cerr << "Invalid operation: " << op << std::endl;
            return 1;
        }
    }
    std::clog << "Log is consistent" << std::endl;
    return 0;
}
