#include "multiqueue/heap.hpp"

#include <papi.h>
#include <iterator>
#include <queue>
#include <random>
#include <string_view>

volatile bool dummy;

template <typename Queue>
void do_test() {
    {
        auto pq = Queue{};
        for (int i = 1; i <= 100'000; ++i) {
            pq.push(i);
        }
        for (int i = 1; i <= 100'000; ++i) {
            pq.pop();
        }
        dummy = pq.empty();
    }
    {
        auto pq = Queue{};
        for (int i = 100'000; i > 0; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= 100'000; ++i) {
            pq.pop();
        }
        dummy = pq.empty();
    }
    {
        auto pq = Queue{};
        for (int i = 1; i <= 50'000; ++i) {
            pq.push(i);
        }
        for (int i = 100'000; i > 50'000; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= 100'000; ++i) {
            pq.pop();
        }
        dummy = pq.empty();
    }
}

template <typename Queue>
void benchmark_queue() {
    long_long start_cycles, end_cycles, start_usec, end_usec;
    long_long start_cycles_v, end_cycles_v, start_usec_v, end_usec_v;

    start_cycles = PAPI_get_real_cyc();
    start_usec = PAPI_get_real_usec();
    start_cycles_v = PAPI_get_virt_cyc();
    start_usec_v = PAPI_get_virt_usec();

    do_test<Queue>();

    end_cycles = PAPI_get_real_cyc();
    end_usec = PAPI_get_real_usec();
    end_cycles_v = PAPI_get_virt_cyc();
    end_usec_v = PAPI_get_virt_usec();

    printf("Wall clock cycles: %lld\n", end_cycles - start_cycles);
    printf("Wall clock time in microseconds: %lld\n", end_usec - start_usec);
    printf("Virt clock cycles: %lld\n", end_cycles_v - start_cycles_v);
    printf("Virt clock time in microseconds: %lld\n", end_usec_v - start_usec_v);
}

template <unsigned int Degree>
using PQ = multiqueue::local_nonaddressable::Heap<int, std::less<int>, Degree>;

int main(int argc, char *argv[]) {
    int retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        fprintf(stderr, "PAPI library init error!\n");
        exit(1);
    }
    for (int i = 0; i < 10; ++i) {
        do_test<PQ<2>>();
    }
    printf("%s\n", "Degree 2");
    benchmark_queue<PQ<2>>();
    for (int i = 0; i < 10; ++i) {
        do_test<PQ<4>>();
    }
    printf("%s\n", "Degree 4");
    benchmark_queue<PQ<4>>();
    for (int i = 0; i < 10; ++i) {
        do_test<PQ<17>>();
    }
    printf("%s\n", "Degree 17");
    benchmark_queue<PQ<17>>();
    return 0;
}
