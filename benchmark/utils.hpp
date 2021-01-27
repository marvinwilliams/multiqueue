#ifndef UTILS_YHEJJOUQ
#define UTILS_YHEJJOUQ

#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>

extern "C" {
#include <pthread.h>
}

template <typename CompletionFunc>
class barrier {
    pthread_barrier_t b_;
    CompletionFunc f_;

   public:
    constexpr explicit barrier(unsigned int num_threads, CompletionFunc f = CompletionFunc()) : f_{std::move(f)} {
        int result = pthread_barrier_init(&b_, NULL, num_threads);
        if (result != 0) {
            throw std::runtime_error{"Failed to initialize barrier"};
        }
    }

    barrier(barrier const &) = delete;
    barrier &operator=(barrier const &) = delete;

    template <typename...Args>
    void wait(Args&& ...args) {
        int result = pthread_barrier_wait(&b_);
        if (result == PTHREAD_BARRIER_SERIAL_THREAD) {
            f_(std::forward<Args>(args)...);
        } else if (result != 0) {
            throw std::runtime_error{"Failed to wait for barrier"};
        }
    }

    ~barrier() noexcept {
        int result = pthread_barrier_destroy(&b_);
        if (result != 0) {
            std::cerr << "Failed to destroy barrier" << std::endl;
        }
    }
};

struct ThreadCoordinator {};
struct Stage {};
struct Scenario {};
#endif
