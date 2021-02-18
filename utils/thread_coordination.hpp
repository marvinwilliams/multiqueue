#ifndef THREAD_COORDINATION_HPP_7F7173D6
#define THREAD_COORDINATION_HPP_7F7173D6

#include "threading.hpp"

#include <pthread.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

namespace thread_coordination {

class ThreadCoordinator;

class Context {
    friend ThreadCoordinator;
    ThreadCoordinator& coordinator_;
    unsigned int id_;
    unsigned int num_threads_;
    unsigned int step_ = 0;

   private:
    Context(ThreadCoordinator& coordinator, unsigned int id, unsigned int num_threads)
        : coordinator_{coordinator}, id_{id}, num_threads_{num_threads} {
    }

   public:
    inline bool is_main() const noexcept {
        return id_ == 0;
    }

    inline unsigned int get_id() const {
        return id_;
    }

    inline unsigned int get_num_threads() const {
        return num_threads_;
    }

    inline void synchronize(unsigned int step);

    template <typename Callable>
    inline void synchronize(unsigned int step, Callable&& f);

    inline void notify_coordinator() const noexcept;
};

class ThreadCoordinator {
    friend Context;
    unsigned int num_threads_;
    threading::barrier barrier_;
    std::vector<threading::pthread> threads_;
    std::mutex m_;
    std::condition_variable cv_;
    bool notified_ = false;

   public:
    ThreadCoordinator(unsigned int num_threads) : num_threads_{num_threads}, barrier_{num_threads} {
    }

    template <typename Task, typename... Args>
    void run(Args&&... args) {
        threads_.clear();
        threads_.reserve(num_threads_);
        for (unsigned int i = 0; i < num_threads_; ++i) {
            // No forward, since we don't want to move the args
            // We could forward in the last iteration, but it does not matter, since rvalue overloads are useless
            // for f
            threading::thread_config config = Task::get_config(i);
            threads_.emplace_back(config, Task::run, Context{*this, i, num_threads_}, args...);
        }
    }

    void join() {
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    void wait_until_notified() {
      auto lock = std::unique_lock<std::mutex>{m_};
      cv_.wait(lock, [this]() {return notified_;});
      notified_ = false;
    }
};

inline void Context::synchronize(unsigned int step) {
    if (step_ > step) {
        return;
    }
    for (; step_ <= step; ++step_) {
        coordinator_.barrier_.wait();
    }
}

template <typename Callable>
inline void Context::synchronize(unsigned int step, Callable&& f) {
    if (step_ > step) {
        return;
    }
    for (; step_ < step; ++step_) {
        coordinator_.barrier_.wait();
    }
    coordinator_.barrier_.wait(std::forward<Callable>(f));
}

inline void Context::notify_coordinator() const noexcept {
    {
        auto lock = std::unique_lock<std::mutex>{coordinator_.m_};
        coordinator_.notified_ = true;
    }
    coordinator_.cv_.notify_one();
}

}  // namespace thread_coordination

#endif
