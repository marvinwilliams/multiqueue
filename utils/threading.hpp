#ifndef THREADING_HPP_YHEJJOUQ
#define THREADING_HPP_YHEJJOUQ

#include <bitset>
#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <system_error>
#include <variant>

#include <pthread.h>
#include <sched.h>

namespace threading {

class barrier {
    pthread_barrier_t b_;

   public:
    explicit barrier(unsigned int num_threads) {
        int rc;
        rc = pthread_barrier_init(&b_, NULL, num_threads);
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to create barrier: "};
        }
    }

    barrier(barrier const &) = delete;
    barrier &operator=(barrier const &) = delete;

    template <typename Callable>
    void wait(Callable &&f) {
        int rc;
        rc = pthread_barrier_wait(&b_);
        if (rc == PTHREAD_BARRIER_SERIAL_THREAD) {
            f();
        } else if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to wait for barrier: "};
        }
    }

    void wait() {
        int rc;
        rc = pthread_barrier_wait(&b_);
        if (rc == PTHREAD_BARRIER_SERIAL_THREAD) {
        } else if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to wait for barrier: "};
        }
    }

    ~barrier() noexcept {
        int rc = pthread_barrier_destroy(&b_);
        if (rc != 0) {
            auto e = std::system_error{rc, std::system_category(), "Failed to destroy barrier: "};
            std::cerr << e.what() << '\n';
        }
    }
};

namespace scheduling {
namespace detail {

template <bool>
struct WithNice {
    static constexpr bool has_nice = false;
};

template <>
struct WithNice<true> {
    static constexpr bool has_nice = true;
    int nice = 0;
};

template <bool>
struct WithPriority {
    static constexpr bool has_priority = false;
};

template <>
struct WithPriority<true> {
    static constexpr bool has_priority = true;
    int priority = 1;
};

template <bool with_nice, bool with_priority>
struct PolicyBase : WithNice<with_nice>, WithPriority<with_priority> {};

}  // namespace detail

struct Normal : detail::PolicyBase<true, false> {
    static constexpr int id = SCHED_OTHER;
    constexpr Normal(int n = 0) noexcept {
        nice = n;
    }
};

struct Idle : detail::PolicyBase<true, false> {
    static constexpr int id = SCHED_IDLE;
    constexpr Idle(int n = 0) noexcept {
        nice = n;
    }
};

struct Fifo : detail::PolicyBase<false, true> {
    static constexpr int id = SCHED_FIFO;
    constexpr Fifo(int p = sched_get_priority_min(id)) noexcept {
        priority = p;
    }
};

struct RoundRobin : detail::PolicyBase<false, true> {
    static constexpr int id = SCHED_RR;
    constexpr RoundRobin(int p = sched_get_priority_min(id)) noexcept {
        priority = p;
    }
};

using Policy = std::variant<scheduling::Normal, scheduling::Idle, scheduling::Fifo, scheduling::RoundRobin>;

inline void check_policy(Policy const &policy) {
    std::visit(
        [](auto &&p) {
            // p.has_nice is not constexpr
            using policy_type = std::decay_t<decltype(p)>;
            if constexpr (policy_type::has_nice) {
                if (p.nice < -20 || p.nice > 19) {
                    throw std::domain_error{"Niceness out of range"};
                }
            }
            if constexpr (policy_type::has_priority) {
                if (p.priority < sched_get_priority_min(p.id) || p.priority > sched_get_priority_max(p.id)) {
                    throw std::domain_error{"Priority out of range"};
                }
            }
        },
        policy);
}

}  // namespace scheduling

struct thread_config {
    static constexpr auto cpu_setsize = CPU_SETSIZE;
    std::bitset<cpu_setsize> cpu_set;
    std::optional<scheduling::Policy> policy = std::nullopt;
    bool detached = false;

    thread_config() noexcept {
        cpu_set.set();
    }

    void check() const {
        if (cpu_set.none()) {
            throw std::domain_error{"Empty cpu set"};
        }
        if (policy) {
            if (std::holds_alternative<scheduling::Idle>(*policy)) {
                throw std::domain_error{"Not possible to schedule idle thread"};
            }
            scheduling::check_policy(*policy);
        }
    }
};

struct invoker_base {
    virtual ~invoker_base() {
    }
    virtual void operator()() noexcept = 0;
};

template <typename Callable, typename... Args>
struct invoker : invoker_base {
    using ArgTuple = std::tuple<Args...>;
    Callable f;
    ArgTuple args;

    invoker(Callable func, std::tuple<Args...> arg_tuple) : f{std::move(func)}, args{std::move(arg_tuple)} {
    }

    template <size_t... I>
    void invoke(std::index_sequence<I...>) {
        std::invoke(std::move(f), std::get<I>(args)...);
    }

    void operator()() noexcept override {
        invoke(std::make_index_sequence<std::tuple_size_v<ArgTuple>>());
    }
};

extern "C" void *trampoline(void *invoker);

class pthread {
    std::optional<pthread_t> thread_handle_ = std::nullopt;
    bool detached_ = false;

    static void init_attr_with_config(pthread_attr_t *attr, sched_param *sched_param_ptr, cpu_set_t *cpu_set_ptr,
                                      thread_config const &config);

   public:
    pthread() noexcept = default;

    pthread(pthread const &) = delete;

    pthread(pthread &&other) noexcept;

    pthread &operator=(pthread const &other) = delete;

    pthread &operator=(pthread &&other) noexcept;

    ~pthread() noexcept;

    template <typename Callable, typename... Args>
    pthread(thread_config const &config, Callable &&f, Args &&...args) {
        static_assert(std::is_invocable_v<std::decay_t<Callable>, std::decay_t<Args>...>,
                      "pthread arguments must be invocable after conversion to rvalues");
        using invoker_t = invoker<std::decay_t<Callable>, std::decay_t<Args>...>;
        auto invoker_ptr =
            std::unique_ptr<invoker_base>{new invoker_t(std::forward<Callable>(f), {std::forward<Args>(args)...})};
        thread_handle_ = pthread_t{};
        pthread_attr_t attr;
        sched_param param;
        cpu_set_t set;
        init_attr_with_config(&attr, &param, &set, config);
        detached_ = config.detached;
        int rc = pthread_create(&(*thread_handle_), &attr, trampoline, invoker_ptr.get());
        pthread_attr_destroy(&attr);
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to create thread: "};
        }
        (void)invoker_ptr.release();
    }

    template <typename Callable, typename... Args,
              typename = std::enable_if_t<!std::is_same_v<std::decay_t<Callable>, thread_config>>>
    pthread(Callable &&f, Args &&...args) {
        static_assert(std::is_invocable_v<std::decay_t<Callable>, std::decay_t<Args>...>,
                      "pthread arguments must be invocable after conversion to rvalues");
        using invoker_t = invoker<std::decay_t<Callable>, std::decay_t<Args>...>;
        auto invoker_ptr =
            std::unique_ptr<invoker_base>{new invoker_t(std::forward<Callable>(f), {std::forward<Args>(args)...})};
        thread_handle_ = pthread_t{};
        int rc = pthread_create(&(*thread_handle_), NULL, trampoline, invoker_ptr.get());
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to create thread: "};
        }
        (void)invoker_ptr.release();
    }

    bool joinable() const;

    void detach();

    void set_policy(scheduling::Policy policy);

    void set_priority(int priority);

    void pin_to_core(size_t core);

    void set_affinity(std::bitset<CPU_SETSIZE> const &cpu_set);

    bool join();

    bool try_join();

    bool join_for(std::chrono::milliseconds ms);

    void cancel();
};

std::chrono::milliseconds get_thread_runtime();

}  // namespace threading

#endif
