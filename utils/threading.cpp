#include "threading.hpp"

#include <pthread.h>
#include <memory>
#include <stdexcept>

namespace threading {

extern "C" void *trampoline(void *state) {
    auto ptr = std::unique_ptr<invoker_base>{static_cast<invoker_base *>(state)};
    (*ptr)();
    return nullptr;
}

pthread::pthread(pthread &&other) noexcept {
    std::swap(thread_handle_, other.thread_handle_);
    std::swap(detached_, other.detached_);
}

pthread &pthread::operator=(pthread &&other) noexcept {
    if (thread_handle_) {
        std::terminate();
    }
    std::swap(thread_handle_, other.thread_handle_);
    std::swap(detached_, other.detached_);
    return *this;
}

pthread::~pthread() noexcept {
    if (joinable()) {
        std::terminate();
    }
}

void pthread::init_attr_with_config(pthread_attr_t *attr_ptr, sched_param *sched_param_ptr, cpu_set_t *cpu_set_ptr,
                                    thread_config const &config) {
    if (attr_ptr == nullptr) {
        throw std::invalid_argument{"attr_ptr must not be nullptr"};
    }
    config.check();
    int rc;
    rc = pthread_attr_init(attr_ptr);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to init pthread attribute: "};
    }
    if (config.detached) {
        rc = pthread_attr_setdetachstate(attr_ptr, PTHREAD_CREATE_DETACHED);
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to set to detached state: "};
        }
    }
    if (config.policy) {
        rc = pthread_attr_setinheritsched(attr_ptr, PTHREAD_EXPLICIT_SCHED);
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to set explicit scheduling for thread: "};
        }
        std::visit(
            [&](auto &&p) {
                rc = pthread_attr_setschedpolicy(attr_ptr, p.id);
                if (rc != 0) {
                    throw std::system_error{rc, std::system_category(), "Failed to set scheduling policy: "};
                }
                using policy_type = std::decay_t<decltype(p)>;
                if constexpr (policy_type::has_priority) {
                    if (sched_param_ptr == nullptr) {
                        throw std::invalid_argument{"sched_param_ptr must not be nullptr"};
                    }
                    sched_param_ptr->sched_priority = p.priority;
                    rc = pthread_attr_setschedparam(attr_ptr, sched_param_ptr);
                    if (rc != 0) {
                        throw std::system_error{rc, std::system_category(), "Failed to set scheduling parameter: "};
                    }
                }
            },
            *config.policy);
    }
    if (!config.cpu_set.all()) {
        if (cpu_set_ptr == nullptr) {
            throw std::invalid_argument{"cpu_set_ptr must not be nullptr"};
        }
        CPU_ZERO(cpu_set_ptr);
        for (size_t i = 0; i < config.cpu_set.size(); ++i) {
            if (config.cpu_set[i]) {
                CPU_SET(i, cpu_set_ptr);
            }
        }
        rc = pthread_attr_setaffinity_np(attr_ptr, sizeof(cpu_set_t), cpu_set_ptr);
        if (rc != 0) {
            throw std::system_error{rc, std::system_category(), "Failed to set thread affinity: "};
        }
    }
}

bool pthread::joinable() const {
    return thread_handle_.has_value() && !detached_;
}

void pthread::detach() {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    if (detached_) {
        throw std::runtime_error{"Thread is already detached"};
    }
    int rc = pthread_detach(*thread_handle_);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to detach thread: "};
    }
    detached_ = true;
}

void pthread::set_policy(scheduling::Policy policy) {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    scheduling::check_policy(policy);
    std::visit(
        [&](auto &&p) {
            sched_param param;
            using policy_type = std::decay_t<decltype(p)>;
            if constexpr (policy_type::has_priority) {
                param.sched_priority = p.priority;
            } else {
                param.sched_priority = 0;
            }
            int rc = pthread_setschedparam(*thread_handle_, p.id, &param);
            if (rc != 0) {
                throw std::system_error{rc, std::system_category(), "Failed to set scheduling parameter: "};
            }
        },
        policy);
}

void pthread::set_priority(int priority) {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    int rc = pthread_setschedprio(*thread_handle_, priority);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to set thread priority: "};
    }
}

void pthread::pin_to_core(size_t core) {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    int rc = pthread_setaffinity_np(*thread_handle_, sizeof(cpu_set_t), &set);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Pin to core failed: "};
    }
}

void pthread::set_affinity(std::bitset<CPU_SETSIZE> const &cpu_set) {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    for (size_t i = 0; i < cpu_set.size(); ++i) {
        if (cpu_set[i]) {
            CPU_SET(i, &set);
        }
    }
    int rc = pthread_setaffinity_np(*thread_handle_, sizeof(cpu_set_t), &set);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to set thread affinity"};
    }
}

bool pthread::join() {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    if (detached_) {
        throw std::runtime_error{"Cannot join detached thread"};
    }
    void *retval;
    int rc = pthread_join(*thread_handle_, &retval);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to join thread: "};
    }
    thread_handle_ = std::nullopt;
    return retval != PTHREAD_CANCELED;
}

bool pthread::try_join() {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    if (detached_) {
        throw std::runtime_error{"Cannot join detached thread"};
    }
    void *retval;
    int rc = pthread_tryjoin_np(*thread_handle_, &retval);
    if (rc == EBUSY) {
        return false;
    } else if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to try to join: "};
    }
    thread_handle_ = std::nullopt;
    return true;
}

bool pthread::join_for(std::chrono::milliseconds ms) {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    if (detached_) {
        throw std::runtime_error{"Cannot join detached thread"};
    }
    void *retval;
    timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        throw std::system_error{errno, std::system_category(), "Failed to get time: "};
    }
    auto s = std::chrono::duration_cast<std::chrono::seconds>(ms);
    ts.tv_sec += s.count();
    ts.tv_nsec += std::chrono::nanoseconds{ms - s}.count();
    int rc = pthread_timedjoin_np(*thread_handle_, &retval, &ts);
    if (rc == ETIMEDOUT) {
        return false;
    } else if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to join timed: "};
    }
    thread_handle_ = std::nullopt;
    return true;
}

void pthread::cancel() {
    if (!thread_handle_) {
        throw std::runtime_error{"Thread object does not represent an active thread of execution"};
    }
    int rc = pthread_cancel(*thread_handle_);
    if (rc != 0) {
        throw std::system_error{rc, std::system_category(), "Failed to cancel thread: "};
    }
}

std::chrono::milliseconds get_thread_runtime() {
    clockid_t cid;
    auto rc = pthread_getcpuclockid(pthread_self(), &cid);
    if (rc != 0) {
        std::system_error{rc, std::system_category(), "Failed to get thread's clock id"};
    }
    timespec ts;
    if (clock_gettime(cid, &ts) == -1) {
        std::system_error{errno, std::system_category(), "Failed to get thread's time"};
    }
    return std::chrono::seconds{ts.tv_sec} +
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds{ts.tv_nsec});
}

}  // namespace threading
