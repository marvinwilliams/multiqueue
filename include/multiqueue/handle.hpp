#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>

namespace multiqueue {

struct Counters {
    long long locked_push_pq = 0;
    long long empty_pop_pqs = 0;
    long long locked_pop_pq = 0;
    long long stale_pop_pq = 0;
};

namespace detail {

template <bool CountStats>
struct HandleBase {};

template <>
struct HandleBase<true> {
    Counters counters;
};

}  // namespace detail

template <typename MQ>
class Handle : public MQ::traits_type::stick_policy_type,
               private detail::HandleBase<MQ::traits_type::count_stats> {
    friend MQ;

    using base_type = typename MQ::traits_type::stick_policy_type;
    using stick_policy_shared_data_type = typename base_type::SharedData;

   public:
    using key_type = typename MQ::key_type;
    using value_type = typename MQ::value_type;
    using key_compare = typename MQ::key_compare;

    using size_type = typename MQ::size_type;
    using reference = typename MQ::reference;
    using const_reference = typename MQ::const_reference;

   private:
    MQ &mq_;
    [[no_unique_address]] key_compare comp_;

    explicit Handle(MQ &mq) noexcept
        : base_type{mq.num_pqs_, mq.stick_policy_config_, mq.stick_policy_shared_data_},
          mq_{mq},
          comp_{mq_.comp_} {
    }

    bool sentinel_aware_compare(key_type const &lhs, key_type const &rhs) const {
        if (!MQ::sentinel_type::is_implicit) {
            if (rhs == MQ::sentinel_type::get()) {
                return false;
            }
            if (lhs == MQ::sentinel_type::get()) {
                return true;
            }
        }
        return comp_(lhs, rhs);
    };

    std::optional<value_type> try_pop_best() {
        do {
            auto indices = this->get_pop_pqs();
            do {
                auto best_pq = &mq_.pq_list_[indices[0]];
                auto best_key = best_pq->concurrent_top_key();
                for (auto it = std::begin(indices) + 1; it != std::end(indices); ++it) {
                    auto pq = &mq_.pq_list_[*it];
                    auto key = pq->concurrent_top_key();
                    if (sentinel_aware_compare(best_key, key)) {
                        best_pq = pq;
                        best_key = key;
                    }
                }
                if (best_key == MQ::sentinel_type::get()) {
                    if constexpr (MQ::traits_type::count_stats) {
                        ++this->counters.empty_pop_pqs;
                    }
                    return std::nullopt;
                }
                if (!best_pq->try_lock()) {
                    if constexpr (MQ::traits_type::count_stats) {
                        ++this->counters.locked_pop_pq;
                    }
                    break;
                }
                if (best_pq->unsafe_empty() ||
                    (MQ::traits_type::strict_comparison &&
                     MQ::key_of_value_type::get(best_pq->unsafe_top()) != best_key)) {
                    // Top got empty (or changed) before locking
                    best_pq->unlock();
                    if constexpr (MQ::traits_type::count_stats) {
                        ++this->counters.stale_pop_pq;
                    }
                    continue;
                }
                auto retval = best_pq->unsafe_top();
                best_pq->unsafe_pop();
                best_pq->unlock();
                return retval;
            } while (true);
            this->reset_pop_pqs();
        } while (true);
    }

    std::optional<value_type> try_pop_scan() {
        do {
            auto best_pq = mq_.pq_list_;
            auto best_key = best_pq->concurrent_top_key();
            for (auto pq = mq_.pq_list_ + 1, end = mq_.pq_list_ + mq_.num_pqs_; pq != end; ++pq) {
                auto key = pq->concurrent_top_key();
                if (sentinel_aware_compare(best_key, key)) {
                    best_pq = pq;
                    best_key = key;
                }
            }
            if (best_key == MQ::sentinel_type::get()) {
                // All pqs appear to be empty (not necessarily true in concurrent setting)
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.empty_pop_pqs;
                }
                return std::nullopt;
            }
            if (!best_pq->try_lock()) {
                // Not lock-free, but we could also just return the first found element without loss of quality, as most
                // pqs are empty, anyways
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.locked_pop_pq;
                }
                continue;
            }
            if (best_pq->unsafe_empty() ||
                (MQ::traits_type::strict_comparison && MQ::key_of_value_type::get(best_pq->unsafe_top()) != best_key)) {
                // Top got empty (or changed) before locking
                best_pq->unlock();
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.stale_pop_pq;
                }
                continue;
            }
            auto retval = best_pq->unsafe_top();
            best_pq->unsafe_pop();
            best_pq->unlock();
            return retval;
        } while (true);
    }

   public:
    Handle(Handle const &) = delete;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    Handle(Handle &&) noexcept = default;
    ~Handle() = default;

    void push(value_type const &v) {
        size_type i = this->get_push_pq();
        auto pq = &mq_.pq_list_[i];
        while (!pq->try_lock()) {
            if constexpr (MQ::traits_type::count_stats) {
                ++this->counters.locked_push_pq;
            }
            this->reset_push_pq();
            i = this->get_push_pq();
            pq = &mq_.pq_list_[i];
        }
        pq->unsafe_push(v);
        pq->unlock();
        this->use_push_pq();
    }

    std::optional<value_type> try_pop() {
        for (unsigned i = 0; i < MQ::traits_type::num_pop_tries; ++i) {
            auto retval = try_pop_best();
            if (retval) {
                this->use_pop_pqs();
                return *retval;
            }
            this->reset_pop_pqs();
        }
        if (!MQ::traits_type::scan_on_failed_pop) {
            return std::nullopt;
        }
        return try_pop_scan();
    }

    [[nodiscard]] Counters const &get_counters() const noexcept {
        return this->counters;
    }

    void reset_counters() noexcept {
        this->counters = {};
    }
};

}  // namespace multiqueue
