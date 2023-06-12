#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <type_traits>

namespace multiqueue {

struct Counters {
    long long locked_push_pq = 0;
    long long empty_pop_pqs = 0;
    long long locked_pop_pq = 0;
    long long stale_pop_pq = 0;

    friend Counters &operator+=(Counters &lhs, Counters const &rhs) noexcept {
        lhs.locked_push_pq += rhs.locked_push_pq;
        lhs.empty_pop_pqs += rhs.empty_pop_pqs;
        lhs.locked_pop_pq += rhs.locked_pop_pq;
        lhs.stale_pop_pq += rhs.stale_pop_pq;
        return lhs;
    }
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
class Handle : public MQ::traits_type::queue_selection_policy_type,
               private detail::HandleBase<MQ::traits_type::count_stats> {
    friend MQ;

    using base_type = typename MQ::traits_type::queue_selection_policy_type;

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
        : base_type{mq.num_pqs_, mq.queue_selection_config_, mq.queue_selection_shared_data_},
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

    bool try_pop_best(reference retval) {
        do {
            auto indices = this->get_pop_pqs();
            do {
                auto best_pq = &mq_.pq_list_[indices[0]];
                auto best_key = best_pq->concurrent_top_key();
                for (auto it = indices.begin() + 1; it != indices.end(); ++it) {
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
                    return false;
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
                    break;
                }
                retval = best_pq->unsafe_top();
                best_pq->unsafe_pop();
                best_pq->unlock();
                this->use_pop_pqs();
                return true;
            } while (true);
            this->reset_pop_pqs();
        } while (true);
    }

    bool try_pop_scan(reference retval) {
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
                return false;
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
            retval = best_pq->unsafe_top();
            best_pq->unsafe_pop();
            best_pq->unlock();
            return true;
        } while (true);
    }

   public:
    Handle(Handle const &) = delete;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    Handle(Handle &&) noexcept = default;
    ~Handle() = default;

    void push(value_type const &v) {
        size_type index = this->get_push_pq();
        auto pq = &mq_.pq_list_[index];
        while (!pq->try_lock()) {
            if constexpr (MQ::traits_type::count_stats) {
                ++this->counters.locked_push_pq;
            }
            this->reset_push_pq();
            index = this->get_push_pq();
            pq = &mq_.pq_list_[index];
        }
        pq->unsafe_push(v);
        pq->unlock();
        this->use_push_pq();
    }

    bool try_pop(value_type &retval) {
        for (int i = 0; i < MQ::traits_type::num_pop_tries; ++i) {
            if (try_pop_best(retval)) {
                return true;
            }
            this->reset_pop_pqs();
        }
        return MQ::traits_type::scan_on_failed_pop && try_pop_scan(retval);
    }

    [[nodiscard]] Counters const &get_counters() const noexcept {
        return this->counters;
    }

    void reset_counters() noexcept {
      this->counters = {};
    }
};

}  // namespace multiqueue
