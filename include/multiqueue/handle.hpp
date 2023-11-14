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
class Handle : public MQ::traits_type::stick_policy_type, private detail::HandleBase<MQ::traits_type::count_stats> {
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
    std::size_t push_index_{0};
    [[no_unique_address]] key_compare comp_;

    explicit Handle(MQ &mq) noexcept
        : base_type{mq.num_pqs_, mq.stick_policy_config_, mq.stick_policy_shared_data_}, mq_{mq}, comp_{mq_.comp_} {
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
        auto const &indices = this->get_pq_indices();
        while (true) {
            std::size_t best_pq = 0;
            auto best_key = mq_.pq_list_[indices[0]].concurrent_top_key();
            for (std::size_t i = 1; i < indices.size(); ++i) {
                auto key = mq_.pq_list_[indices[i]].concurrent_top_key();
                if (sentinel_aware_compare(best_key, key)) {
                    best_pq = i;
                    best_key = key;
                }
            }
            if (best_key == MQ::sentinel_type::get()) {
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.empty_pop_pqs;
                }
                return std::nullopt;
            }
            auto &pq = mq_.pq_list_[indices[best_pq]];
            if (!pq.try_lock()) {
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.locked_pop_pq;
                }
                this->reset_pq(best_pq);
                continue;
            }
            if (pq.unsafe_empty() ||
                (MQ::traits_type::strict_comparison && MQ::key_of_value_type::get(pq.unsafe_top()) != best_key)) {
                // Top got empty (or changed) before locking
                pq.unlock();
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.stale_pop_pq;
                }
                continue;
            }
            auto retval = pq.unsafe_top();
            pq.unsafe_pop();
            pq.unlock();
            return retval;
        }
    }

    std::optional<value_type> try_pop_scan() {
        while (true) {
            std::size_t best_pq = 0;
            auto best_key = mq_.pq_list_[0].concurrent_top_key();
            for (std::size_t i = 1; i < mq_.num_pqs_; ++i) {
                auto key = mq_.pq_list_[i].concurrent_top_key();
                if (sentinel_aware_compare(best_key, key)) {
                    best_pq = i;
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
            auto &pq = mq_.pq_list_[best_pq];
            if (!pq.try_lock()) {
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.locked_pop_pq;
                }
                this->reset_pq(best_pq);
                continue;
            }
            if (pq.unsafe_empty() ||
                (MQ::traits_type::strict_comparison && MQ::key_of_value_type::get(pq.unsafe_top()) != best_key)) {
                // Top got empty (or changed) before locking
                pq.unlock();
                if constexpr (MQ::traits_type::count_stats) {
                    ++this->counters.stale_pop_pq;
                }
                continue;
            }
            auto retval = pq.unsafe_top();
            pq.unsafe_pop();
            pq.unlock();
            return retval;
        }
    }

   public:
    Handle(Handle const &) = delete;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    Handle(Handle &&) noexcept = default;
    ~Handle() = default;

    void push(value_type const &v) {
        auto const &indices = this->get_pq_indices();
        while (!mq_.pq_list_[indices[push_index_]].try_lock()) {
            if constexpr (MQ::traits_type::count_stats) {
                ++this->counters.locked_push_pq;
            }
            this->reset_pq(push_index_);
        }
        mq_.pq_list_[indices[push_index_]].unsafe_push(v);
        mq_.pq_list_[indices[push_index_]].unlock();
        this->use_pqs();
    }

    std::optional<value_type> try_pop() {
        for (unsigned i = 0; i < MQ::traits_type::num_pop_tries; ++i) {
            auto retval = try_pop_best();
            if (retval) {
                this->use_pqs();
                return *retval;
            }
            this->reset_pqs();
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
