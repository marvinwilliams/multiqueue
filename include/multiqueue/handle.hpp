#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>

namespace multiqueue {

template <typename Context>
class Handle : public Context::policy_type::mode_type {
    using mode_type = typename Context::policy_type::mode_type;

    Context *context_;
    using value_type = typename Context::value_type;

   public:
    explicit Handle(Context &ctx) noexcept : mode_type{ctx.config(), ctx.shared_data()}, context_{&ctx} {
    }

    Handle(Handle const &) = delete;
    Handle(Handle &&) noexcept = default;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    ~Handle() = default;

    void push(value_type const &v) {
        mode_type::push(*context_, v);
    }

    std::optional<value_type> scan() {
        for (auto *it = context_->pq_guards(); it != context_->pq_guards() + context_->num_pqs(); ++it) {
            if (!it->try_lock()) {
                continue;
            }
            if (it->get_pq().empty()) {
                it->unlock();
                continue;
            }
            auto v = it->get_pq().top();
            it->get_pq().pop();
            it->popped();
            it->unlock();
            return v;
        }
        return std::nullopt;
    }

    std::optional<value_type> try_pop() {
        for (int i = 0; i < Context::policy_type::pop_tries; ++i) {
            std::optional<value_type> v = mode_type::try_pop(*context_);
            if (v) {
                return v;
            }
        }
        if (!Context::policy_type::scan) {
            return std::nullopt;
        }
        return scan();
    }
};

}  // namespace multiqueue
