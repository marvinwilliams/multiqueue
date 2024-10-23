#pragma once

#include "multififo/stick_random.hpp"

#include <optional>

namespace multififo {

template <typename Context>
class Handle : public multififo::mode::StickRandom<2> {
    using mode_type = multififo::mode::StickRandom<2>;
    using clock_type = typename Context::clock_type;
    Context *context_;
    using value_type = typename Context::value_type;

    bool scan_push(value_type const &v) {
        for (auto *it = context_->queue_guards(); it != context_->queue_guards() + context_->num_queues(); ++it) {
            if (!it->try_lock()) {
                continue;
            }
            if (it->get_queue().full()) {
                it->unlock();
                continue;
            }
            auto tick = static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
            it->get_queue().push({tick, v});
            it->pushed();
            it->unlock();
            return true;
        }
        return false;
    }

    std::optional<value_type> scan_pop() {
        for (auto *it = context_->queue_guards(); it != context_->queue_guards() + context_->num_queues(); ++it) {
            if (!it->try_lock()) {
                continue;
            }
            if (it->get_queue().empty()) {
                it->unlock();
                continue;
            }
            auto v = it->get_queue().top().value;
            it->get_queue().pop();
            it->popped();
            it->unlock();
            return v;
        }
        return std::nullopt;
    }

   public:
    explicit Handle(Context &ctx) noexcept : mode_type{ctx.seed(), ctx.new_id()}, context_{&ctx} {
    }

    Handle(Handle const &) = delete;
    Handle(Handle &&) noexcept = default;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    ~Handle() = default;

    bool try_push(value_type const &v) {
        auto success = mode_type::try_push(*context_, v);
        if (success) {
            return true;
        }
        return scan_push(v);
    }

    std::optional<value_type> try_pop() {
        auto v = mode_type::try_pop(*context_);
        if (v) {
            return *v;
        }
        return scan_pop();
    }
};

}  // namespace multififo
