#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>

namespace multiqueue {

template <typename Context, typename OperationPolicy, bool ScanIfEmpty>
class Handle : public OperationPolicy {
    using policy_type = OperationPolicy;

    Context &context_;
    using value_type = typename Context::value_type;

   public:
    explicit Handle(Context &ctx) noexcept : policy_type{ctx}, context_{ctx} {
    }

   private:
    std::optional<value_type> scan() {
        for (auto *it = context_.pq_list(); it != context_.pq_list() + context_.num_pqs(); ++it) {
            if (it->try_lock()) {
                auto &pq = it->get_pq();
                if (!pq.empty()) {
                    auto retval = pq.top();
                    pq.pop();
                    it->update_top_key();
                    it->unlock();
                    return retval;
                }
                it->unlock();
            }
        }
        return std::nullopt;
    }

   public:
    Handle(Handle const &) = delete;
    Handle &operator=(Handle const &) = delete;
    Handle &operator=(Handle &&) noexcept = default;
    Handle(Handle &&) noexcept = default;
    ~Handle() = default;

    void push(value_type const &v) {
        policy_type::push(context_, v);
    }

    std::optional<value_type> try_pop() {
        std::optional<value_type> retval = policy_type::try_pop(context_);
        if (!retval && ScanIfEmpty) {
            retval = scan();
        }
        return retval;
    }
};

}  // namespace multiqueue
