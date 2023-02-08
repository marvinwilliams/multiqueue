/**
******************************************************************************
* @file:   stick_policy.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once

#include "multiqueue/stick_policies/global_permutation.hpp"
#include "multiqueue/stick_policies/no_sticking.hpp"
#include "multiqueue/stick_policies/no_sticking_strict.hpp"
#include "multiqueue/stick_policies/random.hpp"
#include "multiqueue/stick_policies/random_strict.hpp"
#include "multiqueue/stick_policies/swapping.hpp"
#include "multiqueue/stick_policies/swapping_blocking.hpp"
#include "multiqueue/stick_policies/swapping_lazy.hpp"

namespace multiqueue {

enum class StickPolicy {
    NoneStrict,
    None,
    RandomStrict,
    Random,
    Swapping,
    SwappingLazy,
    SwappingBlocking,
    Permutation
};

namespace detail {

template <typename Impl, StickPolicy>
struct StickPolicyImpl;

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::NoneStrict> {
    using type = NoStickingStrict<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::None> {
    using type = NoSticking<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::RandomStrict> {
    using type = RandomStrict<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::Random> {
    using type = Random<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::Swapping> {
    using type = Swapping<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::SwappingLazy> {
    using type = SwappingLazy<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::SwappingBlocking> {
    using type = SwappingBlocking<Impl>;
};

template <typename Impl>
struct StickPolicyImpl<Impl, StickPolicy::Permutation> {
    using type = GlobalPermutation<Impl>;
};

template <typename Impl, StickPolicy P>
using stick_policy_impl_type = typename detail::StickPolicyImpl<Impl, P>::type;

}  // namespace detail

}  // namespace multiqueue
