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

enum class StickPolicy { NoneStrict, None, RandomStrict, Random, Swapping, SwappingLazy, SwappingBlocking, Permutation };

namespace detail {

template <typename ImplData, StickPolicy>
struct StickPolicyImpl;

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::NoneStrict> {
    using type = NoStickingStrict<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::None> {
    using type = NoSticking<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::RandomStrict> {
    using type = RandomStrict<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::Random> {
    using type = Random<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::Swapping> {
    using type = Swapping<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::SwappingLazy> {
    using type = SwappingLazy<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::SwappingBlocking> {
    using type = SwappingBlocking<ImplData>;
};

template <typename ImplData>
struct StickPolicyImpl<ImplData, StickPolicy::Permutation> {
    using type = GlobalPermutation<ImplData>;
};

}  // namespace detail

template <typename ImplData, StickPolicy P>
using stick_policy_impl_type = typename detail::StickPolicyImpl<ImplData, P>::type;

}  // namespace multiqueue
