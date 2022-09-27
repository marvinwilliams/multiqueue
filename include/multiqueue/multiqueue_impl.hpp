#pragma once

#include "multiqueue/stick_policy.hpp"

namespace multiqueue {

template <typename Data, StickPolicy>
struct StickPolicyImpl;

}  // namespace multiqueue

#include "multiqueue/stick_policy_none.hpp"
#include "multiqueue/stick_policy_random.hpp"
#include "multiqueue/stick_policy_random_strict.hpp"
#include "multiqueue/stick_policy_swapping.hpp"
#include "multiqueue/stick_policy_swapping_lazy.hpp"
#include "multiqueue/stick_policy_swapping_blocking.hpp"
#include "multiqueue/stick_policy_permutation.hpp"
