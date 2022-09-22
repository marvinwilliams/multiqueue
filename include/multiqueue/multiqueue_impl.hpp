/**
******************************************************************************
* @file:   multiqueue_impl.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once

#include "multiqueue/build_config.hpp"
#include "multiqueue/config.hpp"
#include "multiqueue/stick_policy.hpp"
#include "multiqueue/third_party/pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <utility>

namespace multiqueue {

template <typename Base, StickPolicy>
struct MultiQueueImpl;

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::None> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept : rng_{impl.rng()}, impl_{impl} {
        }

       public:
        void push(const_reference value) noexcept {
            size_type index;
            do {
                index = impl_.random_index(rng_);
            } while (!impl_.pq_list[index].try_lock());
            impl_.pq_list[index].unsafe_push(value);
            impl_.pq_list[index].unlock();
        }

        bool try_pop(reference retval) noexcept {
            do {
                std::array<size_type, 2> index = {impl_.random_index(rng_), impl_.random_index(rng_)};
                while (index[0] == index[1]) {
                    index[1] = impl_.random_index(rng_);
                }
                std::array<key_type, 2> key = {impl_.pq_list[index[0]].concurrent_top_key(),
                                               impl_.pq_list[index[1]].concurrent_top_key()};
                std::size_t select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    return false;
                }
                size_type select_index = index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare) {
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: None\n";
        return out;
    }
};

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::RandomStrict> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;
        std::array<size_type, 2> stick_index_;
        int use_count_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              stick_index_{impl_.random_index(rng_), impl_.random_index(rng_)},
              use_count_{2 * impl_.stickiness} {
            while (stick_index_[0] == stick_index_[1]) {
                stick_index_[1] = impl_.random_index(rng_);
            }
        }

       public:
        void push(const_reference value) noexcept {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            if (use_count_ <= 0 || !impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    stick_index_[push_pq] = impl_.random_index(rng_);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                do {
                    stick_index_[1 - push_pq] = impl_.random_index(rng_);
                } while (stick_index_[0] == stick_index_[1]);
                use_count_ = 2 * impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_ > 0);
            --use_count_;
        }

        bool try_pop(reference retval) noexcept {
            if (use_count_ > 0) {
                std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                               impl_.pq_list[stick_index_[1]].concurrent_top_key()};
                std::size_t select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_ = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        use_count_ -= 2;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            }
            do {
                stick_index_[0] = impl_.random_index(rng_);
                do {
                    stick_index_[1] = impl_.random_index(rng_);
                } while (stick_index_[0] == stick_index_[1]);
                std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                               impl_.pq_list[stick_index_[1]].concurrent_top_key()};
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_ = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        use_count_ = 2 * impl_.stickiness - 2;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    int stickiness;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Random Strict\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::Random> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              stick_index_{impl_.random_index(rng_), impl_.random_index(rng_)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

       public:
        void push(const_reference value) noexcept {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            if (use_count_[push_pq] == 0 || !impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    stick_index_[push_pq] = impl_.random_index(rng_);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(reference retval) noexcept {
            if (use_count_[0] == 0) {
                stick_index_[0] = impl_.random_index(rng_);
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                stick_index_[1] = impl_.random_index(rng_);
                use_count_[1] = impl_.stickiness;
            }
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        --use_count_[0];
                        --use_count_[1];
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                stick_index_[select_pq] = impl_.random_index(rng_);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    int stickiness;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Random\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::Swapping> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;
        std::size_t permutation_index_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              permutation_index_{impl_.handle_count * 2},
              stick_index_{impl_.permutation[permutation_index_].i.load(std::memory_order_relaxed),
                           impl_.permutation[permutation_index_ + 1].i.load(std::memory_order_relaxed)},
              use_count_{impl_.stickiness, impl_.stickiness} {
            ++impl_.handle_count;
        }

        void swap_assignment(std::size_t pq) noexcept {
            assert(pq <= 1);
            if (!impl_.permutation[permutation_index_ + pq].i.compare_exchange_strong(stick_index_[pq], impl_.num_pqs,
                                                                                      std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(stick_index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index = 0;
            size_type target_assigned;
            do {
                target_index = impl_.random_index(rng_);
                target_assigned = impl_.permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !impl_.permutation[target_index].i.compare_exchange_strong(target_assigned, stick_index_[pq],
                                                                                std::memory_order_relaxed));
            impl_.permutation[permutation_index_ + pq].i.store(target_assigned, std::memory_order_relaxed);
            stick_index_[pq] = target_assigned;
        }

        void refresh_pq(std::size_t pq) noexcept {
            if (use_count_[pq] != 0) {
                auto current_index = impl_.permutation[permutation_index_ + pq].i.load(std::memory_order_relaxed);
                if (current_index == stick_index_[pq]) {
                    return;
                }
                stick_index_[pq] = current_index;
            } else {
                swap_assignment(pq);
            }
            use_count_[pq] = impl_.stickiness;
        }

       public:
        void push(const_reference value) {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            refresh_pq(push_pq);
            if (!impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    swap_assignment(push_pq);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(reference retval) {
            refresh_pq(0);
            refresh_pq(1);
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        --use_count_[0];
                        --use_count_[1];
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                swap_assignment(select_pq);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    struct alignas(BuildConfiguration::L1CacheLinesize) AlignedIndex {
        std::atomic<size_type> i;
    };

    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    int stickiness;
    int handle_count = 0;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare), permutation(this->num_pqs), stickiness{config.stickiness} {
        for (std::size_t i = 0; i < this->num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Swapping\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::SwappingLazy> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;
        std::size_t permutation_index_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              permutation_index_{impl_.handle_count * 2},
              stick_index_{impl_.permutation[permutation_index_].i.load(std::memory_order_relaxed),
                           impl_.permutation[permutation_index_ + 1].i.load(std::memory_order_relaxed)},
              use_count_{impl_.stickiness, impl_.stickiness} {
            ++impl_.handle_count;
        }

        void swap_assignment(std::size_t pq) noexcept {
            assert(pq <= 1);
            if (!impl_.permutation[permutation_index_ + pq].i.compare_exchange_strong(stick_index_[pq], impl_.num_pqs,
                                                                                      std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(stick_index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index = 0;
            size_type target_assigned;
            do {
                target_index = impl_.random_index(rng_);
                target_assigned = impl_.permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !impl_.permutation[target_index].i.compare_exchange_strong(target_assigned, stick_index_[pq],
                                                                                std::memory_order_relaxed));
            impl_.permutation[permutation_index_ + pq].i.store(target_assigned, std::memory_order_relaxed);
            stick_index_[pq] = target_assigned;
        }

       public:
        void push(const_reference value) {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            if (use_count_[push_pq] == 0 || !impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                do {
                    swap_assignment(push_pq);
                } while (!impl_.pq_list[stick_index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(reference retval) {
            if (use_count_[0] == 0) {
                swap_assignment(0);
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                swap_assignment(1);
                use_count_[1] = impl_.stickiness;
            }
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        --use_count_[0];
                        --use_count_[1];
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                swap_assignment(select_pq);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    struct alignas(BuildConfiguration::L1CacheLinesize) AlignedIndex {
        std::atomic<size_type> i;
    };

    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    int stickiness;
    int handle_count = 0;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare), permutation(this->num_pqs), stickiness{config.stickiness} {
        for (std::size_t i = 0; i < this->num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Swapping Lazy\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::SwappingBlocking> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;
        std::size_t permutation_index_;
        std::array<size_type, 2> stick_index_;
        std::array<int, 2> use_count_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              permutation_index_{impl_.handle_count * 2},
              stick_index_{impl_.permutation[permutation_index_].i.load(std::memory_order_relaxed),
                           impl_.permutation[permutation_index_ + 1].i.load(std::memory_order_relaxed)},
              use_count_{impl_.stickiness, impl_.stickiness} {
            ++impl_.handle_count;
        }

        void swap_assignment(std::size_t pq) noexcept {
            assert(pq <= 1);
            if (!impl_.permutation[permutation_index_ + pq].i.compare_exchange_strong(stick_index_[pq], impl_.num_pqs,
                                                                                      std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(stick_index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index = 0;
            size_type target_assigned;
            do {
                target_index = impl_.random_index(rng_);
                target_assigned = impl_.permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !impl_.permutation[target_index].i.compare_exchange_strong(target_assigned, stick_index_[pq],
                                                                                std::memory_order_relaxed));
            impl_.permutation[permutation_index_ + pq].i.store(target_assigned, std::memory_order_relaxed);
            stick_index_[pq] = target_assigned;
        }

        void refresh_pq(std::size_t pq) noexcept {
            if (use_count_[pq] != 0) {
                auto current_index = impl_.permutation[permutation_index_ + pq].i.load(std::memory_order_relaxed);
                if (current_index == stick_index_[pq]) {
                    return;
                }
                stick_index_[pq] = current_index;
            } else {
                swap_assignment(pq);
            }
            use_count_[pq] = impl_.stickiness;
        }

       public:
        void push(const_reference value) {
            std::size_t const push_pq = std::bernoulli_distribution{}(rng_) ? 1 : 0;
            refresh_pq(push_pq);
            while (!impl_.pq_list[stick_index_[push_pq]].try_lock()) {
                auto current_index = impl_.permutation[permutation_index_ + push_pq].i.load(std::memory_order_relaxed);
                if (current_index != stick_index_[push_pq]) {
                    stick_index_[push_pq] = current_index;
                    use_count_[push_pq] = impl_.stickiness;
                }
            }
            impl_.pq_list[stick_index_[push_pq]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq]].unlock();
            assert(use_count_[push_pq] > 0);
            --use_count_[push_pq];
        }

        bool try_pop(reference retval) {
            refresh_pq(0);
            refresh_pq(1);
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::array<key_type, 2> key = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                           impl_.pq_list[stick_index_[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                size_type select_index = stick_index_[select_pq];
                while (true) {
                    if (impl_.pq_list[select_index].try_lock()) {
                        if (!impl_.pq_list[select_index].unsafe_empty()) {
                            retval = impl_.pq_list[select_index].unsafe_top();
                            impl_.pq_list[select_index].unsafe_pop();
                            impl_.pq_list[select_index].unlock();
                            --use_count_[0];
                            --use_count_[1];
                            return true;
                        }
                        impl_.pq_list[select_index].unlock();
                        key[select_pq] = Sentinel::get();
                        break;
                    }
                    auto current_index =
                        impl_.permutation[permutation_index_ + select_pq].i.load(std::memory_order_relaxed);
                    if (current_index != stick_index_[select_pq]) {
                        stick_index_[select_pq] = current_index;
                        use_count_[select_pq] = impl_.stickiness;
                        key[select_pq] = impl_.pq_list[stick_index_[select_pq]].concurrent_top_key();
                        break;
                    }
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    struct alignas(BuildConfiguration::L1CacheLinesize) AlignedIndex {
        std::atomic<size_type> i;
    };

    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    int stickiness;
    int handle_count = 0;

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(num_threads * config.c, config, compare), permutation(this->num_pqs), stickiness{config.stickiness} {
        for (std::size_t i = 0; i < this->num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Swapping Blocking\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
template <typename Base>
struct MultiQueueImpl<Base, StickPolicy::Permutation> : public Base {
    using reference = typename Base::reference;
    using const_reference = typename Base::const_reference;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;
    using key_compare = typename Base::key_compare;
    using size_type = typename Base::size_type;
    using Sentinel = typename Base::Sentinel;

    class Handle {
        friend MultiQueueImpl;

        pcg32 rng_;
        MultiQueueImpl &impl_;

        std::size_t permutation_index_;
        std::uint64_t current_permutation_;
        int use_count_{};

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;

       private:
        explicit Handle(MultiQueueImpl &impl) noexcept
            : rng_{impl.rng()},
              impl_{impl},
              permutation_index_{impl_.handle_count * 2},
              current_permutation_{impl_.permutation.load(std::memory_order_relaxed)} {
            ++impl_.handle_count;
        }

        void update_permutation() {
            std::uint64_t new_permutation = rng_() | 1;
            if (impl_.permutation.compare_exchange_strong(current_permutation_, new_permutation,
                                                          std::memory_order_relaxed)) {
                current_permutation_ = new_permutation;
            }
        }

        size_type get_index(std::size_t pq) const noexcept {
            static constexpr int shift_width = 32;
            size_type a = current_permutation_ & Mask;
            size_type b = current_permutation_ >> shift_width;
            assert((a & 1) == 1);
            return ((permutation_index_ + pq) * a + b) & (impl_.num_pqs - 1);
        }

        void refresh_permutation() {
            if (use_count_ >= 2 * impl_.stickiness) {
                auto p = impl_.permutation.load(std::memory_order_relaxed);
                if (p == current_permutation_) {
                    return;
                }
                current_permutation_ = p;
            } else {
                update_permutation();
            }
            use_count_ = 0;
        }

       public:
        void push(const_reference value) noexcept {
            refresh_permutation();
            bool push_pq = std::bernoulli_distribution{}(rng_);
            size_type pq_index = get_index(push_pq);
            while (!impl_.pq_list[pq_index].try_lock()) {
                auto p = impl_.permutation.load(std::memory_order_relaxed);
                if (p != current_permutation_) {
                    current_permutation_ = p;
                    use_count_ = 0;
                }
            }
            impl_.pq_list[pq_index].unsafe_push(value);
            impl_.pq_list[pq_index].unlock();
            ++use_count_;
        }

        bool try_pop(reference retval) noexcept {
            refresh_permutation();
            assert(use_count_ <= 2 * impl_.stickiness);
            std::array<size_type, 2> pq_index = {get_index(0), get_index(1)};
            std::array<key_type, 2> key = {impl_.pq_list[pq_index[0]].concurrent_top_key(),
                                           impl_.pq_list[pq_index[1]].concurrent_top_key()};
            do {
                std::size_t const select_pq = impl_.compare_top_key(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == Sentinel::get()) {
                    use_count_ = 2 * impl_.stickiness;
                    return false;
                }
                size_type select_index = pq_index[select_pq];
                while (true) {
                    if (impl_.pq_list[select_index].try_lock()) {
                        if (!impl_.pq_list[select_index].unsafe_empty()) {
                            retval = impl_.pq_list[select_index].unsafe_top();
                            impl_.pq_list[select_index].unsafe_pop();
                            impl_.pq_list[select_index].unlock();
                            return true;
                        }
                        key[select_index] = Sentinel::get();
                        impl_.pq_list[select_index].unlock();
                        break;
                    }
                    auto p = impl_.permutation.load(std::memory_order_relaxed);
                    if (p != current_permutation_) {
                        current_permutation_ = p;
                        use_count_ = 0;
                        pq_index[0] = get_index(0);
                        pq_index[1] = get_index(1);
                        key[0] = impl_.pq_list[pq_index[0]].concurrent_top_key();
                        key[1] = impl_.pq_list[pq_index[1]].concurrent_top_key();
                        break;
                    }
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using handle_type = Handle;

    static constexpr std::uint64_t Mask = 0xffffffff;

    alignas(BuildConfiguration::L1CacheLinesize) std::atomic_uint64_t permutation;
    int stickiness;
    int handle_count = 0;

    static constexpr std::size_t next_power_of_two(std::size_t n) {
        return std::size_t{1} << static_cast<unsigned int>(std::ceil(std::log2(n)));
    }

    MultiQueueImpl(int num_threads, Config const &config, key_compare const &compare)
        : Base(next_power_of_two(static_cast<std::size_t>(num_threads) * config.c), config, compare),
          stickiness{config.stickiness},
          permutation{1} {
    }

    handle_type get_handle() noexcept {
        return handle_type{*this};
    }

    std::ostream &describe(std::ostream &out) const {
        out << "Stick policy: Permutation\n";
        out << "Stickiness: " << stickiness << '\n';
        return out;
    }
};

}  // namespace multiqueue
