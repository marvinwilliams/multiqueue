/**
******************************************************************************
* @file:   fifo.hpp
*
* @author: Marvin Williams
* @date:   2021/03/29 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef MULTIFIFO_HPP_INCLUDED
#define MULTIFIFO_HPP_INCLUDED

#ifndef L1_CACHE_LINESIZE
#define L1_CACHE_LINESIZE 64
#endif

#ifndef PAGESIZE
#define PAGESIZE 4096
#endif

#ifndef MULTIFIFO_DEFAULT_SELECTION_STRATEGY
#define MULTIFIFO_DEFAULT_SELECTION_STRATEGY selection_strategy::Sticky
#endif

#include "multififo/guarded_fifo.hpp"
#include "multififo/selection_strategy/perm.hpp"
#include "multififo/selection_strategy/random.hpp"
#include "multififo/selection_strategy/sticky.hpp"
#include "multififo/selection_strategy/swapping.hpp"

#include "multififo/external/fastrange.h"
#include "multififo/external/xoroshiro256starstar.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace multififo {

template <typename... Configs>
struct MultififoParameters : Configs::Parameters... {
  std::uint64_t seed = 1;
  std::size_t c = 4;
};

template <typename T,
          typename SelectionStrategy = MULTIFIFO_DEFAULT_SELECTION_STRATEGY,
          typename Allocator = std::allocator<T>>
class Multififo {
 private:
  using guarded_fifo_type = GuardedFifo<T>;

 public:
  using tick_type = typename guarded_fifo_type::tick_type;
  using value_type = typename guarded_fifo_type::value_type;
  using reference = typename guarded_fifo_type::reference;
  using const_reference = typename guarded_fifo_type::const_reference;
  using size_type = typename guarded_fifo_type::size_type;
  using allocator_type = Allocator;
  using param_type = MultififoParameters<SelectionStrategy>;

 public:
  class alignas(2 * L1_CACHE_LINESIZE) Handle {
    friend Multififo;
    using data_t = typename SelectionStrategy::handle_data_t;

    Multififo &mf_;
    data_t data_;

   public:
    Handle(Handle const &) = delete;
    Handle &operator=(Handle const &) = delete;
    Handle(Handle &&) = default;

   private:
    explicit Handle(Multififo &mf, unsigned int id, std::uint64_t seed) noexcept
        : mf_{mf}, data_{seed, id} {}

   public:
    bool try_extract_top(reference retval) noexcept {
      auto indices = mf_.selector_.get_delete_indices(data_);
      auto first_tick = mf_.queue_list_[indices.first].get_tick();
      auto second_tick = mf_.queue_list_[indices.second].get_tick();
      if (first_tick == tick_type::max() && second_tick == tick_type::max()) {
        // Both queues are empty
        mf_.selector_.delete_index_used(true, data_);
        return false;
      }
      // put the earlier queue first
      if (second_tick < first_tick) {
        std::swap(indices.first, indices.second);
        std::swap(first_tick, second_tick);
      }
      if (mf_.queue_list_[indices.first].try_lock_if_earlier(second_tick)) {
        mf_.queue_list_[indices.first].extract_top(retval);
        mf_.queue_list_[indices.first].unlock();
        mf_.selector_.delete_index_used(true, data_);
        return true;
      }
      do {
        indices = mf_.selector_.get_fallback_delete_indices(data_);
        first_tick = mf_.queue_list_[indices.first].get_tick();
        second_tick = mf_.queue_list_[indices.second].get_tick();
        if (first_tick == tick_type::max() && second_tick == tick_type::max()) {
          // Both queues are empty
          mf_.selector_.delete_index_used(false, data_);
          return false;
        }
        // put the earlier queue first
        if (second_tick < first_tick) {
          std::swap(indices.first, indices.second);
          std::swap(first_tick, second_tick);
        }
      } while (
          !mf_.queue_list_[indices.first].try_lock_if_earlier(second_tick));
      mf_.queue_list_[indices.first].extract_top(retval);
      mf_.queue_list_[indices.first].unlock();
      mf_.selector_.delete_index_used(false, data_);
      return true;
    }

    void push(const_reference value) noexcept {
      auto index = mf_.selector_.get_push_index(data_);
      if (mf_.queue_list_[index].try_lock()) {
        mf_.queue_list_[index].push(value);
        mf_.queue_list_[index].unlock();
        mf_.selector_.push_index_used(true, data_);
        return;
      }
      do {
        index = mf_.selector_.get_fallback_push_index(data_);
      } while (!mf_.queue_list_[index].try_lock());
      mf_.queue_list_[index].push(value);
      mf_.queue_list_[index].unlock();
      mf_.selector_.push_index_used(false, data_);
    }

    void push(value_type&& value) noexcept {
      auto index = mf_.selector_.get_push_index(data_);
      if (mf_.queue_list_[index].try_lock()) {
        mf_.queue_list_[index].push(std::move(value));
        mf_.queue_list_[index].unlock();
        mf_.selector_.push_index_used(true, data_);
        return;
      }
      do {
        index = mf_.selector_.get_fallback_push_index(data_);
      } while (!mf_.queue_list_[index].try_lock());
      mf_.queue_list_[index].push(std::move(value));
      mf_.queue_list_[index].unlock();
      mf_.selector_.push_index_used(false, data_);
    }

    bool try_extract_from(size_type pos, value_type &retval) noexcept {
      assert(pos < mf_.num_queues());
      auto &queue = mf_.queue_list_[pos];
      if (queue.try_lock()) {
        if (!queue.unsafe_empty()) {
          queue.extract_top(retval);
          queue.unlock();
          return true;
        }
        queue.unlock();
      }
      return false;
    }
  };

 private:
  using queue_alloc_type = typename std::allocator_traits<
      allocator_type>::template rebind_alloc<guarded_fifo_type>;
  using queue_alloc_traits = typename std::allocator_traits<queue_alloc_type>;

  struct QueueDeleter {
    Multififo &mf;

    QueueDeleter(Multififo &mq_ref) : mf{mq_ref} {}

    void operator()(guarded_fifo_type *queue_list) noexcept {
      for (guarded_fifo_type *s = queue_list; s != queue_list + mf.num_queues_;
           ++s) {
        queue_alloc_traits::destroy(mf.alloc_, s);
      }
      queue_alloc_traits::deallocate(mf.alloc_, queue_list, mf.num_queues_);
    }
  };

 private:
  // False sharing is avoided by class alignment, but the members do not need
  // to reside in individual cache lines, as they are not written concurrently
  std::unique_ptr<guarded_fifo_type[], QueueDeleter> queue_list_;
  size_type num_queues_;
  [[no_unique_address]] queue_alloc_type alloc_;
  std::unique_ptr<std::uint64_t[]> handle_seeds_;
  std::atomic_uint handle_index_ = 0;

  // strategy data in separate cache line, as it might be written to
  [[no_unique_address]] alignas(2 *
                                L1_CACHE_LINESIZE) SelectionStrategy selector_;

  void abort_on_data_misalignment() {
    for (guarded_fifo_type *s = queue_list_.get();
         s != queue_list_.get() + num_queues(); ++s) {
      if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
        std::abort();
      }
    }
  }

 public:
  explicit Multififo(unsigned int num_threads, param_type const &params,
                     allocator_type const &alloc = allocator_type())
      : queue_list_(nullptr, QueueDeleter(*this)),
        num_queues_{num_threads * params.c},
        alloc_{alloc},
        selector_(num_queues_, params) {
    assert(num_threads > 0);
    assert(params.c > 0);
    queue_list_.reset(queue_alloc_traits::allocate(
        alloc_, num_queues_));  // Empty unique_ptr does not call deleter
    for (guarded_fifo_type *queue = queue_list_.get();
         queue != queue_list_.get() + num_queues_; ++queue) {
      queue_alloc_traits::construct(alloc_, queue);
    }
#ifdef MULTIQUEUE_ABORT_MISALIGNMENT
    abort_on_data_misalignment();
#endif
    handle_seeds_ = std::make_unique<std::uint64_t[]>(num_threads);
    std::generate(handle_seeds_.get(), handle_seeds_.get() + num_threads,
                  xoroshiro256starstar{params.seed});
  }

  explicit Multififo(size_type /* initial_capacity */, unsigned int num_threads,
                     param_type const &params,
                     allocator_type const &alloc = allocator_type())
      : queue_list_(nullptr, QueueDeleter(*this)),
        num_queues_{num_threads * params.c},
        alloc_{alloc},
        selector_(num_queues_, params) {
    assert(num_threads > 0);
    assert(params.c > 0);
    queue_list_.reset(queue_alloc_traits::allocate(
        alloc_, num_queues_));  // Empty unique_ptr does not call deleter
    for (guarded_fifo_type *queue = queue_list_.get();
         queue != queue_list_.get() + num_queues_; ++queue) {
      queue_alloc_traits::construct(alloc_, queue);
    }
#ifdef MULTIQUEUE_ABORT_MISALIGNMENT
    abort_on_data_misalignment();
#endif
    handle_seeds_ = std::make_unique<std::uint64_t[]>(num_threads);
    std::generate(handle_seeds_.get(), handle_seeds_.get() + num_threads,
                  xoroshiro256starstar{params.seed});
  }

  Handle get_handle() noexcept {
    unsigned int index = handle_index_.fetch_add(1, std::memory_order_relaxed);
    return Handle(*this, index, handle_seeds_[index]);
  }

  constexpr size_type num_queues() const noexcept { return num_queues_; }

  std::string description() const {
    std::stringstream ss;
    ss << "multififo\n\t";
    ss << "Queues: " << num_queues() << "\n\t";
    ss << "Selection strategy: " << selector_.description() << "\n\t";
    return ss.str();
  }
};

}  // namespace multififo

#endif  //! MULTIQUEUE_HPP_INCLUDED
