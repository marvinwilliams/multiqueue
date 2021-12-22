/**
******************************************************************************
* @file:   buffered_pq.hpp
*
* @author: Marvin Williams
* @date:   2021/09/14 17:54
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef BUFFERED_PQ_HPP_INCLUDED
#define BUFFERED_PQ_HPP_INCLUDED

#include "multiqueue/addressable.hpp"
#include "multiqueue/buffer.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/value.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {

template <typename PriorityQueue, std::size_t InsertionBufferSize, std::size_t DeletionBufferSize>
class BufferedPQ {
   private:
    using pq_type = PriorityQueue;

   public:
    using value_type = typename pq_type::value_type;
    using value_compare = typename pq_type::value_compare;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = std::size_t;

   private:
    using insertion_buffer_type = Buffer<value_type, InsertionBufferSize>;
    using deletion_buffer_type = RingBuffer<value_type, DeletionBufferSize>;

    insertion_buffer_type insertion_buffer_;
    deletion_buffer_type deletion_buffer_;
    pq_type pq_;

   private:
    template <typename Info>
    void flush_insertion_buffer(Info* info) {
        std::for_each(insertion_buffer_.begin(), insertion_buffer_.end(), [this, info](value_type& v) {
            assert(info[static_cast<std::size_t>(v.second)].location == Location::InsertionBuffer);
            info[static_cast<std::size_t>(v.second)].location = Location::Heap;
            pq_.push(std::move(v), info);
        });
        insertion_buffer_.clear();
    }

    template <typename Info>
    void refill_deletion_buffer(Info* info) {
        assert(deletion_buffer_.empty());
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer(info);
        size_type num_refill = std::min(deletion_buffer_type::Capacity, pq_.size());
        for (; num_refill != 0; --num_refill) {
            Info* top_info = info + static_cast<std::size_t>(pq_.top().second);
            assert(top_info->location == Location::Heap);
            top_info->location = Location::DeletionBuffer;
            top_info->index = deletion_buffer_.size();
            deletion_buffer_.push_back(pq_.top());
            pq_.pop(info);
        }
    }

   public:
    explicit BufferedPQ(value_compare const& comp = value_compare()) : pq_(comp) {
    }

    template <typename Allocator>
    explicit BufferedPQ(value_compare const& comp, Allocator const& alloc) : pq_(comp, alloc) {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return deletion_buffer_.empty();
    }

    constexpr size_type size() const noexcept {
        return insertion_buffer_.size() + deletion_buffer_.size() + pq_.size();
    }

    constexpr const_reference top() const {
        assert(!empty());
        return deletion_buffer_.front();
    }

    template <typename Info>
    void pop(Info* info) {
        assert(!empty());
        deletion_buffer_.pop_front();
        for (auto it = deletion_buffer_.begin(); it != deletion_buffer_.end(); ++it) {
            assert(info[static_cast<std::size_t>(it->second)].index > 0);
            --info[static_cast<std::size_t>(it->second)].index;
        }
        if (deletion_buffer_.empty()) {
            refill_deletion_buffer(info);
        }
        assert(verify(info));
    }

    template <typename Info>
    void extract_top(reference retval, Info* info) {
        assert(!empty());
        retval = std::move(deletion_buffer_.front());
        pop(info);
    }

    template <typename Info>
    void push(const_reference value, Info* info) {
        Info* val_info = info + static_cast<std::size_t>(value.second);
        if (empty()) {
            val_info->location = Location::DeletionBuffer;
            val_info->index = deletion_buffer_.size();
            deletion_buffer_.push_back(value);
            assert(verify(info));
            return;
        }
        auto it = std::find_if(deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                               [&value, this](const_reference entry) { return pq_.value_comp()(entry, value); });
        if (it == deletion_buffer_.rbegin()) {
            // Insert into insertion buffer
            if (!insertion_buffer_.full()) {
                val_info->location = Location::InsertionBuffer;
                val_info->index = insertion_buffer_.size();
                insertion_buffer_.push_back(value);
            } else {
                // Could also do a merging refill into the deletion buffer
                flush_insertion_buffer(info);
                val_info->location = Location::Heap;
                pq_.push(value, info);
            }
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                Info* back_info = info + static_cast<std::size_t>(deletion_buffer_.back().second);
                if (!insertion_buffer_.full()) {
                    back_info->location = Location::InsertionBuffer;
                    back_info->index = insertion_buffer_.size();
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                } else {
                    flush_insertion_buffer(info);
                    back_info->location = Location::Heap;
                    pq_.push(std::move(deletion_buffer_.back()), info);
                }
                deletion_buffer_.pop_back();
            }
            val_info->location = Location::DeletionBuffer;
            val_info->index = static_cast<std::size_t>(it.base() - deletion_buffer_.begin());
            std::for_each(it.base(), deletion_buffer_.end(), [info](auto const& v) {
                assert(info[static_cast<std::size_t>(v.second)].location == Location::DeletionBuffer);
                ++info[static_cast<std::size_t>(v.second)].index;
            });
            deletion_buffer_.insert(it.base(), value);
        }
        assert(verify(info));
    }

    template <typename Info>
    void push(value_type&& value, Info* info) {
        Info* val_info = info + static_cast<std::size_t>(value.second);
        if (empty()) {
            val_info->location = Location::DeletionBuffer;
            val_info->index = deletion_buffer_.size();
            deletion_buffer_.push_back(std::move(value));
            assert(verify(info));
            return;
        }
        auto it = std::find_if(deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                               [&value, this](const_reference entry) { return pq_.value_comp()(entry, value); });
        if (it == deletion_buffer_.rbegin()) {
            // Insert into insertion buffer
            if (!insertion_buffer_.full()) {
                val_info->location = Location::InsertionBuffer;
                val_info->index = insertion_buffer_.size();
                insertion_buffer_.push_back(std::move(value));
            } else {
                // Could also do a merging refill into the deletion buffer
                flush_insertion_buffer(info);
                val_info->location = Location::Heap;
                pq_.push(std::move(value), info);
            }
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                Info* back_info = info + static_cast<std::size_t>(deletion_buffer_.back().second);
                if (!insertion_buffer_.full()) {
                    back_info->location = Location::InsertionBuffer;
                    back_info->index = insertion_buffer_.size();
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                } else {
                    flush_insertion_buffer(info);
                    back_info->location = Location::Heap;
                    pq_.push(std::move(deletion_buffer_.back()), info);
                }
                deletion_buffer_.pop_back();
            }
            val_info->location = Location::DeletionBuffer;
            val_info->index = static_cast<std::size_t>(it.base() - deletion_buffer_.begin());
            std::for_each(it.base(), deletion_buffer_.end(), [info](auto const& v) {
                assert(info[static_cast<std::size_t>(v.second)].location == Location::DeletionBuffer);
                ++info[static_cast<std::size_t>(v.second)].index;
            });
            deletion_buffer_.insert(it.base(), std::move(value));
        }
        assert(verify(info));
    }

    template <typename Info>
    void update(const_reference new_val, Info* info) {
        Info* val_info = info + static_cast<std::size_t>(new_val.second);
        std::size_t const i = val_info->index;
        switch (info[static_cast<std::size_t>(new_val.second)].location) {
            case Location::InsertionBuffer: {
                // The element is in the insertion buffer.
                // a) If the priority decreases, we can just update the key.
                // b) If the priority increases, we  check if belongs into the
                //    deletion buffer (which can't be empty per invariant).
                //    If yes, we insert it into the deletion buffer and push
                //    its largest element into the insertion buffer if necessary
                assert(insertion_buffer_[i].second == new_val.second);
                if (pq_.value_comp()(new_val, insertion_buffer_[i])) {
                    assert(!deletion_buffer_.empty());
                    auto it = std::find_if(
                        deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                        [this, &new_val](const_reference entry) { return pq_.value_comp()(entry, new_val); });
                    if (it == deletion_buffer_.rbegin()) {
                        insertion_buffer_[i].first = new_val.first;
                    } else {
                        if (deletion_buffer_.full()) {
                            Info* back_info = info + static_cast<std::size_t>(deletion_buffer_.back().second);
                            back_info->location = Location::InsertionBuffer;
                            back_info->index = i;
                            insertion_buffer_[i] = std::move(deletion_buffer_.back());
                            deletion_buffer_.pop_back();
                        } else {
                            if (i != insertion_buffer_.size() - 1) {
                                info[static_cast<std::size_t>(insertion_buffer_.back().second)].index = i;
                                insertion_buffer_[i] = std::move(insertion_buffer_.back());
                            }
                            insertion_buffer_.pop_back();
                        }
                        val_info->location = Location::DeletionBuffer;
                        val_info->index = static_cast<std::size_t>(it.base() - deletion_buffer_.begin());
                        std::for_each(deletion_buffer_.rbegin(), it,
                                      [info](auto const& v) { ++info[static_cast<std::size_t>(v.second)].index; });
                        deletion_buffer_.insert(it.base(), new_val);
                    }
                } else if (pq_.value_comp()(insertion_buffer_[i], new_val)) {
                    insertion_buffer_[i].first = new_val.first;
                }
                break;
            }
            case Location::DeletionBuffer: {
                // The element is in the deletion buffer.
                // a) If the priority increases, we can just update the key and move the element to the correct
                // position. b) If the priority decreases, we  check if it still belongs into the
                //    deletion buffer. If yes, we update the key and move it into the correct position
                //    Otherwise, we push it into the insertion buffer. If the
                //    insertion buffer overflows, we flush it into the pq. If
                //    the deletion buffer becomes empty, we need to refill it
                //    to uphold the invariant.
                assert(deletion_buffer_[i].second == new_val.second);
                auto const elem_it = deletion_buffer_.begin() + static_cast<std::ptrdiff_t>(i);
                if (pq_.value_comp()(new_val, deletion_buffer_[i])) {
                    auto it = std::find_if(deletion_buffer_.begin(), elem_it, [this, &new_val](const_reference entry) {
                        return pq_.value_comp()(new_val, entry);
                    });
                    if (it != elem_it) {
                        std::for_each(it, elem_it,
                                      [info](auto const& v) { ++info[static_cast<std::size_t>(v.second)].index; });
                        auto const pos = static_cast<std::size_t>(it - deletion_buffer_.begin());
                        val_info->index = pos;
                        deletion_buffer_.erase(elem_it);
                        deletion_buffer_.insert(deletion_buffer_.cbegin() + static_cast<ptrdiff_t>(pos), new_val);
                    } else {
                        deletion_buffer_[i].first = new_val.first;
                    }
                } else if (pq_.value_comp()(deletion_buffer_[i], new_val)) {
                    auto it = std::find_if(
                        std::next(elem_it), deletion_buffer_.end(),
                        [this, &new_val](const_reference entry) { return pq_.value_comp()(new_val, entry); });
                    if (it == deletion_buffer_.end()) {
                        // The element is pushed out of the deletion buffer
                        // Move it into the insertion buffer if possible, flush otherwise
                        std::for_each(std::next(elem_it), deletion_buffer_.end(),
                                      [info](auto const& v) { --info[static_cast<std::size_t>(v.second)].index; });
                        deletion_buffer_.erase(elem_it);
                        if (!insertion_buffer_.full()) {
                            val_info->location = Location::InsertionBuffer;
                            val_info->index = insertion_buffer_.size();
                            insertion_buffer_.push_back(new_val);
                        } else {
                            flush_insertion_buffer(info);
                            val_info->location = Location::Heap;
                            pq_.push(new_val, info);
                        }
                        if (deletion_buffer_.empty()) {
                            refill_deletion_buffer(info);
                        }
                    } else if (it != std::next(elem_it)) {
                        // The element is moved back (but not to the end) in the deletion buffer
                        std::for_each(std::next(elem_it), it, [info](auto const& v) {
                            assert(info[static_cast<std::size_t>(v.second)].index > 0);
                            --info[static_cast<std::size_t>(v.second)].index;
                        });
                        --it;
                        auto const pos = static_cast<std::size_t>(it - deletion_buffer_.begin());
                        val_info->index = pos;
                        deletion_buffer_.erase(elem_it);
                        deletion_buffer_.insert(deletion_buffer_.cbegin() + static_cast<std::ptrdiff_t>(pos), new_val);
                    } else {
                        // The element is not the last and did not move its position, just update the key
                        deletion_buffer_[i].first = new_val.first;
                    }
                }
                break;
            }
            case Location::Heap: {
                // The element is in the heap. If it now goes into the deletion buffer, we delete it from the heap and
                // insert it
                assert(!deletion_buffer_.empty());
                auto it =
                    std::find_if(deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                                 [this, &new_val](const_reference entry) { return pq_.value_comp()(entry, new_val); });
                if (it == deletion_buffer_.rbegin()) {
                    // The element stays in the heap
                    pq_.update(new_val, info);
                } else {
                    // The element goes into the deletion buffer
                    pq_.erase(new_val.second, info);
                    if (deletion_buffer_.full()) {
                        info[static_cast<std::size_t>(deletion_buffer_.back().second)].location = Location::Heap;
                        pq_.push(std::move(deletion_buffer_.back()), info);
                        deletion_buffer_.pop_back();
                    }
                    val_info->location = Location::DeletionBuffer;
                    val_info->index = static_cast<std::size_t>(it.base() - deletion_buffer_.begin());
                    std::for_each(it.base(), deletion_buffer_.end(),
                                  [info](auto const& v) { ++info[static_cast<std::size_t>(v.second)].index; });
                    deletion_buffer_.insert(it.base(), new_val);
                }
                break;
            }
        }
        assert(verify(info));
    }

    void reserve(size_type cap) {
        pq_.reserve(cap);
    }

    constexpr void clear() noexcept {
        insertion_buffer_.clear();
        deletion_buffer_.clear();
        pq_.clear();
    }

    constexpr value_compare value_comp() const {
        return pq_.value_comp();
    }

    template <typename Info>
    bool verify(Info* info) const noexcept {
        if (deletion_buffer_.empty() && (!insertion_buffer_.empty() || !pq_.empty())) {
            return false;
        }
        for (auto it = insertion_buffer_.begin(); it != insertion_buffer_.end(); ++it) {
            if ((info[static_cast<std::size_t>(it->second)].location != Location::InsertionBuffer) ||
                (info[static_cast<std::size_t>(it->second)].index !=
                 static_cast<std::size_t>(it - insertion_buffer_.begin()))) {
                return false;
            }
        }
        for (auto it = deletion_buffer_.begin(); it != deletion_buffer_.end(); ++it) {
            if ((info[static_cast<std::size_t>(it->second)].location != Location::DeletionBuffer) ||
                info[static_cast<std::size_t>(it->second)].index !=
                    static_cast<std::size_t>(it - deletion_buffer_.begin())) {
                return false;
            }
        }
        return pq_.verify(info);
    }

    static std::string description() {
        std::stringstream ss;
        ss << "InsertionBufferSize: " << InsertionBufferSize << "\n\t";
        ss << "DeletionBufferSize: " << DeletionBufferSize << "\n\t";
        ss << pq_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

namespace std {
template <typename PriorityQueue, std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<PriorityQueue, InsertionBufferSize, DeletionBufferSize>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std

#endif  //! BUFFERED_PQ_HPP_INCLUDED
