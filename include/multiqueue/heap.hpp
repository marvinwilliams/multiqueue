/**
******************************************************************************
* @file:   heap.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 17:26
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef HEAP_HPP_INCLUDED
#define HEAP_HPP_INCLUDED

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>  // allocator
#include <sstream>
#include <string>
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward, pair
#include <vector>

namespace multiqueue {

#ifdef HEAP_DEBUG

#define HEAP_ASSERT(x) \
    do {               \
        assert(x);     \
    } while (false)

#else

#define HEAP_ASSERT(x) \
    do {               \
    } while (false)

#endif

template <typename T, typename Compare = std::less<>, unsigned int Degree = 8, typename Container = std::vector<T>>
class Heap {
    static_assert(Degree >= 2, "Degree must be at least two");

   public:
    using value_type = T;
    using value_compare = Compare;
    using container_type = Container;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;

    using size_type = typename container_type::size_type;

    enum struct Location { Heap };

   private:
    static constexpr size_type degree_ = Degree;
    static constexpr size_type root = size_type{0};

    container_type data_;
    [[no_unique_address]] value_compare comp_;

   private:
    static constexpr size_type parent(size_type index) noexcept {
        HEAP_ASSERT(index != root);
        return (index - size_type(1)) / degree_;
    }

    static constexpr size_type first_child(size_type index) noexcept {
        return index * degree_ + size_type(1);
    }

    // returns the index of the first node without all children
    constexpr size_type current_parrent() const noexcept {
        HEAP_ASSERT(!empty());
        return parent(size());
    }

    // Find the index of the smallest node smaller than provided val
    // If no index is smaller than parent, return parent
    size_type top_child(size_type first, size_type last, value_type const &val) const {
        HEAP_ASSERT(first <= last);
        HEAP_ASSERT(last <= size());
        for (; first != last; ++first) {
            if (comp_(data_[first], val)) {
                break;
            }
        }
        auto top = first;
        for (; first != last; ++first) {
            if (comp_(data_[first], data_[top])) {
                top = first;
            }
        }
        return top;
    }

    template <typename Info>
    void sift_up(size_type index, Info *info) {
        HEAP_ASSERT(index < size());
        if (index == root) {
            return;
        }
        value_type value = std::move(data_[index]);
        size_type p = parent(index);
        while (comp_(value, data_[p])) {
            info[static_cast<std::size_t>(data_[p].second)].index = index;
            data_[index] = std::move(data_[p]);
            index = p;
            if (index == root) {
                break;
            }
            p = parent(index);
        }
        info[static_cast<std::size_t>(value.second)].index = index;
        data_[index] = std::move(value);
    }

    template <typename Info>
    void sift_down(size_type index, Info *info) {
        HEAP_ASSERT(index < size());
        value_type value = std::move(data_[index]);
        size_type const first_nonfull = current_parrent();
        while (index < first_nonfull) {
            auto const first = first_child(index);
            auto const next = top_child(first, first + degree_, value);
            if (next == first + degree_) {
                info[static_cast<std::size_t>(value.second)].index = index;
                data_[index] = std::move(value);
                return;
            }
            info[static_cast<std::size_t>(data_[next].second)].index = index;
            data_[index] = std::move(data_[next]);
            index = next;
        }
        if (index == first_nonfull) {
            auto const first = first_child(index);
            auto const next = top_child(first, size(), value);
            if (next != size()) {
                info[static_cast<std::size_t>(data_[next].second)].index = index;
                data_[index] = std::move(data_[next]);
                index = next;
            }
        }
        info[static_cast<std::size_t>(value.second)].index = index;
        data_[index] = std::move(value);
    }

   public:
    explicit Heap(value_compare const &comp = value_compare()) noexcept : data_(), comp_{comp} {
    }

    template <typename Alloc>
    explicit Heap(value_compare const &comp, Alloc const &alloc) noexcept : data_(alloc), comp_{comp} {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return data_.empty();
    }

    constexpr size_type size() const noexcept {
        return data_.size();
    }

    constexpr const_reference top() const {
        return data_.front();
    }

    template <typename Info>
    void pop(Info *info) {
        HEAP_ASSERT(!empty());
        if (size() > size_type(1)) {
            info[static_cast<std::size_t>(data_.back().second)].index = 0;
            data_.front() = std::move(data_.back());
            data_.pop_back();
            sift_down(0, info);
        } else {
            data_.pop_back();
        }
        HEAP_ASSERT(verify(info));
    }

    template <typename Info>
    void extract_top(reference retval, Info *info) {
        retval = std::move(data_.front());
        pop(info);
        HEAP_ASSERT(verify(info));
    }

    template <typename Info>
    void push(const_reference value, Info *info) {
        info[static_cast<std::size_t>(value.second)].index = data_.size();
        data_.push_back(value);
        sift_up(size() - 1, info);
        HEAP_ASSERT(verify(info));
    }

    template <typename Info>
    void push(value_type &&value, Info *info) {
        info[static_cast<std::size_t>(value.second)].index = data_.size();
        data_.push_back(std::move(value));
        sift_up(size() - 1, info);
        HEAP_ASSERT(verify(info));
    }

    template <typename Info>
    void update(const_reference new_val, Info *info) {
        std::size_t i = info[static_cast<std::size_t>(new_val.second)].index;
        HEAP_ASSERT(i < size());
        HEAP_ASSERT(data_[i].second == new_val.second);
        if (new_val.first > data_[i].first) {
            data_[i].first = new_val.first;
            sift_down(i, info);
        } else {
            data_[i].first = new_val.first;
            sift_up(i, info);
        }
        HEAP_ASSERT(verify(info));
    }

    template <typename Info>
    void erase(typename T::second_type value, Info *info) {
        std::size_t i = info[static_cast<std::size_t>(value)].index;
        HEAP_ASSERT(i < size());
        HEAP_ASSERT(data_[i].second == value);
        if (i == size() - 1) {
            data_.pop_back();
            HEAP_ASSERT(verify(info));
            return;
        }
        info[static_cast<std::size_t>(data_.back().second)].index = i;
        if (comp_(data_[i], data_.back())) {
            data_[i] = std::move(data_.back());
            data_.pop_back();
            sift_down(i, info);
        } else {
            data_[i] = std::move(data_.back());
            data_.pop_back();
            sift_up(i, info);
        }
        HEAP_ASSERT(verify(info));
    }

    void reserve(size_type cap) {
        data_.reserve(cap);
    }

    constexpr void clear() noexcept {
        data_.clear();
    }

    constexpr value_compare value_comp() const {
        return comp_;
    }

    template <typename Info>
    bool verify(Info *info) const noexcept {
        if (empty()) {
            return true;
        }
        if (info[top().second].index != 0) {
            return false;
        }
        for (size_type i = 0; i < size(); i++) {
            auto const first = first_child(i);
            for (size_type j = 0; j < Degree; ++j) {
                if (first + j >= size()) {
                    return true;
                }
                if (info[data_[first + j].second].index != first + j) {
                    return false;
                }
                if (comp_(data_[first + j], data_[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string description() {
        std::stringstream ss;
        ss << "Heap degree: " << Degree;
        return ss.str();
    }
};

#undef HEAP_ASSERT

}  // namespace multiqueue

namespace std {

template <typename T, typename Compare, unsigned int Degree, typename Container, typename Alloc>
struct uses_allocator<multiqueue::Heap<T, Compare, Degree, Container>, Alloc> : uses_allocator<Container, Alloc>::type {
};

}  // namespace std

#endif  //! HEAP_HPP_INCLUDED
