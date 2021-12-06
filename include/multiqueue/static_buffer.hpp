/**
******************************************************************************
* @file:   static_buffer.hpp
*
* @author: Marvin Williams
* @date:   2021/11/17 14:17
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef STATIC_BUFFER_HPP_INCLUDED
#define STATIC_BUFFER_HPP_INCLUDED

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

namespace multiqueue {

template <typename T, std::size_t Size>
struct BufferStorageBase;

struct Empty {};

template <typename U, bool = std::is_trivially_destructible_v<U>, bool = std::is_trivially_default_constructible_v<U>>
union StorageSlot {
    Empty empty;
    U value;

   public:
    constexpr StorageSlot() noexcept : empty() {
    }

    template <typename... Args>
    constexpr StorageSlot(Args &&...args) : value(std::forward<Args>(args)...) {
    }

    ~StorageSlot() {
    }
};

template <typename U>
union StorageSlot<U, true, false> {
    Empty empty;
    U value;

   public:
    constexpr StorageSlot() noexcept : empty() {
    }

    template <typename... Args>
    constexpr StorageSlot(Args &&...args) : value(std::forward<Args>(args)...) {
    }
};

template <typename U>
union StorageSlot<U, true, true> {
    U value;

   public:
    constexpr StorageSlot() noexcept : value() {
    }

    template <typename... Args>
    constexpr StorageSlot(Args &&...args) : value(std::forward<Args>(args)...) {
    }
};

template <typename T, std::size_t Size>
class StorageIterator {
    friend BufferStorageBase<T, Size>;
    friend std::conditional_t<!std::is_const_v<T>, StorageIterator<T const, Size>,
                              void>;  // const iterator is friend for converting constructor

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type &;
    using const_reference = value_type const &;
    using pointer = value_type *;
    using const_pointer = value_type const *;

   protected:
    using internal_pointer_type = std::conditional_t<std::is_const_v<T>, StorageSlot<std::remove_const_t<T>> const *,
                                                     StorageSlot<std::remove_const_t<T>> *>;

    internal_pointer_type p_;

   public:
    constexpr StorageIterator() noexcept : p_{nullptr} {
    }

    explicit constexpr StorageIterator(internal_pointer_type p) noexcept : p_{p} {
    }

    constexpr StorageIterator(StorageIterator const &other) noexcept : p_{other.p_} {
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    constexpr StorageIterator(StorageIterator<std::remove_const_t<T>, Size> const &other) noexcept : p_{other.p_} {
    }

    constexpr StorageIterator &operator=(StorageIterator const &other) noexcept {
        p_ = other.p_;
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    constexpr StorageIterator &operator=(StorageIterator<std::remove_const_t<T>, Size> const &other) noexcept {
        p_ = other.p_;
    }

    reference operator*() {
        assert(p_);
        return p_->value;
    }

    const_reference operator*() const {
        assert(p_);
        return p_->value;
    }

    pointer operator->() {
        assert(p_);
        return &p_->value;
    }

    const_pointer operator->() const {
        assert(p_);
        return &p_->value;
    }

    constexpr StorageIterator &operator++() noexcept {
        ++p_;
        return *this;
    }

    constexpr StorageIterator operator++(int) noexcept {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    constexpr StorageIterator &operator--() noexcept {
        --p_;
        return *this;
    }

    constexpr StorageIterator operator--(int) noexcept {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    constexpr StorageIterator &operator+=(difference_type n) noexcept {
        p_ += n;
        return *this;
    }

    constexpr StorageIterator operator+(difference_type n) const noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr StorageIterator &operator-=(difference_type n) noexcept {
        p_ -= n;
        return *this;
    }

    constexpr StorageIterator operator-(difference_type n) const noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    const_reference operator[](difference_type n) const {
        return (p_ + n)->value;
    }

    reference operator[](difference_type n) {
        return (p_ + n)->value;
    }

    friend constexpr difference_type operator-(StorageIterator const &lhs, StorageIterator const &rhs) noexcept {
        return lhs.p_ - rhs.p_;
    }

    friend constexpr bool operator==(StorageIterator const &lhs, StorageIterator const &rhs) noexcept {
        return lhs.p_ == rhs.p_;
    }

    friend constexpr bool operator!=(StorageIterator const &lhs, StorageIterator const &rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename Iter, typename IterType>
static constexpr bool is_iter_type_v =
    std::is_base_of_v<IterType, typename std::iterator_traits<Iter>::iterator_category>;

template <typename T, std::size_t N>
struct BufferStorageBase {
    using stored_type = std::remove_const_t<T>;
    using iterator = StorageIterator<T, N>;
    using const_iterator = StorageIterator<T const, N>;

   public:
    using slot_type = StorageSlot<stored_type>;
    StorageSlot<stored_type> slots[N];
    std::size_t size = 0;

   public:
    BufferStorageBase() = default;
    ~BufferStorageBase() = default;

    BufferStorageBase(BufferStorageBase const &) = default;
    BufferStorageBase(BufferStorageBase &&) = default;

    BufferStorageBase &operator=(BufferStorageBase const &) = default;
    BufferStorageBase &operator=(BufferStorageBase &&) = default;

    template <typename Iter, typename = std::enable_if_t<is_iter_type_v<Iter, std::input_iterator_tag>>>
    constexpr BufferStorageBase(Iter first, Iter last) {
        auto end = construct_copy_range(begin(), first, last);
        size = end - begin();
    }

    constexpr BufferStorageBase(std::size_t n, T const &value) noexcept(std::is_nothrow_copy_constructible_v<T>)
        : size{n} {
        construct_fill(begin(), n, value);
    }

    constexpr BufferStorageBase(int, BufferStorageBase const &other) noexcept(
        std::is_nothrow_copy_constructible_v<stored_type>)
        : size{other.size} {
        construct_copy_range(begin(), other.cbegin(), other.cend());
    }

    constexpr BufferStorageBase(int,
                                BufferStorageBase &&other) noexcept(std::is_nothrow_move_constructible_v<stored_type>)
        : size{other.size} {
        construct_move_range(begin(), other.begin(), other.end());
    }

    template <typename Iter>
    constexpr void assign(Iter first, Iter last, std::input_iterator_tag) {
        auto it = begin();
        while (it != end() && first != last) {
            *it++ = *first++;
        }
        if (first == last) {
            destroy_range(it, end());
        } else {
            construct_at(it++, *first++);
            it = construct_copy_range(it, first, last);
        }
        size = it - begin();
    }

    template <typename Iter>
    constexpr void assign(Iter first, Iter last, std::forward_iterator_tag) {
        auto n = std::distance(first, last);
        if (n < size) {
            std::copy(first, last, begin());
            destroy_range(begin() + n, end());
        } else {
            std::copy_n(first, size, begin());
            construct_copy_range(end(), first + size, last);
        }
        size = std::distance(first, last);
    }

    constexpr void assign(std::size_t n, T const &value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_copy_assignable<T>>) {
        if (n < size) {
            std::fill_n(begin(), n, value);
            destroy_range(begin() + n, end());
        } else {
            std::fill(begin(), end(), value);
            construct_fill(end(), n - size, value);
        }
        size = n;
    }

    constexpr void assign(BufferStorageBase const &other) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<stored_type>,
                           std::is_nothrow_copy_assignable<stored_type>>) {
        assign(other.begin(), other.end());
    }

    constexpr void assign(BufferStorageBase &&other) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<stored_type>,
                           std::is_nothrow_move_assignable<stored_type>>) {
        assign(std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()));
    }

    constexpr void insert(iterator pos, T const &value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_move_constructible<T>,
                           std::is_nothrow_move_assignable<T>>) {
        construct_at(end(), std::move(*(end() - 1)));
        std::move_backward(pos, end(), end());
        pos->value = value;
        ++size;
    }

    constexpr void insert(iterator pos, T &&value) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        construct_at(end(), std::move(*(end() - 1)));
        std::move_backward(pos, end(), end());
        pos->value = std::move(value);
        ++size;
    }

    template <typename Iter, typename = std::enable_if_t<is_iter_type_v<Iter, std::forward_iterator_tag>>>
    constexpr void insert(iterator pos, Iter first, Iter last) noexcept(
        std::conjunction_v<std::is_nothrow_constructible<T, typename std::iterator_traits<Iter>::reference>,
                           std::is_nothrow_assignable<T, typename std::iterator_traits<Iter>::reference>,
                           std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        std::size_t const num_insert = std::distance(first, last);
        std::size_t const slots_after = end() - pos;
        if (slots_after > num_insert) {
            auto const split_pos = end() - num_insert;
            construct_move_range(end(), split_pos, end());
            std::move_backward(pos, split_pos, end());
            std::copy(first, last, pos);
        } else {
            auto const split_pos = first + slots_after;
            auto const it = construct_copy_range(end(), split_pos, last);
            construct_move_range(it, pos, end());
            std::copy(first, split_pos, pos);
        }
        size += num_insert;
    }

    constexpr void insert(iterator pos, std::size_t n, T const &value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_copy_assignable<T>,
                           std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        std::size_t const slots_after = end() - pos;
        if (slots_after > n) {
            auto const split_pos = end() - n;
            construct_move_range(end(), split_pos, end());
            std::move_backward(pos, split_pos, end());
            std::fill_n(pos, n, value);
        } else {
            auto const it = construct_fill(end(), n - slots_after, value);
            construct_move_range(it, pos, end());
            std::fill(pos, end(), value);
        }
        size += n;
    }

    template <typename... Args>
    constexpr void emplace(iterator pos, Args &&...args) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>,
                           std::is_nothrow_constructible<T, Args...>>) {
        insert(pos, stored_value(std::forward<Args>(args)...));
    }

    constexpr void emplace(iterator pos, T &&value) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>,
                           std::is_nothrow_move_constructible<T>>) {
        insert(pos, std::move(value));
    }

    constexpr void erase(iterator pos) noexcept(std::is_nothrow_move_assignable_v<T>) {
        if (pos + 1 != end()) {
            std::move(pos + 1, end(), pos);
        }
        --size;
        destroy_at(end());
    }

    constexpr void erase(iterator first, iterator last) noexcept(std::is_nothrow_move_assignable_v<T>) {
        if (first != last) {
            if (last != end()) {
                std::move(last, end(), first);
            }
            destroy_range(end() - (last - first), end());
            size -= last - first;
        }
    }

    constexpr iterator construct_fill(iterator pos, std::size_t n,
                                      T const &value) noexcept(std::is_nothrow_copy_constructible_v<stored_type>) {
        for (; n != 0; --n) {
            construct_at(pos++, value);
        }
        return pos;
    }

    template <typename Iter>
    constexpr iterator construct_copy_range(iterator pos, Iter first, Iter last) {
        for (; first != last; ++first) {
            construct_at(pos++, *first);
        }
        return pos;
    }

    template <typename Iter>
    constexpr iterator construct_move_range(iterator pos, Iter first, Iter last) {
        for (; first != last; ++first) {
            construct_at(pos++, std::move(*first));
        }
        return pos;
    }

    template <typename... Args>
    constexpr void construct_at(iterator pos,
                                Args &&...args) noexcept(std::is_nothrow_constructible_v<stored_type, Args...>) {
        if constexpr (std::conjunction_v<std::is_trivially_default_constructible<T>,
                                         std::is_trivially_move_assignable<T>, std::is_trivially_destructible<T>>) {
            *pos = stored_type(std::forward<Args>(args)...);
        } else {
            ::new (static_cast<void *>(std::addressof(*pos))) stored_type(std::forward<Args>(args)...);
        }
    }

    constexpr void destroy_range(iterator first, iterator last) noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            for (; first != last; ++first) {
                destroy_at(first);
            }
        }
    }

    constexpr void destroy_at(iterator pos) noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            pos->~stored_type();
        }
    }

    constexpr void clear() noexcept {
        std::for_each_n(std::begin(slots), size, [](auto &s) { s.value.~stored_type(); });
        for (auto it = begin(); it != end(); ++it) {
            destroy_at(it);
        }
    }

    constexpr T &get(std::size_t n) noexcept {
        return slots[n].value;
    }

    constexpr T const &get(std::size_t n) const noexcept {
        return slots[n].value;
    }

    constexpr iterator begin() noexcept {
        return iterator{std::begin(slots)};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{std::cbegin(slots)};
    }

    constexpr iterator end() noexcept {
        return iterator{std::begin(slots) + size};
    }

    constexpr const_iterator cend() const noexcept {
        return const_iterator{std::cbegin(slots) + size};
    }
};

template <typename T, std::size_t N, bool = std::is_trivially_destructible_v<T>,
          bool = std::conjunction_v<std::is_trivially_copy_assignable<T>, std::is_trivially_copy_constructible<T>>,
          bool = std::conjunction_v<std::is_trivially_move_assignable<T>, std::is_trivially_move_constructible<T>>>
struct BufferStorage : BufferStorageBase<T, N> {
    using BufferStorageBase<T, N>::BufferStorageBase;

   public:
    BufferStorage() = default;
};

template <typename T, std::size_t N>
struct BufferStorage<T, N, true, false, true> : BufferStorageBase<T, N> {
    using BufferStorageBase<T, N>::BufferStorageBase;

   public:
    BufferStorage() = default;
    ~BufferStorage() = default;
    BufferStorage(BufferStorage const &) = default;
    BufferStorage(BufferStorage &&) = default;

    constexpr BufferStorage &operator=(BufferStorage const &other) {
        this->assign(other);
        return *this;
    }

    constexpr BufferStorage &operator=(BufferStorage &&) = default;
};

template <typename T, std::size_t N>
struct BufferStorage<T, N, true, true, false> : BufferStorageBase<T, N> {
    using BufferStorageBase<T, N>::BufferStorageBase;

   public:
    BufferStorage() = default;
    ~BufferStorage() = default;
    BufferStorage(BufferStorage const &) = default;
    BufferStorage(BufferStorage &&) = default;
    constexpr BufferStorage &operator=(BufferStorage const &) = default;

    constexpr BufferStorage &operator=(BufferStorage &&other) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        this->assign(std::move(other));
        return *this;
    }
};

template <typename T, std::size_t N>
struct BufferStorage<T, N, true, false, false> : BufferStorageBase<T, N> {
    using BufferStorageBase<T, N>::BufferStorageBase;

   public:
    BufferStorage() = default;
    ~BufferStorage() = default;
    BufferStorage(BufferStorage const &) = default;
    BufferStorage(BufferStorage &&) = default;

    constexpr BufferStorage &operator=(BufferStorage const &other) {
        this->assign(other);
        return *this;
    }

    constexpr BufferStorage &operator=(BufferStorage &&other) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        this->assign(std::move(other));
        return *this;
    }
};

template <typename T, std::size_t N, bool Copy, bool Move>
struct BufferStorage<T, N, false, Copy, Move> : BufferStorage<T, N, true, false, false> {
    using BufferStorage<T, N, true, false, false>::BufferStorage;

   public:
    BufferStorage() = default;

    ~BufferStorage() {
        this->clear();
    }

    BufferStorage(BufferStorage const &) = default;
    BufferStorage(BufferStorage &&) = default;
    constexpr BufferStorage &operator=(BufferStorage const &other) = default;
    constexpr BufferStorage &operator=(BufferStorage &&) = default;
};

template <typename T, std::size_t N, bool = std::is_trivially_copy_constructible_v<T>,
          bool = std::is_trivially_move_constructible_v<T>>
struct BufferBase {
    BufferStorage<T, N> storage;

   public:
    constexpr BufferBase() = default;

    constexpr BufferBase(std::size_t n, T const &value) : storage(n, value) {
    }

    template <typename InputIt>
    constexpr BufferBase(InputIt first, InputIt last) : storage(first, last) {
    }

    constexpr BufferBase(BufferBase const &other) : storage(0, other.storage) {
    }

    constexpr BufferBase(BufferBase &&other) noexcept(std::is_nothrow_move_constructible_v<T>)
        : storage(0, std::move(other.storage)) {
    }

    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, false, true> {
    BufferStorage<T, N> storage;

   public:
    constexpr BufferBase() = default;

    constexpr BufferBase(std::size_t n, T const &value) : storage(n, value) {
    }

    template <typename InputIt>
    constexpr BufferBase(InputIt first, InputIt last) : storage(first, last) {
    }

    constexpr BufferBase(BufferBase const &other) : storage(0, other.storage) {
    }

    constexpr BufferBase(BufferBase &&other) = default;
    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, true, false> {
    BufferStorage<T, N> storage;

   public:
    constexpr BufferBase() = default;

    constexpr BufferBase(std::size_t n, T const &value) : storage(n, value) {
    }

    template <typename InputIt>
    constexpr BufferBase(InputIt first, InputIt last) : storage(first, last) {
    }

    constexpr BufferBase(BufferBase const &other) = default;

    constexpr BufferBase(BufferBase &&other) noexcept(std::is_nothrow_move_constructible_v<T>)
        : storage(0, std::move(other.storage)) {
    }

    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, true, true> {
    BufferStorage<T, N> storage;

   public:
    constexpr BufferBase() = default;

    constexpr BufferBase(std::size_t n, T const &value) : storage(n, value) {
    }

    template <typename InputIt>
    constexpr BufferBase(InputIt first, InputIt last) : storage(first, last) {
    }

    constexpr BufferBase(BufferBase const &other) = default;
    constexpr BufferBase(BufferBase &&other) = default;
    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct Buffer : private BufferBase<T, N> {
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type &;
    using const_reference = value_type const &;
    using pointer = value_type *;
    using const_pointer = value_type const *;
    using iterator = StorageIterator<T, N>;
    using const_iterator = StorageIterator<T const, N>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   public:
    constexpr Buffer() noexcept {
    }

    constexpr Buffer(size_type count, const_reference value) noexcept(std::is_nothrow_copy_constructible_v<T>)
        : BufferBase<T, N>(count, value) {
    }

    constexpr explicit Buffer(size_type count) noexcept(
        std::conjunction_v<std::is_nothrow_default_constructible<T>, std::is_nothrow_copy_constructible<T>>)
        : BufferBase<T, N>(count, value_type()) {
    }

    template <typename InputIt>
    constexpr Buffer(InputIt first, InputIt last) : BufferBase<T, N>(first, last) {
    }

    constexpr Buffer(std::initializer_list<value_type> init) : BufferBase<T, N>(init.begin(), init.end()) {
    }

    constexpr Buffer &operator=(std::initializer_list<value_type> ilist) {
        this->storage.assign(ilist.begin(), ilist.end());
        return *this;
    }

    constexpr void assign(size_type count, const_reference value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_copy_assignable<T>>) {
        this->storage.assign(count, value);
    }

    template <typename InputIt>
    constexpr void assign(InputIt first, InputIt last) {
        this->storage.assign(first, last);
    }

    constexpr void assign(std::initializer_list<value_type> ilist) {
        this->storage.assign(ilist.begin(), ilist.end());
    }

    constexpr reference operator[](std::size_t n) noexcept {
        return this->storage.get(n);
    }

    constexpr const_reference operator[](std::size_t n) const noexcept {
        return this->get(n);
    }

    constexpr reference front() noexcept {
        return this->storage.get(0);
    }

    constexpr const_reference front() const noexcept {
        return this->get(0);
    }

    constexpr reference back() {
        return this->storage.get(this->storage.size - 1);
    }

    constexpr const_reference back() const noexcept {
        return this->get(this->storage.size - 1);
    }

    constexpr bool empty() const noexcept {
        return this->storage.size == 0;
    }

    constexpr std::size_t size() const noexcept {
        return this->storage.size;
    }

    constexpr void clear() noexcept {
        this->storage.clear();
    }

    constexpr iterator insert(const_iterator pos, const_reference value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_move_constructible<T>,
                           std::is_nothrow_move_assignable<T>>) {
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            this->storage.construct_at(it, value);
            ++this->storage.size;
        } else {
            auto value_cpy = value;
            this->storage.insert(it, std::move(value_cpy));
        }
        return it;
    }

    constexpr iterator insert(const_iterator pos, value_type &&value) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_constructible<T>,
                           std::is_nothrow_move_assignable<T>>) {
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            this->storage.construct_at(it, std::move(value));
            ++this->storage.size;
        } else {
            auto value_cpy = std::move(value);
            this->storage.insert(it, std::move(value_cpy));
        }
        return it;
    }

    constexpr iterator insert(const_iterator pos, size_type count, const_reference value) noexcept(
        std::conjunction_v<std::is_nothrow_copy_constructible<T>, std::is_nothrow_move_constructible<T>,
                           std::is_nothrow_copy_assignable<T>>) {
        auto it = begin() + (pos - cbegin());
        if (count != 0) {
            auto value_cpy = value;
            this->storage.insert(it, count, value_cpy);
        }
        return it;
    }

    template <typename InputIt>
    constexpr iterator insert(const_iterator pos, InputIt first, InputIt last) {
        auto it = begin() + (pos - cbegin());
        if (first != last) {
            this->storage.insert(it, first, last);
        }
        return it;
    }

    constexpr iterator insert(const_iterator pos, std::initializer_list<value_type> ilist) {
        auto it = begin() + (pos - cbegin());
        if (ilist.size() != 0) {
            this->storage.insert(it, ilist.begin(), ilist.end());
        }
        return it;
    }

    template <typename... Args>
    constexpr iterator emplace(const_iterator pos, Args &&...args) noexcept(
        std::conjunction_v<std::is_nothrow_constructible<T, Args...>, std::is_nothrow_move_constructible<T>,
                           std::is_nothrow_copy_assignable<T>>) {
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            this->storage.construct_at(it, std::forward<Args>(args)...);
            ++this->storage.size;
        } else {
            this->storage.emplace(it, std::forward<Args>(args)...);
        }
        return it;
    }

    constexpr iterator erase(const_iterator pos) {
        auto it = begin() + (pos - cbegin());
        this->storage.erase(it);
        return it;
    }

    constexpr iterator erase(const_iterator first, const_iterator last) {
        auto const first_it = begin() + (last - cbegin());
        auto const last_it = begin() + (last - cbegin());
        this->storage.erase(first_it, last_it);
        return first_it;
    }

    constexpr void push_back(T const &value) {
        this->storage.construct_at(end(), value);
        ++this->storage.size;
    }

    constexpr void push_back(T &&value) {
        this->storage.construct_at(end(), std::move(value));
        ++this->storage.size;
    }

    template <typename... Args>
    constexpr void emplace_back(Args &&...args) {
        this->storage.construct_at(end(), std::forward<Args>(args)...);
        ++this->storage.size;
    }

    constexpr void pop_back() {
        --this->storage.size;
        this->storage.destroy_at(end());
    }
    constexpr iterator begin() noexcept {
        return this->storage.begin();
    }

    constexpr const_iterator cbegin() const noexcept {
        return this->storage.cbegin();
    }

    constexpr iterator end() noexcept {
        return this->storage.end();
    }

    constexpr const_iterator end() const noexcept {
        return this->storage.end();
    }
};

}  // namespace multiqueue

#endif  //! STATIC_BUFFER_HPP_INCLUDED
