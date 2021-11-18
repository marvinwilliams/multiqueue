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
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <type_traits>

namespace multiqueue {

namespace detail {

template <typename T, std::size_t N>
struct BufferStorageBase {
    using stored_type = std::remove_const_t<T>;

    struct Empty {};

    template <typename U, bool = std::is_trivially_destructible_v<U>>
    union StorageSlot {
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
    union StorageSlot<U, false> {
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

   public:
    StorageSlot<stored_type> slots[N];
    std::size_t size = 0;

   public:
    BufferStorageBase() = default;
    ~BufferStorageBase() = default;

    BufferStorageBase(BufferStorageBase const &) = default;
    BufferStorageBase(BufferStorageBase &&) = default;

    BufferStorageBase &operator=(BufferStorageBase const &) = default;
    BufferStorageBase &operator=(BufferStorageBase &&) = default;

    constexpr BufferStorageBase(bool, BufferStorageBase const &other) : size{other.size} {
        for (std::size_t i = 0; i < size; ++i) {
            ::new (static_cast<void *>(std::addressof(slots[i]))) stored_type(other.get(i));
        }
    }

    constexpr BufferStorageBase(bool, BufferStorageBase &&other) : size{other.size} {
        for (std::size_t i = 0; i < size; ++i) {
            ::new (static_cast<void *>(std::addressof(slots[i]))) stored_type(std::move(other.get(i)));
        }
    }

    constexpr BufferStorageBase(std::size_t n, T const &value) : size{n} {
        for (std::size_t i = 0; i < n; ++i) {
            ::new (static_cast<void *>(std::addressof(slots[i]))) stored_type(value);
        }
    }

    template <typename InputIt>
    constexpr BufferStorageBase(InputIt first, InputIt last) {
        for (; first != last; ++first) {
            ::new (static_cast<void *>(std::addressof(slots[size]))) stored_type(*first);
            ++size;
        }
    }

    constexpr void copy_assign(BufferStorageBase const &other) {
        std::size_t i = 0;
        if (size < other.size) {
            for (; i < size; ++i) {
                get(i) = other.get(i);
            }
            for (; i < other.size; ++i) {
                ::new (static_cast<void *>(std::addressof(slots[i]))) stored_type(other.get(i));
            }
        } else {
            for (; i < other.size; ++i) {
                get(i) = other.get(i);
            }
            for (; i < size; ++i) {
                get(i).~stored_type();
            }
        }
        size = other.size;
    }

    constexpr void move_assign(BufferStorageBase &&other) noexcept(
        std::conjunction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        std::size_t i = 0;
        if (size < other.size) {
            for (; i < size; ++i) {
                get(i) = std::move(other.get(i));
            }
            for (; i < other.size; ++i) {
                ::new (static_cast<void *>(std::addressof(slots[i]))) stored_type(std::move(other.get(i)));
            }
        } else {
            for (; i < other.size; ++i) {
                get(i) = other.get(i);
            }
            for (; i < size; ++i) {
                get(i).~stored_type();
            }
        }
        size = other.size;
    }

    template <typename... Args>
    void emplace_back(Args &&...args) noexcept(std::is_nothrow_constructible_v<stored_type, Args...>) {
        ::new (static_cast<void *>(std::addressof(slots[size]))) stored_type(std::forward<Args>(args)...);
        ++size;
    }

    void pop_back() noexcept {
        --size;
        get(size).~stored_type();
    }

    constexpr void clear() noexcept {
        std::for_each_n(std::begin(slots), size, [](auto &s) { s.value.~stored_type(); });
        size = 0;
    }

    constexpr T &get(std::size_t n) noexcept {
        return slots[n].value;
    }

    constexpr T const &get(std::size_t n) const noexcept {
        return slots[n].value;
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
        this->copy_assign(other);
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
        std::conjuction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        this->move_assign(std::move(other));
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
        this->copy_assign(other);
        return *this;
    }

    constexpr BufferStorage &operator=(BufferStorage &&other) noexcept(
        std::conjuction_v<std::is_nothrow_move_constructible<T>, std::is_nothrow_move_assignable<T>>) {
        this->move_assign(std::move(other));
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

    constexpr BufferBase(BufferBase const &other) : storage(true, other.storage) {
    }

    constexpr BufferBase(BufferBase &&other) noexcept(std::is_nothrow_move_constructible_v<T>)
        : storage(true, std::move(other.storage)) {
    }

    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, false, true> : BufferBaseImpl<T, N> {
    BufferStorage<T, N> storage;

   public:
    constexpr BufferBase() = default;

    constexpr BufferBase(std::size_t n, T const &value) : storage(n, value) {
    }

    template <typename InputIt>
    constexpr BufferBase(InputIt first, InputIt last) : storage(first, last) {
    }

    constexpr BufferBase(BufferBase const &other) : storage(true, other.storage) {
    }

    constexpr BufferBase(BufferBase &&other) = default;
    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, true, false> : BufferBaseImpl<T, N> {
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
        : storage(true, std::move(other.storage)) {
    }

    BufferBase &operator=(BufferBase const &) = default;
    BufferBase &operator=(BufferBase &&) = default;
};

template <typename T, std::size_t N>
struct BufferBase<T, N, true, true> : BufferBaseImpl<T, N> {
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

template <typename T, std::size_t N, bool IsConst>
class BufferIterator;

template <typename T, std::size_t N>
struct Buffer : private BufferBase<T, N> {
    using value_type = T;
    size_type = std::size_t;
    difference_type = std::ptrdiff_t;
    reference = value_type &;
    const_reference = value_type const &;
    pointer = value_type *;
    const_pointer = value_type const *;
    iterator = BufferIterator<T, N, false>;
    const_iterator = BufferIterator<T, N, true>;
    reverse_iterator = std::reverse_iterator<iterator>;
    const_reverse_iterator = std::reverse_iterator<const_iterator>;

   public:
    constexpr Buffer() noexcept {
    }

    constexpr Buffer(size_type count, const_reference value) : BufferBase(count, value) {
    }

    constexpr explicit Buffer(size_type count) : BufferBase(count, value_type()) {
    }

    template <typename InputIt>
    constexpr Buffer(InputIt first, InputIt last) : BufferBase(first, last) {
    }

    constexpr Buffer(std::initializer_list<value_type> init) : BufferBase(init.begin(), init.end()) {
    }

    constexpr Buffer &operator=(std::initializer_list<value_type> ilist) {
        this->storage.assign(ilist.begin(), ilist.end());
    }

    constexpr void assign(size_type count, const_reference value) {
        this->storage.assign(count, value);
    }

    template <typename InputIt>
    constexpr void assign(InputIt first, InputIt last) {
        this->storage.assign(first, last);
    }

    constexpr void assign(std::initializer_list<value_type> ilist) {
        this->storage.assign(ilist.begin(), ilist.end());
    }

    constexpr T &operator[](std::size_t n) {
        return this->get(n);
    }

    constexpr T const &operator[](std::size_t n) const {
        return this->get(n);
    }

    constexpr T &front() {
        return this->get(0);
    }

    constexpr T &back() {
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

    constexpr iterator insert(const_iterator pos, const_reference value) {
        // TODO
    }

    constexpr iterator insert(const_iterator pos, value_type &&value) {
        // TODO
    }

    constexpr iterator insert(const_iterator pos, size_type count, const_reference value) {
        // TODO
    }

    template <typename InputIter>
    constexpr iterator insert(const_iterator pos, InputIt first, InputIt last) {
        // TODO
    }

    constexpr iterator insert(const_iterator pos, std::initializer_list<value_type> ilist) {
        // TODO
    }

    template <typename... Args>
    constexpr iterator emplace(const_iterator pos, Args &&...args) {
        // TODO
    }

    constexpr iterator erase(const_iterator pos) {
        // TODO
    }

    constexpr iterator erase(const_iterator first, const_iterator last) {
        // TODO
    }

    constexpr void push_back(T const &value) {
        this->emplace_back(value);
    }

    constexpr void push_back(T &&value) {
        this->emplace_back(std::move(value));
    }

    template <typename... Args>
    constexpr void emplace_back(Args &&...args) {
        this->emplace_back(std::forward<Args>(args)...);
    }

    constexpr void pop_back() {
        this->pop_back();
    }

    void swap(Buffer &other) noexcept(std::conjunction_v < std::is_nothrow_move_constructible<value_type>,
                                      std::is_nothrow_swappable<value_type>) {
      using std::swap;
    // TODO
    }
};

}  // namespace detail

}  // namespace multiqueue

#endif  //! STATIC_BUFFER_HPP_INCLUDED
