/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_MEMORY_MANAGER_ALLOCATOR_H
#define OMNI_RUNTIME_MEMORY_MANAGER_ALLOCATOR_H

#include <cstddef>
#include <cstdlib>
#include <climits>

namespace omniruntime::mem {
/**
 * Our memory manager manages the heap memory, such as allocation and statistics. This has a problem, that is,
 * loop calls may be occur when the internal data structure of the memory manager needs to apply for heap memory.
 * To solve the problem, the class MemoryManagerAllocator is introduced to replace stl::allocator and
 * implement memory management of the internal data structure of our memory manager.
 *  */
template <class T> class MemoryManagerAllocator {
public:
    typedef T value_type;
    typedef T *pointer;
    typedef const T *const_pointer;
    typedef const T &const_reference;
    typedef T &reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    template <class U> struct rebind {
        typedef MemoryManagerAllocator<U> other;
    };

    MemoryManagerAllocator() noexcept {}

    MemoryManagerAllocator(const MemoryManagerAllocator &) noexcept {}

    MemoryManagerAllocator& operator=(const MemoryManagerAllocator& allocator) noexcept {}

    template <class U> explicit MemoryManagerAllocator(const MemoryManagerAllocator<U> &) noexcept {}

    ~MemoryManagerAllocator() noexcept {}

    pointer address(reference x) noexcept
    {
        return static_cast<pointer>(&x);
    }

    const_pointer address(const_reference x) noexcept
    {
        return static_cast<const_pointer>(&x);
    }

    // modified allocation method from "::operator new(n * sizeof(T))" in stl::allocator to "malloc(n * sizeof(T)))"
    pointer allocate(size_type n)
    {
        void *pMem = nullptr;
        if (n > this->max_size() || (pMem = malloc(n * sizeof(T))) == nullptr) {
            throw std::bad_alloc();
        }
        return static_cast<T *>(pMem);
    }

    // modified allocation method from "::operator delete(p)" in stl::allocator to "free(p)"
    void deallocate(pointer p, size_type)
    {
        free(p);
    }

    size_type max_size() const noexcept
    {
        return size_t(UINT_MAX / sizeof(T));
    }

    void construct(pointer p, const_reference value)
    {
        new (p)T(value);
    }
};
}

#endif // OMNI_RUNTIME_MEMORY_MANAGER_ALLOCATOR_H
