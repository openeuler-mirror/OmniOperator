/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_UNSAFE_STRING_CONTAINER_H
#define OMNI_RUNTIME_UNSAFE_STRING_CONTAINER_H

#include "dictionary_container.h"
#include "large_string_container.h"

namespace omniruntime::vec::unsafe {
/**
 * get raw pointer of values in StringContainer.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeStringContainer {
public:
    static ALWAYS_INLINE char *GetValues(LargeStringContainer<std::string_view> *container)
    {
        return container->bufferSupplier->Data();
    }

    static ALWAYS_INLINE int32_t *GetOffsets(LargeStringContainer<std::string_view> *container)
    {
        return container->offsets.data();
    }

    static ALWAYS_INLINE char *ExpandBufferToCapacity(LargeStringContainer<std::string_view> *container,
        size_t toCapacityInBytes)
    {
        return container->ExpandBufferToCapacity(toCapacityInBytes);
    }

    static ALWAYS_INLINE size_t GetCapacityInBytes(LargeStringContainer<std::string_view> *container)
    {
        return container->GetCapacityInBytes();
    }

    // for UT test
    static char *GetBufferWithSpace(LargeStringContainer<std::string_view> *container, size_t needCapacityInBytes)
    {
        return container->GetBufferWithSpace(needCapacityInBytes);
    }

    // for UT test
    static char *GetStringBufferAddr(LargeStringContainer<std::string_view> *container)
    {
        return container->bufferSupplier->Data();
    }
};
}

#endif // OMNI_RUNTIME_UNSAFE_STRING_CONTAINER_H
