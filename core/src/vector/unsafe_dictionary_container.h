/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_UNSAFE_DICTIONARY_CONTAINER_H
#define OMNI_RUNTIME_UNSAFE_DICTIONARY_CONTAINER_H

#include "dictionary_container.h"

namespace omniruntime::vec::unsafe {
/**
 * get raw pointer of member in DictionaryContainer.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeDictionaryContainer {
public:
    template <typename DATA_TYPE>
    static ALWAYS_INLINE DATA_TYPE *GetDictionary(DictionaryContainer<DATA_TYPE> *container)
    {
        return container->dictionary.get() + container->dictOffset;
    }

    static ALWAYS_INLINE char *GetVarCharDictionary(
        DictionaryContainer<std::string_view, LargeStringContainer> *container)
    {
        return unsafe::UnsafeStringContainer::GetValues(container->dictionary.get());
    }

    static ALWAYS_INLINE int32_t *GetDictionaryOffsets(
        DictionaryContainer<std::string_view, LargeStringContainer> *container)
    {
        return unsafe::UnsafeStringContainer::GetOffsets(container->dictionary.get()) + container->dictOffset;
    }

    template <typename DATA_TYPE> static ALWAYS_INLINE int *GetIds(DictionaryContainer<DATA_TYPE> *container)
    {
        return container->values.data();
    }
};
}

#endif // OMNI_RUNTIME_UNSAFE_DICTIONARY_CONTAINER_H
