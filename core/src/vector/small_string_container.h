/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_SMALL_STRING_CONTAINER_H
#define OMNI_RUNTIME_SMALL_STRING_CONTAINER_H
#include "type_utils.h"
#include "string_utils.h"
#include "util/omni_exception.h"
#include "util/compiler_util.h"

namespace omniruntime::vec::unsafe {
class UnsafeStringContainer;
}

namespace omniruntime::vec {
template <typename RAW_DATA_TYPE> class SmallStringContainer {
public:
    SmallStringContainer(int value_size) : valueSize(value_size)
    {
        if (value_size < 0) {
            // throw size exceeded exception
            throw omniruntime::exception::OmniException("STRING_CONTAINER_ERROR", "string container size invalid");
        }

        values.reserve(value_size);
        bufferSupplier.reserve(value_size);
    }

    ALWAYS_INLINE std::string_view &GetValue(int index)
    {
        return values[index];
    }

    ALWAYS_INLINE void SetValue(int index, std::string_view &value)
    {
        char *valuePtr = bufferSupplier[index].buffer;
        errno_t result = memcpy_s(valuePtr, STRING_SIZE_THRESHOLD, value.data(), value.size());
        ASSERT(result == EOK);
        values[index] = std::string_view(valuePtr, value.size());
    }

private:
    friend class unsafe::UnsafeStringContainer;
    /* * the size of the string rows */
    int valueSize;

    /* * stored the string rows */
    std::vector<std::string_view> values;
    std::vector<SmallStringBuffer> bufferSupplier;
};
}
#endif // OMNI_RUNTIME_SMALL_STRING_CONTAINER_H
