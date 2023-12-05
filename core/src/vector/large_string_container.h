/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_LARGE_STRING_CONTAINER_H
#define OMNI_RUNTIME_LARGE_STRING_CONTAINER_H
#include "type_utils.h"
#include "string_utils.h"
#include "util/omni_exception.h"
#include "util/compiler_util.h"

namespace omniruntime::vec::unsafe {
class UnsafeStringContainer;
}

namespace omniruntime::vec {
template <typename RAW_DATA_TYPE> class LargeStringContainer {
public:
    explicit LargeStringContainer(int value_size, int capacityInBytes = INITIAL_STRING_SIZE) : size(value_size)
    {
        if (value_size < 0) {
            // throw size exceeded exception
            throw omniruntime::exception::OmniException("STRING_CONTAINER_ERROR", "string container size invalid");
        }
        bufferSupplier = std::make_unique<LargeStringBuffer>(capacityInBytes);
        offsets.resize(value_size + 1);
    }

    ~LargeStringContainer() = default;

    ALWAYS_INLINE std::string_view GetValue(int index)
    {
        char *valuePtr = bufferSupplier->Data() + offsets[index];
        size_t valueLen = offsets[index + 1] - offsets[index];
        return std::string_view(valuePtr, valueLen);
    }

    ALWAYS_INLINE void SetValue(int index, std::string_view &value)
    {
        FillSlots(index); // rescue offset for null values

        size_t valueSize = value.size();
        int32_t needCapacityInBytes = offsets[index] + valueSize; // start offset, and then value size
        char *charBuffer = GetBufferWithSpace(needCapacityInBytes);

        // src len can be 0, but dest len can not be 0 when empty strings, otherwise the memcpy_s return errno 34
        errno_t result = memcpy_s(charBuffer + offsets[index], valueSize + 1, value.data(), valueSize);
        if (UNLIKELY(result != EOK)) {
            std::string omniExceptionInfo = "In function SetValue, memcpy faild, ret is：" + std::to_string(result) +
                ", reason is: " + std::string(strerror(errno));
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", omniExceptionInfo);
        }

        offsets[index + 1] = needCapacityInBytes;
        lastOffsetPosition = index;
    }

    ALWAYS_INLINE void SetValues(int index, std::vector<std::string_view> &values, size_t length, int32_t valuesSize)
    {
        FillSlots(index + length - 1);
        int32_t needCapacityInBytes = offsets[index] + valuesSize; // start offset, and then value size
        char *charBuffer = GetBufferWithSpace(needCapacityInBytes);

        for (auto &value : values) {
            auto valueSize = value.size();
            // src len can be 0, but dest len can not be 0 when empty strings, otherwise the memcpy_s return errno 34
            errno_t result = memcpy_sp(charBuffer + offsets[index], valueSize + 1, value.data(), valueSize);
            if (UNLIKELY(result != EOK)) {
                std::string omniExceptionInfo = "In function SetValue, memcpy faild, ret is：" + std::to_string(result) +
                        ", reason is: " + std::string(strerror(errno));
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", omniExceptionInfo);
            }

            offsets[index + 1] = offsets[index] + valueSize;
            lastOffsetPosition = index;
            ++index;
        }
    }

    /* *
     * set the element at the index position to null
     * @param index
     *         */
    ALWAYS_INLINE void SetNull(int32_t index)
    {
        FillSlots(index); // rescue offset for null values
        offsets[index + 1] = offsets[index];
        lastOffsetPosition = index;
    }

    ALWAYS_INLINE void FillSlots(int index)
    {
        for (int i = lastOffsetPosition + 1; i < index; ++i) {
            offsets[i + 1] = offsets[i];
        }
        lastOffsetPosition = index - 1;
    }

    ALWAYS_INLINE size_t GetCapacityInBytes()
    {
        return bufferSupplier->Capacity();
    }

    ALWAYS_INLINE int64_t GetContainerCapacity()
    {
        int64_t containerCapacity = sizeof(LargeStringContainer) + (size + 1) * sizeof(int32_t);
        return containerCapacity;
    }

private:
    ALWAYS_INLINE char *GetBufferWithSpace(uint32_t needCapacityInBytes)
    {
        // Check if the last buffer has enough space.
        if (bufferSupplier->Capacity() > 0 && needCapacityInBytes <= bufferSupplier->Capacity()) {
            return bufferSupplier->Data();
        }

        uint64_t initCapacityInBytes = bufferSupplier->Capacity() ? bufferSupplier->Capacity() : INITIAL_STRING_SIZE;
        uint64_t toCapacityInBytes = initCapacityInBytes;
        while (toCapacityInBytes < needCapacityInBytes) {
            toCapacityInBytes = toCapacityInBytes * 2;
        }

        // expand buffer.
        auto newAddress = ExpandBufferToCapacity(toCapacityInBytes);
        return newAddress;
    };

    ALWAYS_INLINE char *ExpandBufferToCapacity(size_t toCapacityInBytes)
    {
        std::unique_ptr<LargeStringBuffer> oldBuffer = std::move(bufferSupplier);

        // Allocate a new buffer.
        std::unique_ptr<LargeStringBuffer> newBuffer = std::make_unique<LargeStringBuffer>(toCapacityInBytes);
        if (oldBuffer->Capacity() > 0) {
            errno_t result =
                memcpy_s(newBuffer->Data(), newBuffer->Capacity(), oldBuffer->Data(), oldBuffer->Capacity());
            if (UNLIKELY(result != EOK)) {
                std::string omniExceptionInfo = "In function ExpandBufferToCapacity, memcpy faild, ret is：" +
                    std::to_string(result) + ", reason is: " + std::string(strerror(errno));
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", omniExceptionInfo);
            }
        }
        bufferSupplier = std::move(newBuffer);
        return bufferSupplier->Data();
    }

    friend class unsafe::UnsafeStringContainer;

    /* * the size of the string rows */
    int size;

    /* * stored the string rows */
    std::vector<int32_t> offsets;
    uint32_t lastOffsetPosition = -1;
    std::unique_ptr<LargeStringBuffer> bufferSupplier;
};
}
#endif // OMNI_RUNTIME_LARGE_STRING_CONTAINER_H
