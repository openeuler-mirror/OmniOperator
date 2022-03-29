/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_reference.h"
#include <huawei_secure_c/include/securec.h>

namespace omniruntime {
namespace vec {
Chunk *VectorReference::zeroChunk = Chunk::NewChunk(mem::BaseAllocator::GetRootAllocator(), 0);
/*
 * Encoding : values | nulls | offsets(option)
 */
VectorReference::VectorReference(VectorAllocator *allocator, int capacityInBytes, int size, DataTypeId dataTypeId) : writable(true), reference(1)
{
    // for empty vector, like lazy vector.
    if (capacityInBytes == -1) {
        valueChunk = zeroChunk;
        values = valueChunk->GetAddress();
        nullAndOffsetChunk = zeroChunk;
        nulls = nullAndOffsetChunk->GetAddress();
        offsets = nullptr;
        return;
    }

    valueChunk = Chunk::NewChunk(reinterpret_cast<mem::BaseAllocator *>(allocator), capacityInBytes);
    int nullsCapacityInBytes = size;
    int offsetsCapacityInBytes = 0;
    bool isVariableType = IsVariableWidthType(dataTypeId);
    if (isVariableType) {
        offsetsCapacityInBytes += (size + 1) * sizeof(int32_t);
    }

    int32_t nullsAndOffsetsCapacityInBytes = nullsCapacityInBytes + offsetsCapacityInBytes;

    nullAndOffsetChunk = Chunk::NewChunk(reinterpret_cast<mem::BaseAllocator *>(allocator), nullsAndOffsetsCapacityInBytes);
    char *baseAddress = static_cast<char *>(nullAndOffsetChunk->GetAddress());
    if (memset_s(baseAddress, nullsAndOffsetsCapacityInBytes, 0, nullsAndOffsetsCapacityInBytes) != EOK) {
        std::cerr << "init nulls and offsets failed." << std::endl;
        delete nullAndOffsetChunk;
        return;
    }

    values = valueChunk->GetAddress();
    nulls = static_cast<char *>(nullAndOffsetChunk->GetAddress());
    offsets = isVariableType ? (static_cast<char *>(nulls) + nullsCapacityInBytes) : nullptr;
}

VectorReference::~VectorReference()
{
    if (valueChunk != nullptr && valueChunk != zeroChunk) {
        delete valueChunk;
        valueChunk = nullptr;
        values = nullptr;
    }
    if (nullAndOffsetChunk != nullptr && nullAndOffsetChunk != zeroChunk) {
        delete nullAndOffsetChunk;
        nullAndOffsetChunk = nullptr;
        nulls = nullptr;
        offsets = nullptr;
    }
}

bool VectorReference::IsVariableWidthType(int type)
{
    switch (type) {
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return true;
        default:
            return false;
    }
}

void VectorReference::IncRef()
{
    reference++;
    writable = false;
}

int64_t VectorReference::DecRef()
{
    return --reference;
}

int64_t VectorReference::GetRef()
{
    return reference;
}

void *VectorReference::GetValuesAddress()
{
    return values;
}

void *VectorReference::GetValueNullsAddress()
{
    return nulls;
}

void *VectorReference::GetValueOffsetsAddress()
{
    return offsets;
}

void VectorReference::ResizeValueChunk(int32_t currentCapacityInBytes, int32_t toCapacityInBytes)
{
    Chunk *oldChunk = valueChunk;
    valueChunk = Chunk::NewChunk(oldChunk->GetAllocator(),toCapacityInBytes);
    // copy data
    char *newAddr = static_cast<char *>(valueChunk->GetAddress());
    char *oldAddr = static_cast<char *>(oldChunk->GetAddress());
    errno_t ret = memcpy_s(newAddr, toCapacityInBytes, oldAddr, currentCapacityInBytes);
    if (ret != EOK) {
        LogError("Resize Value chunk failed. error code is %d", ret);
        delete valueChunk;
        valueChunk = oldChunk;
        return;
    }
    delete oldChunk;
    values = valueChunk->GetAddress();
}

bool VectorReference::IsWritable()
{
    return writable;
}
} // namespace vec
} // namespace omniruntime