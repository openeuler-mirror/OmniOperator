/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_reference.h"

#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
Chunk *VectorReference::zeroChunk = new Chunk(0);
/*
 * Encoding : values | nulls | offsets(option)
 */
VectorReference::VectorReference(int capacityInBytes, int size, VecType type)
    : reference(1), writable(true)
{
    // for empty vector, like lazy vector.
    if (capacityInBytes == -1) {
        chunk = zeroChunk;
        values = chunk->GetAddress();
        nulls = chunk->GetAddress();
        offsets = nullptr;
        return;
    }

    int valuesCapacityInBytes = capacityInBytes;
    int nullsCapacityInBytes = size;
    int offsetsCapacityInBytes = 0;
    bool isVariableType = IsVariableWidthType(type.GetId());
    if (isVariableType) {
        offsetsCapacityInBytes += (size + 1) * sizeof(int32_t);
    }

    int allocateSize = valuesCapacityInBytes + nullsCapacityInBytes + offsetsCapacityInBytes;

    chunk = new Chunk(allocateSize);
    char *baseAddress = (char *)chunk->GetAddress() + valuesCapacityInBytes;
    int nullsAndOffsetsCapacityInBytes = nullsCapacityInBytes + offsetsCapacityInBytes;
    if (memset_s(baseAddress, nullsAndOffsetsCapacityInBytes, 0, nullsAndOffsetsCapacityInBytes) != EOK) {
        std::cerr << "init nulls and offsets failed." << std::endl;
        delete chunk;
        return;
    }

    values = chunk->GetAddress();
    nulls = (char *)chunk->GetAddress() + valuesCapacityInBytes;
    offsets = isVariableType ? (char *)nulls + nullsCapacityInBytes : nullptr;
}

VectorReference::~VectorReference()
{
    if (chunk != nullptr && chunk != zeroChunk) {
        delete chunk;
        chunk = nullptr;
        values = nullptr;
        nulls = nullptr;
        offsets = nullptr;
    }
}

bool VectorReference::IsVariableWidthType(int type)
{
    switch (type) {
        case OMNI_VEC_TYPE_VARCHAR:
        case OMNI_VEC_TYPE_CHAR:
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

bool VectorReference::IsWritable()
{
    return writable;
}
} // namespace vec
} // namespace omniruntime