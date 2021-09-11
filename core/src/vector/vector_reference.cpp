/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_reference.h"

namespace omniruntime {
namespace vec {
VectorReference::VectorReference(Chunk *values, Chunk *valueNulls, Chunk *valueOffsets)
    : values(values), valueNulls(valueNulls), valueOffsets(valueOffsets), reference(1), writable(true)
{}

VectorReference::~VectorReference()
{
    if (values != nullptr) {
        delete values;
        values = nullptr;
    }
    if (valueNulls != nullptr) {
        delete valueNulls;
        valueNulls = nullptr;
    }
    if (valueOffsets != nullptr) {
        delete valueOffsets;
        valueOffsets = nullptr;
    }
}

void VectorReference::IncRef()
{
    reference++;
    writable = false;
}

int64_t VectorReference::DecRef()
{
    --reference;
    return reference;
}

int64_t VectorReference::GetRef()
{
    return reference;
}

void *VectorReference::GetValuesAddress()
{
    if (values != nullptr) {
        return values->GetAddress();
    }
    return nullptr;
}

void *VectorReference::GetValueNullsAddress()
{
    if (valueNulls != nullptr) {
        return valueNulls->GetAddress();
    }
    return nullptr;
}

void *VectorReference::GetValueOffsetsAddress()
{
    if (valueOffsets != nullptr) {
        return valueOffsets->GetAddress();
    }
    return nullptr;
}

bool VectorReference::IsWritable()
{
    return writable;
}

Chunk *VectorReference::GetValueChunk() const
{
    return values;
}

Chunk *VectorReference::GetValueNullChunk() const
{
    return valueNulls;
}

Chunk *VectorReference::GetValueOffsetChunk() const
{
    return valueOffsets;
}
} // namespace vec
} // namespace omniruntime