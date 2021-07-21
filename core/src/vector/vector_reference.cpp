/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include <stddef.h>
#include "vector_reference.h"

VectorReference::VectorReference(Chunk *values, Chunk *valueNulls, Chunk *valueOffsets, int capacityInBytes,
    VecType type)
    : values(values), valueNulls(valueNulls), valueOffsets(valueOffsets), reference(1), capacityInBytes(capacityInBytes), type(type), writable(true)
{}

VectorReference::~VectorReference() {}

void VectorReference::IncRef()
{
    reference++;
    writable = false;
}

int64_t VectorReference::DecRef()
{
    if (--reference == 0) {
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

VecType VectorReference::GetType()
{
    return type;
}

int VectorReference::GetCapacityInBytes()
{
    return capacityInBytes;
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