/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_REFERENCE_H__
#define __VECTOR_REFERENCE_H__

#include <atomic>
#include "chunk.h"
#include "vector_type.h"

class VectorReference {
public:
    VectorReference(Chunk *values, Chunk *valueNulls, Chunk *valueOffsets, int capacityInBytes, VecType type);

    ~VectorReference();

    int64_t DecRef();

    void IncRef();

    int64_t GetRef();

    void *GetValuesAddress();

    void *GetValueNullsAddress();

    void *GetValueOffsetsAddress();

    VecType GetType();

    int GetCapacityInBytes();

    bool IsWritable();

    Chunk *GetValueChunk() const;

    Chunk *GetValueNullChunk() const;

    Chunk *GetValueOffsetChunk() const;

private:
    int capacityInBytes;
    VecType type;
    Chunk *values;
    Chunk *valueNulls;
    Chunk *valueOffsets;
    bool writable;
    std::atomic<int64_t> reference;
};


#endif // __VECTOR_REFERENCE_H__
