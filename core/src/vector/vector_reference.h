/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VECTOR_REFERENCE_H__
#define __VECTOR_REFERENCE_H__

#include <atomic>
#include "../memory/chunk.h"
#include "../type/data_type.h"

namespace omniruntime {
namespace vec {
using namespace type;
using Chunk = mem::Chunk;

class VectorReference {
public:
    VectorReference(int capacityInBytes, int size, DataTypeId dataTypeId);

    ~VectorReference();

    int64_t DecRef();

    void IncRef();

    int64_t GetRef();

    void *GetValuesAddress();

    void *GetValueNullsAddress();

    void *GetValueOffsetsAddress();

    bool IsWritable();

    void ResizeValueChunk(int32_t currentCapacityInBytes, int32_t toCapacityInBytes);

private:
    bool IsVariableWidthType(int type);
    Chunk *valueChunk;
    Chunk *nullAndOffsetChunk;
    void *values;
    void *nulls;
    void *offsets;
    bool writable;
    std::atomic<int64_t> reference;
    static Chunk *zeroChunk;
};
} // namespace vec
} // namespace omniruntime
#endif // __VECTOR_REFERENCE_H__
