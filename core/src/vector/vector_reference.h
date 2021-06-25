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

    int64_t decRef();

    void incRef();

    int64_t getRef();

    void *getValuesAddress();

    void *getValueNullsAddress();

    void *getValueOffsetsAddress();

    VecType getType();

    int getCapacityInBytes();

    bool isWritable();

    Chunk* getValueChunk();

    Chunk* getValueNullChunk();

    Chunk* getValueOffsetChunk();

private:
    int capacityInBytes;
    VecType type;
    Chunk *values;
    Chunk *valueNulls;
    Chunk *valueOffsets;
    bool writable;
    std::atomic<int64_t> reference;
};


#endif //__VECTOR_REFERENCE_H__
