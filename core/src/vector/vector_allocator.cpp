/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <sstream>
#include "vector.h"
#include "vector_allocator.h"
#include "../memory/chunk.h"
#include "../util/trace_util.h"

namespace omniruntime {
namespace vec {
using Chunk = omniruntime::mem::Chunk;
#ifdef DEBUG_VECTOR
#define RECORD_VECTOR_STACK(vector, opType)        \
    do {                                           \
        std::string stack = TraceUtil::GetStack(); \
        RecordVectorStack(vector, stack, opType);  \
    } while (0)
#else
#define RECORD_VECTOR_STACK(vector, opType)
#endif
VectorAllocator::VectorAllocator(std::string scope) : scope(scope), leakDetector(scope) {}

VectorAllocator::~VectorAllocator() {}

void VectorAllocator::NewVector(Vector *vector, int capacityInBytes, int size, VecType type)
{
    VectorReference *reference = new VectorReference(capacityInBytes, size, type);
    vector->SetVectorReference(reference);
    RECORD_VECTOR_STACK(vector, NEW);
}

void VectorAllocator::SliceVector(Vector *vector, Vector *sliceVector)
{
    VectorReference *reference = vector->GetVectorReference();
    reference->IncRef();
    sliceVector->SetVectorReference(reference);
    RECORD_VECTOR_STACK(sliceVector, SLICE);
}

void VectorAllocator::DeleteVector(Vector *vector)
{
    RECORD_VECTOR_STACK(vector, FREE);
    VectorReference *reference = vector->GetVectorReference();
    if (reference == nullptr) {
        return;
    }
    if (0 == reference->DecRef()) {
        delete reference;
        vector->SetVectorReference(nullptr);
    }
}

std::string VectorAllocator::GetScope() const
{
    return scope;
}

int64_t VectorAllocator::GetAllocatedBytes() const
{
    return allocatedBytes;
}

void VectorAllocator::RecordVectorStack(const Vector *vector, std::string &stack, VecOpType opType)
{
    leakDetector.Record(vector, stack, opType);
}
}
}