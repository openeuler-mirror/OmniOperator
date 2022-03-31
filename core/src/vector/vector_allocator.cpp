/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_allocator.h"
#include <sstream>
#include "vector.h"

namespace omniruntime {
namespace vec {
using Chunk = omniruntime::mem::Chunk;

VectorAllocator::VectorAllocator(std::string scope) : scope(scope), leakDetector(scope) {}

VectorAllocator::~VectorAllocator() {}

void VectorAllocator::NewVector(Vector *vector, int capacityInBytes, int size, DataTypeId dataTypeId)
{
    VectorReference *reference = new VectorReference(capacityInBytes, size, dataTypeId);
    vector->SetVectorReference(reference);
#ifdef DEBUG_VECTOR
    VectorTracer *tracer = leakDetector.NewTracer(vector);
    std::string stack = TraceUtil::GetStack();
    tracer->Record(stack, NEW);
    vector->SetVectorTracer(tracer);
#endif
}

void VectorAllocator::SliceVector(Vector *vector, Vector *sliceVector)
{
    VectorReference *reference = vector->GetVectorReference();
    reference->IncRef();
    sliceVector->SetVectorReference(reference);
#ifdef DEBUG_VECTOR
    VectorTracer *tracer = leakDetector.NewTracer(sliceVector);
    std::string stack = TraceUtil::GetStack();
    tracer->Record(stack, SLICE);
    sliceVector->SetVectorTracer(tracer);
#endif
}

void VectorAllocator::DeleteVector(Vector *vector)
{
#ifdef DEBUG_VECTOR
    VectorTracer *tracer = vector->GetVectorTracer();
    std::string stack = TraceUtil::GetStack();
    tracer->Record(stack, FREE);
    leakDetector.CloseTracer(tracer);
#endif
    VectorReference *reference = vector->GetVectorReference();
    if (reference == nullptr) {
        return;
    }
    if (reference->DecRef() == 0) {
        delete reference;
        vector->SetVectorReference(nullptr);
    }
}

void VectorAllocator::ResizeVectorData(Vector *vector, int32_t toCapacityInBytes)
{
    VectorReference *reference = vector->GetVectorReference();
    reference->ResizeValueChunk(vector->GetCapacityInBytes(), toCapacityInBytes);
}

std::string VectorAllocator::GetScope() const
{
    return scope;
}

int64_t VectorAllocator::GetAllocatedBytes() const
{
    return allocatedBytes;
}
}
}