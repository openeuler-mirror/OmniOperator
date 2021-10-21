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
#define RECORD_VECTOR_STACK(vector, opType)          \
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
    VectorReference *reference = NewVectorReference(capacityInBytes, size, type);
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

VectorReference *VectorAllocator::NewVectorReference(int capacityInBytes, int size, VecType type)
{
    Chunk *values = new Chunk(capacityInBytes);
    Chunk *valueNulls = new Chunk(size);
    if (memset_s(valueNulls->GetAddress(), size, 0, size) != EOK) {
        std::cerr << "init value nulls failed." << std::endl;
        delete values;
        delete valueNulls;
        return nullptr;
    }
    Chunk *valueOffsets = nullptr;
    if (IsVariableWidthType(type.GetId())) {
        // 4-byte length storage variable length type offset
        int offsetSizeInBytes = (size + 1) * sizeof(int32_t);
        valueOffsets = new Chunk(offsetSizeInBytes);
        if (memset_s(valueOffsets->GetAddress(), offsetSizeInBytes, 0, offsetSizeInBytes) != EOK) {
            std::cerr << "init value offsets failed." << std::endl;
            delete values;
            delete valueNulls;
            delete valueOffsets;
            return nullptr;
        }
    }
    return new VectorReference(values, valueNulls, valueOffsets);
}

std::string VectorAllocator::GetScope() const
{
    return scope;
}

int64_t VectorAllocator::GetAllocatedBytes() const
{
    return allocatedBytes;
}

bool VectorAllocator::IsVariableWidthType(int type)
{
    switch (type) {
        case OMNI_VEC_TYPE_VARCHAR:
            return true;
        default:
            return false;
    }
}

void VectorAllocator::RecordVectorStack(const Vector *vector, std::string &stack, VecOpType opType)
{
    leakDetector.Record(vector, stack, opType);
}
}
}