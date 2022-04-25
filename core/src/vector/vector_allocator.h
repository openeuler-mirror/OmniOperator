/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VECTOR_ALLOCATOR_H__
#define __VECTOR_ALLOCATOR_H__

#include <string>
#include "vector_reference.h"
#include "type/data_type.h"
#include "tracer/vector_leak_detector.h"
#include "memory/base_allocator.h"

const static std::string GLOBAL_SCOPE_NAME = "___GLOBAL_SCOPE___";

namespace omniruntime {
using namespace omniruntime::mem;
namespace vec {
using DataTypeId = omniruntime::type::DataTypeId;
class Vector;
class VectorAllocator final : public BaseAllocator {
public:
    ~VectorAllocator();

    void NewVector(Vector *vector, int capacityInBytes, int size, DataTypeId dataTypeId);

    void SliceVector(Vector *vector, Vector *sliceVector);

    void DeleteVector(Vector *vector);

    void ResizeVectorData(Vector *vector, int32_t toCapacityInBytes);

    VectorLeakDetector &GetLeakDetector()
    {
        return leakDetector;
    }

    VectorAllocator *NewChildAllocator(const std::string &scope, int64_t limit = UNLIMIT,
        int64_t reservation = 0) override;

    static VectorAllocator *GetGlobalAllocator()
    {
        static auto *globalAllocator = new VectorAllocator(omniruntime::mem::GetProcessRootAllocator(),
            GLOBAL_SCOPE_NAME, UNLIMIT, DEFAULT_RESERVATION);
        return globalAllocator;
    }

protected:
    VectorAllocator(BaseAllocator *parent, const std::string &scope, int64_t limit = UNLIMIT, int64_t reservation = 0);

private:
    static constexpr int64_t DEFAULT_RESERVATION = 1 << 20;
    VectorLeakDetector leakDetector;
};

VectorAllocator *GetProcessGlobalVecAllocator();
}
}
#endif // __VECTOR_ALLOCATOR_H__
