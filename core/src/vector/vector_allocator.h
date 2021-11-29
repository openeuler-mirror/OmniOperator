/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VECTOR_ALLOCATOR_H__
#define __VECTOR_ALLOCATOR_H__

#include <string>
#include "vector_reference.h"
#include "vector_type.h"
#include "tracer/vector_leak_detector.h"

namespace omniruntime {
namespace vec {
class Vector;
class VectorAllocator {
public:
    explicit VectorAllocator(const std::string scope);

    ~VectorAllocator();

    void NewVector(Vector *vector, int capacityInBytes, int size, VecType type);

    void SliceVector(Vector *vector, Vector *sliceVector);

    void DeleteVector(Vector *vector);

    std::string GetScope() const;

    int64_t GetAllocatedBytes() const;

    VectorLeakDetector &GetLeakDetector()
    {
        return leakDetector;
    }

private:
    const std::string scope;
    VectorLeakDetector leakDetector;
    std::atomic<int64_t> allocatedBytes;
};
}
}
#endif // __VECTOR_ALLOCATOR_H__
