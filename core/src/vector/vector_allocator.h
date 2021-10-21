/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VECTOR_ALLOCATOR_H__
#define __VECTOR_ALLOCATOR_H__

#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include "vector_reference.h"
#include "vector_type.h"
#include "../util/compiler_util.h"

namespace omniruntime {
namespace vec {
class Vector;
enum VecOpType {
    NEW = 0,
    FREE,
    ADD_INPUT,
    GET_OUTPUT,
    MAX_OP_TYPE
};

class VecOpTypeName {
public:
    static std::string GetName(int vecOpType)
    {
        static std::string nameMap[MAX_OP_TYPE + 1] =
        {
            "NEW",
            "FREE",
            "ADD_INPUT",
            "GET_OUTPUT",
            "INVALID_TYPE"
        };
        return nameMap[vecOpType];
    }
};

class VectorTracer {
public:
    explicit VectorTracer(const std::string &scope, const Vector *vec);
    ~VectorTracer();
    void Record(std::string opName, VecOpType vecOpType);
    void Print(const char *message, bool err);
    ALWAYS_INLINE const Vector *GetVec()
    {
        return vec;
    };
    ALWAYS_INLINE bool Closed()
    {
        return closed;
    };
    ALWAYS_INLINE void reset()
    {
        closed = false;
        path.clear();
    };
    VectorTracer *next;

private:
    const std::string &scope;
    const Vector *vec;
    std::vector<std::string> path;
    bool closed;
};

class VectorLeakDetector {
public:
    explicit VectorLeakDetector(const std::string scope);
    ~VectorLeakDetector();
    void Record(const Vector *vec, std::string opName, VecOpType vecOpType);
    VectorTracer *FindTracer(const Vector *vec);

private:
    const std::string scope;
    const static int32_t BUCKET_NUM = 1024;
    const static int32_t RECYCLE_THRESHOLD = 1024;
    std::unique_ptr<std::atomic<VectorTracer *>[]> buckets;

    std::atomic<int64_t> deletedCount;
    std::atomic<bool> recycling;
    mutable std::shared_timed_mutex mutex;

    int32_t HashBucket(const Vector *vec);
    VectorTracer *PushTracer(const Vector *vec);
    VectorTracer *PopTracer(std::atomic<VectorTracer *> &head);

    void RecycleDeletedTracer();
};

class VectorAllocator {
public:
    explicit VectorAllocator(const std::string scope);

    ~VectorAllocator();

    VectorReference *NewVector(int capacityInBytes, int size, VecType type);

    std::string GetScope() const;

    int64_t GetAllocatedBytes();

    VectorLeakDetector &GetLeakDetector()
    {
        return leakDetector;
    }

private:
    bool IsVariableWidthType(int type);

private:
    const std::string scope;
    VectorLeakDetector leakDetector;
    std::atomic<int64_t> allocatedBytes;
};
}
}
#endif // __VECTOR_ALLOCATOR_H__
