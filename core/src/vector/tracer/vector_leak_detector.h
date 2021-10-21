/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_LEAK_DETECTOR_H
#define OMNI_RUNTIME_VECTOR_LEAK_DETECTOR_H

#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include "vector_tracer.h"

namespace omniruntime {
namespace vec {
class Vector;
class VectorLeakDetector {
public:
    explicit VectorLeakDetector(const std::string scope);

    ~VectorLeakDetector();

    void Record(const Vector *vec, std::string &stack, VecOpType vecOpType);

    VectorTracer *FindTracer(const Vector *vec);

    VectorTracer *RemoveTracer(std::atomic<VectorTracer *> &head);

private:
    const std::string scope;
    const static int32_t BUCKET_NUM = 1024;
    const static int32_t RECYCLE_THRESHOLD = 1024;
    std::unique_ptr<std::atomic<VectorTracer *>[]> buckets;
    std::atomic<int64_t> deletedCount;
    std::atomic<bool> recycling;
    mutable std::shared_timed_mutex mutex;

    int32_t HashBucket(const Vector *vec);

    VectorTracer *InsertTracer(const Vector *vec);

    void RecycleDeletedTracer();
};
}
}
#endif // OMNI_RUNTIME_VECTOR_LEAK_DETECTOR_H
