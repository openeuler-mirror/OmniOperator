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
    explicit VectorLeakDetector(const std::string scope = "");

    ~VectorLeakDetector();

    VectorTracer *NewTracer(const Vector *vec);

    void CloseTracer(VectorTracer *tracer);

    VectorTracer *FindTracer(const Vector *vec);

    VectorTracer *RemoveTracer(std::atomic<VectorTracer *> &head);

    void SetScope(const std::string &newScope);

private:

    std::string scope;
    const static int32_t bucketNum = 1024;
    const static int32_t recycleThreshold = 1024;
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
