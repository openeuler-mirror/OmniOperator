/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_leak_detector.h"

#include <map>
#include "../vector.h"

namespace omniruntime {
namespace vec {
VectorLeakDetector::VectorLeakDetector(const std::string scope) : scope(scope), deletedCount(0), recycling(false)
{
    buckets = std::make_unique<std::atomic<VectorTracer *>[]>(BUCKET_NUM);
}

VectorLeakDetector::~VectorLeakDetector()
{
#ifdef DEBUG
    using VectorTracerMap = std::map<std::vector<std::string>, VectorTracer *>;
    using VectorTracerMapIt = VectorTracerMap::iterator;
    VectorTracerMap compact;
    for (int i = 0; i < BUCKET_NUM; ++i) {
        auto &head = buckets[i];
        VectorTracer *tracer = RemoveTracer(head);
        while (tracer != nullptr) {
            if (!tracer->Closed() && compact.end() == compact.find(tracer->GetPath())) {
                compact.insert(VectorTracerMap::value_type(tracer->GetPath(), tracer));
            } else {
                delete tracer;
            }
            tracer = RemoveTracer(head);
        }
    }

    for (VectorTracerMapIt it = compact.begin(); it != compact.end(); it++) {
        VectorTracer *tracer = it->second;
        tracer->Print("Memory Leak");
        delete tracer;
    }
    compact.clear();
#else
    for (int i = 0; i < BUCKET_NUM; ++i) {
        auto &head = buckets[i];
        VectorTracer *tracer = RemoveTracer(head);
        while (tracer != nullptr) {
            if (!tracer->IsClosed()) {
                tracer->Print("Memory Leak");
            }
            delete tracer;
            tracer = RemoveTracer(head);
        }
    }
#endif
}

int32_t VectorLeakDetector::HashBucket(const Vector *vec)
{
    std::hash<int64_t> hashFunc;
    uint64_t hash = hashFunc(reinterpret_cast<int64_t>(vec));
    return hash % BUCKET_NUM;
}

VectorTracer *VectorLeakDetector::NewTracer(const Vector *vec) {
    std::shared_lock<std::shared_timed_mutex> lock(mutex);
    auto &head = buckets[HashBucket(vec)];
    VectorTracer *tracer = new VectorTracer(scope, vec);
    VectorTracer *oldHead = head.load();
    tracer->next = oldHead;
    while (!head.compare_exchange_weak(oldHead, tracer)) {
        tracer->next = oldHead;
    }
    return tracer;
}

void VectorLeakDetector::CloseTracer(VectorTracer *tracer) {
    tracer->Close();
    if (deletedCount.fetch_add(1) >= RECYCLE_THRESHOLD) {
        RecycleDeletedTracer();
    }
}

VectorTracer *VectorLeakDetector::RemoveTracer(std::atomic<VectorTracer *> &head)
{
    VectorTracer *oldHead = head.load();
    if (oldHead == nullptr) {
        return nullptr;
    }
    while (!head.compare_exchange_weak(oldHead, oldHead->next)) {
    }
    return oldHead;
}

VectorTracer *VectorLeakDetector::FindTracer(const Vector *vec)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex);
    auto &head = buckets[HashBucket(vec)];
    VectorTracer *tracer = head.load();
    while (tracer != nullptr) {
        if (vec == tracer->GetVec()) {
            return tracer;
        }
        tracer = tracer->next;
    }
    return nullptr;
}

void VectorLeakDetector::RecycleDeletedTracer()
{
    // recycle start, set flag true
    if (recycling.exchange(true)) {
        return;
    }
    // lock the buckets and remove all closed tracer.
    std::unique_lock<std::shared_timed_mutex> lock(mutex);
    for (int i = 0; i < BUCKET_NUM; ++i) {
        auto &head = buckets[i];
        VectorTracer *tracer = head.load();
        // if head is closed, then head move to next tracer.
        while (tracer != nullptr && tracer->IsClosed()) {
            head = tracer->next;
            delete tracer;
            deletedCount.fetch_sub(1);
            tracer = head.load();
        }
        // then head is not closed.
        while (tracer != nullptr) {
            if (tracer->next != nullptr && tracer->next->IsClosed()) {
                VectorTracer *swapTracer = tracer->next->next;
                delete tracer->next;
                tracer->next = swapTracer;
                deletedCount.fetch_sub(1);
            } else {
                tracer = tracer->next;
            }
        }
    }
    // recycle end, set flag false
    recycling.store(false);
}
}
}