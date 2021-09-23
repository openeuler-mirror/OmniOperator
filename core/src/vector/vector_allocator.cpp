/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <memory>
#include <sstream>
#include "vector.h"
#include "vector_allocator.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
VectorTracer::VectorTracer(const std::string &scope, const Vector *vec) : scope(scope), vec(vec), closed(false) {}

VectorTracer::~VectorTracer() {}

void VectorTracer::Record(std::string opName, VecOpType vecOpType)
{
    std::string op = opName + "(" + VecOpTypeName::GetName(vecOpType) + ")";
    path.push_back(op);
    switch (vecOpType) {
        case NEW:
            break;
        case ADD_INPUT:
        case GET_OUTPUT:
            if (closed) {
                Print("Vector be used after free", true);
            }
            break;
        case FREE:
            if (closed) {
                Print("Double free", true);
            }
            closed = true;
            break;
        default:
            std::cerr << "error operator type: " << vecOpType << std::endl;
            break;
    }
}

void VectorTracer::Print(const char *message, bool err)
{
    std::stringstream ss;
    ss << "[" << message << "][" << scope << "][" << vec->GetAllocator() << "][" << vec << "]";
    int pathSize = path.size();
    for (int i = 0; i < pathSize; ++i) {
        ss << path[i];
        if (i != pathSize - 1) {
            ss << "->";
        }
    }
    if (err) {
        LogError("%s", ss.str().c_str());
    } else {
        LogInfo("%s", ss.str().c_str());
    }
}

VectorLeakDetector::VectorLeakDetector(const std::string scope) : scope(scope), deletedCount(0), recycling(false)
{
    buckets = std::make_unique<std::atomic<VectorTracer *>[]>(BUCKET_NUM);
}

VectorLeakDetector::~VectorLeakDetector()
{
    for (int i = 0; i < BUCKET_NUM; ++i) {
        auto &head = buckets[i];
        VectorTracer *tracer = PopTracer(head);
        while (tracer != nullptr) {
            if (!tracer->Closed()) {
                tracer->Print("Memory leak", true);
            }
            delete tracer;
            tracer = PopTracer(head);
        }
    }
}

void VectorLeakDetector::Record(const Vector *vec, std::string opName, VecOpType vecOpType)
{
    VectorTracer *tracer = nullptr;
    switch (vecOpType) {
        case NEW:
            // check whether the vector address is reused
            tracer = FindTracer(vec);
            if (tracer != nullptr && tracer->Closed()) {
                tracer->reset();
                deletedCount.fetch_sub(1);
            } else if (tracer == nullptr) {
                tracer = PushTracer(vec);
            } else {
                tracer->Print("Memory leak", true);
            }
            break;
        case FREE:
            if (deletedCount.fetch_add(1) >= RECYCLE_THRESHOLD) {
                RecycleDeletedTracer();
            }
        case ADD_INPUT:
        case GET_OUTPUT:
            tracer = FindTracer(vec);
            break;
        default:
            std::cerr << "error operator type: " << vecOpType << std::endl;
            break;
    }
    if (tracer == nullptr) {
        LogWarn("[%s][%s][%p][%p]%s(%s)", "Trace is not exist, vector may have been released", scope.c_str(),
            vec->GetAllocator(), vec, opName.c_str(), VecOpTypeName::GetName(vecOpType).c_str());
    } else {
        LogTrace("[%s][%s][%p][%p]%s(%s)", "Normal", scope.c_str(), vec->GetAllocator(), vec, opName.c_str(),
            VecOpTypeName::GetName(vecOpType).c_str());
        tracer->Record(opName, vecOpType);
    }
}

int32_t VectorLeakDetector::HashBucket(const Vector *vec)
{
    std::hash<int64_t> hashFunc;
    uint64_t hash = hashFunc(reinterpret_cast<int64_t>(vec));
    return hash % BUCKET_NUM;
}

VectorTracer *VectorLeakDetector::PushTracer(const Vector *vec)
{
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

VectorTracer *VectorLeakDetector::PopTracer(std::atomic<VectorTracer *> &head)
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
        while (tracer != nullptr && tracer->Closed()) {
            head = tracer->next;
            delete tracer;
            deletedCount.fetch_sub(1);
            tracer = head.load();
        }
        // then head is not closed.
        while (tracer != nullptr) {
            if (tracer->next != nullptr && tracer->next->Closed()) {
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

VectorAllocator::VectorAllocator(std::string scope) : scope(scope), leakDetector(scope) {}

VectorAllocator::~VectorAllocator() {}

VectorReference *VectorAllocator::NewVector(int capacityInBytes, int size, VecType type)
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

int64_t VectorAllocator::GetAllocatedBytes()
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
}
}