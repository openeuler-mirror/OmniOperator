/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: spill tracker implementation
 */

#ifndef OMNI_RUNTIME_SPILL_TRACKER_H
#define OMNI_RUNTIME_SPILL_TRACKER_H

#include <atomic>

namespace omniruntime {
namespace op {
class SpillTracker {
public:
    SpillTracker() = default;

    virtual ~SpillTracker() = default;

    virtual bool CheckIfExceedAndReserve(uint64_t bytes) = 0;

    virtual uint64_t GetSpilledBytes() = 0;
};

class ChildSpillTracker : public SpillTracker {
public:
    explicit ChildSpillTracker(SpillTracker *parentSpillTracker, uint64_t maxSpillBytes)
        : parentSpillTracker(parentSpillTracker), spilledBytes(0), maxSpillBytes(maxSpillBytes)
    {}

    ~ChildSpillTracker() override = default;

    bool CheckIfExceedAndReserve(uint64_t bytes) override;

    uint64_t GetSpilledBytes() override
    {
        return spilledBytes;
    }

private:
    SpillTracker *parentSpillTracker;
    uint64_t spilledBytes;
    uint64_t maxSpillBytes;
};

class RootSpillTracker : public SpillTracker {
public:
    RootSpillTracker() noexcept : maxBytes(UINT64_MAX), spilledBytes(0), totalSpilledBytes(0) {}

    ~RootSpillTracker() override;

    void SetMaxBytes(uint64_t maxSize)
    {
        maxBytes = maxSize;
    }

    uint64_t GetSpilledBytes() override
    {
        return totalSpilledBytes;
    }

    bool CheckIfExceedAndReserve(uint64_t bytes) override;

    SpillTracker *CreateSpillTracker(uint64_t maxSpillBytes)
    {
        return new ChildSpillTracker(this, maxSpillBytes);
    }

private:
    uint64_t maxBytes;
    std::atomic<uint64_t> spilledBytes;
    uint64_t totalSpilledBytes;
};

void InitRootSpillTracker(uint64_t maxSize);
RootSpillTracker &GetRootSpillTracker();
}
}

#endif // OMNI_RUNTIME_SPILL_TRACKER_H
