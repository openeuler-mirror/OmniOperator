/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: spill unit iterator
 */
#ifndef OMNI_RUNTIME_SPILL_TRACKER_H
#define OMNI_RUNTIME_SPILL_TRACKER_H

#include <atomic>
#include <set>
#include <string>

namespace omniruntime {
namespace op {
class SpillTracker {
public:
    SpillTracker() = default;

    virtual ~SpillTracker() = default;

    virtual bool CheckIfExceedAndReserve(uint64_t bytes) = 0;

    virtual void Free(uint64_t bytes) = 0;

    virtual uint64_t GetSpilledBytes() = 0;
};

class ChildSpillTracker : public SpillTracker {
public:
    explicit ChildSpillTracker(SpillTracker *parentSpillTracker)
        : parentSpillTracker(parentSpillTracker), spilledBytes(0)
    {}

    ~ChildSpillTracker() override = default;

    bool CheckIfExceedAndReserve(uint64_t bytes) override
    {
        if (parentSpillTracker->CheckIfExceedAndReserve(bytes)) {
            return true;
        } else {
            spilledBytes += bytes;
            return false;
        }
    }

    void Free(uint64_t bytes) override
    {
        parentSpillTracker->Free(bytes);
    }

    uint64_t GetSpilledBytes() override
    {
        return spilledBytes;
    }

private:
    SpillTracker *parentSpillTracker;
    uint64_t spilledBytes;
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
        return spilledBytes;
    }

    bool CheckIfExceedAndReserve(uint64_t bytes) override;

    void Free(uint64_t bytes) override
    {
        spilledBytes.fetch_sub(bytes);
    }

    SpillTracker *CreateSpillTracker()
    {
        return new ChildSpillTracker(this);
    }

    bool IsSpillPathPresent(const std::string &spillPath)
    {
        return spillPaths.find(spillPath) != spillPaths.end();
    }

    void AddSpillPath(const std::string &spillPath)
    {
        spillPaths.insert(spillPath);
    }

private:
    uint64_t maxBytes;
    std::atomic<uint64_t> spilledBytes;
    uint64_t totalSpilledBytes;
    std::set<std::string> spillPaths;
};

void InitRootSpillTracker(std::string &spillPath, uint64_t maxSize);
RootSpillTracker &GetRootSpillTracker();
}
}

#endif // OMNI_RUNTIME_SPILL_TRACKER_H
