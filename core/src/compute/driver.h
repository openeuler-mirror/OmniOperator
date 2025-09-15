/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 */
#ifndef __DRIVER_H__
#define __DRIVER_H__
 
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
 
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "vector/vector_batch.h"
#include "compute/reason.h"
#include "plannode/planNode.h"
#include "plannode/RowVectorStream.h"

namespace omniruntime {

namespace compute {

using OperatorSupplier = std::function<
    std::unique_ptr<omniruntime::op::Operator>(const OperatorConfig& operatorConfig)>;

class BlockingState;
class OmniTask;

constexpr const char* kOpMethodNone = "";
constexpr const char* kOpMethodIsBlocked = "isBlocked";
constexpr const char* kOpMethodNeedsInput = "needsInput";
constexpr const char* kOpMethodGetOutput = "getOutput";
constexpr const char* kOpMethodAddInput = "addInput";
constexpr const char* kOpMethodNoMoreInput = "noMoreInput";
constexpr const char* kOpMethodIsFinished = "isFinished";


/// Same as the structure below, but does not have atomic members.
/// Used to return the status from the struct with atomics.
struct OpCallStatusRaw {
    /// cpu time (ms) when the operator call started.
    clock_t cpuTimeStartMs{0};
    /// wall Time (ms) when the operator call started.
    size_t timeStartMs{0};
    /// Id of the operator, method of which is currently running. It is index into
    /// the vector of Driver's operators.
    int32_t opId{0};
    /// Method of the operator, which is currently running.
    const char* method{kOpMethodNone};
};

/// Structure holds the information about the current operator call the driver
/// is in. Can be used to detect deadlocks and otherwise blocked calls.
/// If timeStartMs is zero, then we aren't in an operator call.
struct OpCallStatus {
    OpCallStatus()
    {
    }

    /// The status accessor.
    OpCallStatusRaw operator()() const
    {
        return OpCallStatusRaw{cpuTimeStartNs, timeStartMs, opId, method};
    }

    void Start(int32_t operatorId, const char* operatorMethod);
    void Stop();
    void TimeSegmentStatistic(op::Operator* op, const char* operatorMethod) const;

private:
    /// cpu time (ns) when the operator call started.
    int64_t cpuTimeStartNs{0};
    /// wall Time (ms) when the operator call started.
    size_t  timeStartMs{0};
    /// Id of the operator, method of which is currently running. It is index into
    /// the vector of Driver's operators.
    std::atomic_int32_t opId{0};
    /// Method of the operator, which is currently running.
    std::atomic<const char*> method{kOpMethodNone};
};

class OmniDriver : public std::enable_shared_from_this<OmniDriver> {
public:
    OmniDriver()
        : curOperatorId_(0),
          blockingReason_(BlockingReason::kNotBlocked),
          blockedOperatorId_(0) {}
 
    // Run this pipeline until it produces a batch of data or get blocked.
    vec::VectorBatch* Next(ContinueFuture* future, StopReason* stopReason);

    void addOperator(std::unique_ptr<omniruntime::op::Operator> operatorPtr)
    {
        operators_.emplace_back(std::move(operatorPtr));
    }

    void close();

    std::vector<std::shared_ptr<omniruntime::op::Operator>>* operators()
    {
        return &operators_;
    }

    ALWAYS_INLINE bool isFinished() const
    {
        return finished_;
    }

public:
    bool inputDriver{false};
    bool outputDriver{false};

    bool shouldStop{false};

private:
 
    StopReason RunInternal(
        std::shared_ptr<OmniDriver>& self,
        std::shared_ptr<BlockingState>& blockingState,
        vec::VectorBatch** result);

    ALWAYS_INLINE StopReason BlockDriver(
        const std::shared_ptr<OmniDriver>& self,
        size_t blockedOperatorId,
        ContinueFuture&& future,
        std::shared_ptr<BlockingState>& blockingState);

    // Index of the current operator to run (or the 1st one if we haven't stated yet).
    size_t curOperatorId_;
 
    std::vector<std::shared_ptr<omniruntime::op::Operator>> operators_;
 
    BlockingReason blockingReason_;
    size_t blockedOperatorId_;
    bool trackOperatorCpuUsage_ = true;

    OpCallStatus opCallStatus_;
    CpuWallTiming processLazyIoStats(omniruntime::op::Operator& op, const CpuWallTiming& timing);
    using TimingMemberPtr = CpuWallTiming OperatorStats::*;
    template <typename Func>
    void withDeltaCpuWallTimer(omniruntime::op::Operator* op, TimingMemberPtr opTimingMember, Func&& opFunction);

    bool closed_{false};
    bool finished_{false};
};

class BlockingState {
public:
    BlockingState(
        std::shared_ptr<OmniDriver> driver,
        ContinueFuture&& future,
        omniruntime::op::Operator* op,
        BlockingReason reason);

    ~BlockingState()
    {
        numBlockdDrivers_--;
    }

    ContinueFuture Future()
    {
        return std::move(future_);
    }

private:
    std::shared_ptr<OmniDriver> driver_;
    ContinueFuture future_;
    omniruntime::op::Operator* operator_;
    BlockingReason reason_;

    static std::atomic_uint64_t numBlockdDrivers_;
};

} // end of compute
} // end of omniruntime
#endif