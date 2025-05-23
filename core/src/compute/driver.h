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
class DriverFactory;

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

    void init(
        std::vector<std::unique_ptr<omniruntime::op::Operator>> operators);

    void close();
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
 
    std::vector<std::unique_ptr<omniruntime::op::Operator>> operators_;
 
    BlockingReason blockingReason_;
    size_t blockedOperatorId_;
    std::atomic_bool closed_{false};
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


class DriverFactory {
public:
    std::vector<omniruntime::op::OperatorFactory*> operatorFactories;
    // Function taht will generate the final operator of a driver being constructed
    OperatorSupplier consumerSupplier;
    // Maximum number of drivers that can be run concurrently in this pipeline
    uint32_t maxDrivers;
    // Number of drivers that will be run concurrently in this pipeline for the whole task
    uint32_t numDrivers;
    // Total number of drivers in this pipeline we expect to be run
    uint32_t numTotalDrivers;
    // True if 'planNodes' contains a source node for the task, e.g. TableScan or Exchange
    bool inputDriver{false};
    // True if 'planNodes' contains a sync node for the task, e.g. PartitionedOutput
    bool outputDriver{false};

    std::shared_ptr<OmniDriver> CreateDriver();

    ~DriverFactory()
    {
        for (auto* factory : operatorFactories) {
            delete factory;
        }
        operatorFactories.clear();
    }
};

} // end of compute
} // end of omniruntime
#endif