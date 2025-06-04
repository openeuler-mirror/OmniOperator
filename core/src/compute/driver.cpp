/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
#include "driver.h"
#include <memory>

namespace omniruntime::compute {
std::atomic_uint64_t BlockingState::numBlockdDrivers_{0};

BlockingState::BlockingState(
    std::shared_ptr<OmniDriver> driver,
    ContinueFuture &&future,
    omniruntime::op::Operator *op,
    BlockingReason reason)
    : driver_(std::move(driver)),
      future_(std::move(future)),
      operator_(op),
      reason_(reason)
{
    numBlockdDrivers_++;
}

vec::VectorBatch *OmniDriver::Next(ContinueFuture *future, StopReason *stopReason)
{
    auto self = shared_from_this();
    std::shared_ptr<BlockingState> blockingState;
    vec::VectorBatch *result = nullptr;
    *stopReason = RunInternal(self, blockingState, &result);

    if (blockingState != nullptr) {
        *future = blockingState->Future();
        return nullptr;
    }

    if (*stopReason == StopReason::kPause) {
        return nullptr;
    }

    return result;
}

void OmniDriver::close()
{
    if (closed_) {
        return;
    }
    for (auto &op : operators_) {
        op->Close();
    }
    closed_ = true;
}

StopReason OmniDriver::RunInternal(
    std::shared_ptr<OmniDriver> &self,
    std::shared_ptr<BlockingState> &blockingState,
    vec::VectorBatch **result)
{
    try {
        const uint32_t numOperators = operators_.size();
        ContinueFuture future = OmniFuture::makeEmpty();
        for (;;) {
            for (int32_t i = numOperators - 1; i >= 0; --i) {
                auto *op = operators_[i].get();
                curOperatorId_ = i;

                blockingReason_ = op->IsBlocked(&future);
                if (blockingReason_ != BlockingReason::kNotBlocked) {
                    return BlockDriver(self, i, std::move(future), blockingState);
                }

                if (i < numOperators - 1) {
                    auto *nextOp = operators_[i + 1].get();
                    blockingReason_ = nextOp->IsBlocked(&future);
                    if (blockingReason_ != BlockingReason::kNotBlocked) {
                        return BlockDriver(self, i + 1, std::move(future), blockingState);
                    }

                    bool needsInput = nextOp->needsInput();
                    if (needsInput) {
                        vec::VectorBatch *intermediateResult = nullptr;
                        op->GetOutput(&intermediateResult);
                        if (intermediateResult != nullptr) {
                            nextOp->AddInput(intermediateResult);

                            // The next iteration will see if operators_[i + 1] has
                            // output now that it got input
                            i += 2;
                            continue;
                        } else {
                            blockingReason_ = op->IsBlocked(&future);
                            if (blockingReason_ != BlockingReason::kNotBlocked) {
                                return BlockDriver(self, i, std::move(future), blockingState);
                            }
                            if (op->isFinished()) {
                                nextOp->noMoreInput();
                                break;
                            }
                        }
                    }
                } else {
                    if (outputDriver) {
                        op->GetOutput(result);
                    }
                    if (*result != nullptr  && !op->isFinished()) {
                        blockingReason_ = BlockingReason::kWaitForConsumer;
                        return StopReason::kBlock;
                    }

                    bool finished{false};
                    finished = op->isFinished();
                    if (finished) {
                        close();
                        return StopReason::kAtEnd;
                    }
                }
            }
        }
    } catch (const std::exception &e) {
        throw std::runtime_error(e.what());
    }
}

StopReason OmniDriver::BlockDriver(
    const std::shared_ptr<OmniDriver> &self,
    size_t blockedOperatorId,
    ContinueFuture &&future,
    std::shared_ptr<BlockingState> &blockingState)
{
    auto *op = operators_[blockedOperatorId].get();
    blockedOperatorId_ = blockedOperatorId;
    blockingState = std::make_shared<BlockingState>(
        self, std::move(future), op, blockingReason_);
    return StopReason::kBlock;
}
} // end of omniruntime