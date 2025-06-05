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

// Call an Operator method. record silenced throws, but not a query
// terminating throw. Annotate exceptions with Operator info.
#define CALL_OPERATOR(call, operatorPtr, operatorId, operatorMethod)                             \
    try {                                                                                        \
        opCallStatus_.start(operatorId, operatorMethod);                                         \
        opCallStatus_.stop();                                                                    \
        call;                                                                                    \
    } catch (const std::exception& e) {                                                          \
        LogError("Operator::%d failed for [operator: %d, plan node ID: %d]: %d", operatorMethod, \
            (operatorPtr)->operatorType(), (operatorPtr)->planNodeId(), e.what());               \
    }

void OpCallStatus::start(int32_t operatorId, const char* operatorMethod)
{
    auto start = std::chrono::system_clock::now();
    timeStartMs = std::chrono::duration_cast<std::chrono::milliseconds>(start.time_since_epoch())
      .count();
    opId = operatorId;
    method = operatorMethod;
}

void OpCallStatus::stop()
{
    timeStartMs = 0;
}

size_t OpCallStatusRaw::callDuration() const
{
    return empty() ? 0 : (std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() - timeStartMs);
}

CpuWallTiming OmniDriver::processLazyIoStats(op::Operator& op, const CpuWallTiming& timing)
{
    if (&op == operators_[0].get()) {
        return timing;
    }
    auto lockStats = op.stats();

    int64_t wallDelta = 0;
    uint64_t inputBytesDelta = 0;
    wallDelta = std::min<int64_t>(wallDelta, timing.wallNanos);
    lockStats = operators_[0]->stats();
    lockStats.getOutputTiming.Add(CpuWallTiming{
        1, wallDelta, 0
    });
    lockStats.inputBytes += inputBytesDelta;
    lockStats.outputBytes += inputBytesDelta;
    return CpuWallTiming{
        1,
        timing.wallNanos - wallDelta,
        timing.cpuNanos - 0,
    };
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
                if (shouldStop) {
                    return StopReason::kAtEnd;
                }
                auto *op = operators_[i].get();
                curOperatorId_ = i;

                withDeltaCpuWallTimer(op, &OperatorStats::isBlockedTiming, [&]() {
                    CALL_OPERATOR(blockingReason_ = op->IsBlocked(&future), op, curOperatorId_, kOpMethodIsBlocked);
                });
                if (blockingReason_ != BlockingReason::kNotBlocked) {
                    return BlockDriver(self, i, std::move(future), blockingState);
                }

                if (i < numOperators - 1) {
                    auto *nextOp = operators_[i + 1].get();
                    withDeltaCpuWallTimer(nextOp, &OperatorStats::isBlockedTiming, [&]() {
                       CALL_OPERATOR(blockingReason_ = nextOp->IsBlocked(&future), nextOp, curOperatorId_ + 1,
                                     kOpMethodIsBlocked);
                    });
                    if (blockingReason_ != BlockingReason::kNotBlocked) {
                        return BlockDriver(self, i + 1, std::move(future), blockingState);
                    }

                    bool needsInput;
                    CALL_OPERATOR(needsInput = nextOp->needsInput(), nextOp, curOperatorId_ + 1, kOpMethodNeedsInput);
                    if (needsInput) {
                        uint64_t resultBytes = 0;
                        vec::VectorBatch *intermediateResult = nullptr;
                        withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
                            CALL_OPERATOR(op->GetOutput(&intermediateResult), op, curOperatorId_, kOpMethodGetOutput);
                            if (intermediateResult) {
                                resultBytes = intermediateResult->CalculateTotalSize();
                                {
                                    auto &lockedStats = op->stats();
                                    lockedStats.AddOutputVector(resultBytes, intermediateResult->GetCapacity());
                                }
                            }
                        });
                        if (intermediateResult != nullptr) {
                            withDeltaCpuWallTimer(nextOp, &OperatorStats::addInputTiming, [&]() {
                                {
                                    auto &lockedStats = nextOp->stats();
                                    lockedStats.addInputVector(resultBytes, intermediateResult->GetCapacity());
                                }
                                CALL_OPERATOR(nextOp->AddInput(intermediateResult), nextOp, curOperatorId_ + 1,
                                              kOpMethodAddInput);
                            });

                            // The next iteration will see if operators_[i + 1] has
                            // output now that it got input
                            i += 2;
                            continue;
                        } else {
                            withDeltaCpuWallTimer(op, &OperatorStats::isBlockedTiming, [&]() {
                                CALL_OPERATOR(blockingReason_ = op->IsBlocked(&future), op, curOperatorId_,
                                              kOpMethodIsBlocked);
                            });
                            if (blockingReason_ != BlockingReason::kNotBlocked) {
                                return BlockDriver(self, i, std::move(future), blockingState);
                            }
                            if (op->isFinished()) {
                                withDeltaCpuWallTimer(nextOp, &OperatorStats::finishTiming, [this, &nextOp]() {
                                        CALL_OPERATOR(nextOp->noMoreInput(), nextOp, curOperatorId_ + 1,
                                                      kOpMethodNoMoreInput);
                                });
                                break;
                            }
                        }
                    }
                } else {
                    op->GetOutput(result);
                    withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
                        CALL_OPERATOR(op->GetOutput(result), op, curOperatorId_, kOpMethodGetOutput);
                        if (*result != nullptr) {
                            {
                                auto &lockedStats = op->stats();
                                lockedStats.AddOutputVector((*result)->CalculateTotalSize(), (*result)->GetCapacity());
                            }
                        }
                    });
                    if (*result != nullptr  && !op->isFinished()) {
                        blockingReason_ = BlockingReason::kWaitForConsumer;
                        return StopReason::kBlock;
                    }

                    bool finished{false};
                    withDeltaCpuWallTimer(op, &OperatorStats::finishTiming, [&]() {
                        CALL_OPERATOR(finished = op->isFinished(), op, curOperatorId_, kOpMethodIsFinished);
                    });
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
#undef CALL_OPERATOR

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

template <typename Func>
void OmniDriver::withDeltaCpuWallTimer(op::Operator* op, TimingMemberPtr opTimingMember, Func&& opFunction)
{
    // If 'trackOperatorCpuUsage_' is true, create and initialize the timer object
    // to track cpu and wall time of the opFunction.
    if (!trackOperatorCpuUsage_) {
        opFunction();
        return;
    }

    // The delta CpuWallTiming object would be recorded to the corresponding
    // 'opTimingMember' upon destruction of the timer when withDeltaCpuWallTimer
    // ends. The timer is created on the stack to avoid heap allocation
    auto f = [op, opTimingMember, this](const CpuWallTiming& elapsedTime) {
        auto elapsedSelfTime = processLazyIoStats(*op, elapsedTime);
        (op->stats().*opTimingMember).Add(elapsedSelfTime);
    };
    DeltaCpuWallTimer<decltype(f)> timer(std::move(f));

    opFunction();
}
} // end of omniruntime