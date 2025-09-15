/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
#include "driver.h"
#include "codegen/time_util.h"
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
        op = nullptr;
    }
    closed_ = true;
}

// Call an Operator method. record silenced throws, but not a query
// terminating throw. Annotate exceptions with Operator info.
#define CALL_OPERATOR(call, operatorPtr, operatorId, operatorMethod)                                    \
        opCallStatus_.Start(operatorId, operatorMethod);                                                \
        call;                                                                                           \
        opCallStatus_.TimeSegmentStatistic(operatorPtr, operatorMethod);                                \
        opCallStatus_.Stop();                                                                           \

void OpCallStatus::Start(int32_t operatorId, const char* operatorMethod)
{
    opId = operatorId;
    method = operatorMethod;
    cpuTimeStartNs = ThreadCpuNanos();
}

void OpCallStatus::Stop()
{
    cpuTimeStartNs = 0;
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
    lockStats.getOutputTime.Add(CpuWallTiming{
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

void OpCallStatus::TimeSegmentStatistic(op::Operator* op, const char* operatorMethod) const
{
    const int64_t cpuTimeSegment = ThreadCpuNanos() - cpuTimeStartNs;
    std::string_view opMethod(operatorMethod);
    if (opMethod != kOpMethodAddInput && opMethod != kOpMethodGetOutput) {
        LogDebug("not input or output for operator");
        return;
    }
    auto &lockedStats = op->stats();
    if (opMethod == kOpMethodAddInput) {
        lockedStats.addInputTime.cpuNanos = cpuTimeSegment / static_cast<int64_t>(1e6);
        lockedStats.addInputTime.count = 1;
    } else if (opMethod == kOpMethodGetOutput) {
        lockedStats.getOutputTime.cpuNanos = cpuTimeSegment / static_cast<int64_t>(1e6);
        lockedStats.getOutputTime.count = 1;
    }
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

                    bool needsInput;
                    CALL_OPERATOR(needsInput = nextOp->needsInput(), nextOp, curOperatorId_ + 1, kOpMethodNeedsInput);
                    if (needsInput) {
                        uint64_t resultBytes = 0;
                        vec::VectorBatch *intermediateResult = nullptr;
                        withDeltaCpuWallTimer(op, &OperatorStats::getOutputTime, [&]() {
                            CALL_OPERATOR(op->GetOutput(&intermediateResult), op, curOperatorId_, kOpMethodGetOutput);
                            if (intermediateResult) {
                                resultBytes = intermediateResult->CalculateTotalSize();
                                {
                                    auto &lockedStats = op->stats();
                                    lockedStats.AddOutputVector(resultBytes, intermediateResult->GetVectorCount(), intermediateResult->GetRowCount());
                                }
                            }
                        });
                        if (intermediateResult != nullptr) {
                            withDeltaCpuWallTimer(nextOp, &OperatorStats::addInputTime, [&]() {
                                {
                                    auto &lockedStats = nextOp->stats();
                                    lockedStats.AddInputVector(resultBytes, intermediateResult->GetVectorCount(), intermediateResult->GetRowCount());
                                }
                                CALL_OPERATOR(nextOp->AddInput(intermediateResult), nextOp, curOperatorId_ + 1,
                                              kOpMethodAddInput);
                            });

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
                    withDeltaCpuWallTimer(op, &OperatorStats::getOutputTime, [&]() {
                        CALL_OPERATOR(op->GetOutput(result), op, curOperatorId_, kOpMethodGetOutput);
                        if (*result != nullptr) {
                            {
                                auto &lockedStats = op->stats();
                                lockedStats.AddOutputVector((*result)->CalculateTotalSize(), (*result)->GetVectorCount(), (*result)->GetRowCount());
                            }
                        }
                    });
                    if (*result != nullptr  && !op->isFinished()) {
                        blockingReason_ = BlockingReason::kWaitForConsumer;
                        return StopReason::kBlock;
                    }

                    bool finished{false};
                    finished = op->isFinished();
                    if (finished) {
                        finished_ = true;
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