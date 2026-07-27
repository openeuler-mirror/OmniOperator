#include "two_step_join.h"

#include <chrono>
#include <iostream>
#include <utility>

namespace omniruntime {
namespace op {

namespace {

using ProfileClock = std::chrono::steady_clock;

inline uint64_t ElapsedNs(const ProfileClock::time_point& start)
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            ProfileClock::now() - start)
            .count());
}

inline double NsToMs(uint64_t ns)
{
    return static_cast<double>(ns) / 1.0e6;
}

} // namespace

//
// TwoStepJoinOperator
//

TwoStepJoinOperator::TwoStepJoinOperator(
    LookupJoinOperator* join1,
    LookupJoinOperator* join2,
    std::shared_ptr<TwoStepJoinRuntimeStats> runtimeStats)
    : join1(join1),
      join2(join2),
      intermediateIndex(0),
      join1DrainedForCurrentInput(true),
      join2HasActiveInput(false),
      runtimeStats(std::move(runtimeStats))
{
}

TwoStepJoinOperator::~TwoStepJoinOperator()
{
    if (join1) {
        Operator::DeleteOperator(join1);
        join1 = nullptr;
    }

    if (join2) {
        Operator::DeleteOperator(join2);
        join2 = nullptr;
    }

    // Only batches not yet handed to join2 are still owned here.
    for (auto* batch : intermediateResults) {
        if (batch) {
            vec::VectorHelper::FreeVecBatch(batch);
        }
    }
    intermediateResults.clear();
}

int32_t TwoStepJoinOperator::AddInput(VectorBatch* input)
{
    if (!input) {
        return OMNI_STATUS_NORMAL;
    }

    if (runtimeStats) {
        runtimeStats->inputRows +=
            static_cast<uint64_t>(input->GetRowCount());
        ++runtimeStats->inputBatches;
        ++runtimeStats->join1AddInputCalls;
    }

    UpdateAddInputInfo(input->GetRowCount());

    // One AddInput means join1 now has one new input that must be drained.
    join1DrainedForCurrentInput = false;

    const auto start = ProfileClock::now();
    const int32_t status = join1->AddInput(input);

    if (runtimeStats) {
        runtimeStats->join1AddInputNs += ElapsedNs(start);
    }

    return status;
}

int32_t TwoStepJoinOperator::DrainJoin1()
{
    if (join1DrainedForCurrentInput) {
        return OMNI_STATUS_NORMAL;
    }

    constexpr uint64_t MAX_DRAIN_ITERATIONS = 1000000;
    uint64_t drainIterations = 0;

    while (drainIterations < MAX_DRAIN_ITERATIONS) {
        ++drainIterations;

        VectorBatch* intermediate = nullptr;

        if (runtimeStats != nullptr) {
            ++runtimeStats->join1GetOutputCalls;
        }

        const auto start = ProfileClock::now();

        const int32_t status = join1->GetOutput(&intermediate);

        if (runtimeStats != nullptr) {
            runtimeStats->join1GetOutputNs += ElapsedNs(start);
        }

        if (status != OMNI_STATUS_NORMAL) {
            return status;
        }

        if (intermediate == nullptr) {
            join1DrainedForCurrentInput = true;
            return OMNI_STATUS_NORMAL;
        }

        if (runtimeStats != nullptr) {
            runtimeStats->intermediateRows +=
                static_cast<uint64_t>(intermediate->GetRowCount());

            ++runtimeStats->intermediateBatches;
        }

        intermediateResults.push_back(intermediate);
    }

    return OMNI_STATUS_ERROR;
}

void TwoStepJoinOperator::RecordFinalOutput(VectorBatch* output)
{
    if (output == nullptr) {
        return;
    }

    if (runtimeStats) {
        runtimeStats->finalOutputRows +=
            static_cast<uint64_t>(output->GetRowCount());
        ++runtimeStats->finalOutputBatches;
    }

    // Make the outer TwoStepJoin benchmark statistics reflect
    // the real final cardinality returned by join2.
    UpdateGetOutputInfo(output->GetRowCount());
}

int32_t TwoStepJoinOperator::DrainActiveJoin2Input(VectorBatch** output)
{
    *output = nullptr;

    if (!join2HasActiveInput) {
        return OMNI_STATUS_NORMAL;
    }

    VectorBatch* result = nullptr;

    if (runtimeStats) {
        ++runtimeStats->join2GetOutputCalls;
    }

    const auto start = ProfileClock::now();
    const int32_t status = join2->GetOutput(&result);

    if (runtimeStats) {
        runtimeStats->join2GetOutputNs += ElapsedNs(start);
    }

    if (result != nullptr) {
        RecordFinalOutput(result);
        *output = result;

        // Keep true: the same join2 input may still have another output batch.
        return status;
    }

    // nullptr means this join2 input has been drained.
    join2HasActiveInput = false;

    if (status == OMNI_STATUS_NORMAL) {
        UpdateGetOutputInfo(0);
    }

    return status;
}

int32_t TwoStepJoinOperator::GetOutput(VectorBatch** output)
{
    if (!output) {
        return OMNI_STATUS_NORMAL;
    }

    *output = nullptr;

    // If the previous intermediate input produced a final batch,
    // drain the rest of that same join2 input before adding a new one.
    if (join2HasActiveInput) {
        const int32_t status = DrainActiveJoin2Input(output);
        if (*output != nullptr || status != OMNI_STATUS_NORMAL) {
            return status;
        }
    }

    // Step 1: collect all AB intermediate batches for the current A input.
    const int32_t join1Status = DrainJoin1();
    if (join1Status != OMNI_STATUS_NORMAL) {
        return join1Status;
    }

    // Step 2: feed each AB batch into join2.
    while (intermediateIndex < intermediateResults.size()) {
        VectorBatch* inter = intermediateResults[intermediateIndex];

        // Ownership is transferred to join2. Null the slot so our destructor
        // does not free a batch already owned/consumed by join2.
        intermediateResults[intermediateIndex] = nullptr;
        ++intermediateIndex;

        if (inter == nullptr) {
            continue;
        }

        if (runtimeStats) {
            ++runtimeStats->join2AddInputCalls;
        }

        const auto addStart = ProfileClock::now();
        const int32_t addStatus = join2->AddInput(inter);

        if (runtimeStats) {
            runtimeStats->join2AddInputNs += ElapsedNs(addStart);
        }

        if (addStatus != OMNI_STATUS_NORMAL) {
            return addStatus;
        }

        join2HasActiveInput = true;

        const int32_t outputStatus = DrainActiveJoin2Input(output);
        if (*output != nullptr || outputStatus != OMNI_STATUS_NORMAL) {
            return outputStatus;
        }
    }

    intermediateResults.clear();
    intermediateIndex = 0;

    UpdateGetOutputInfo(0);
    return OMNI_STATUS_NORMAL;
}

//
// TwoStepJoinOperatorFactory
//

TwoStepJoinOperatorFactory::TwoStepJoinOperatorFactory(
    LookupJoinOperatorFactory* factory1,
    LookupJoinOperatorFactory* factory2)
    : factory1(factory1),
      factory2(factory2),
      runtimeStats(std::make_shared<TwoStepJoinRuntimeStats>())
{
}

TwoStepJoinOperatorFactory::~TwoStepJoinOperatorFactory()
{
    factory1 = nullptr;
    factory2 = nullptr;
}

Operator* TwoStepJoinOperatorFactory::CreateOperator()
{
    if (!factory1 || !factory2) {
        return nullptr;
    }

    LookupJoinOperator* join1 =
        static_cast<LookupJoinOperator*>(factory1->CreateOperator());
    LookupJoinOperator* join2 =
        static_cast<LookupJoinOperator*>(factory2->CreateOperator());

    if (!join1 || !join2) {
        if (join1) {
            Operator::DeleteOperator(join1);
        }
        if (join2) {
            Operator::DeleteOperator(join2);
        }
        return nullptr;
    }

    return new TwoStepJoinOperator(join1, join2, runtimeStats);
}

const TwoStepJoinRuntimeStats&
TwoStepJoinOperatorFactory::GetRuntimeStats() const
{
    return *runtimeStats;
}

void TwoStepJoinOperatorFactory::PrintRuntimeStats() const
{
    const auto& stats = *runtimeStats;

    const uint64_t join1TotalNs =
        stats.join1AddInputNs + stats.join1GetOutputNs;

    const uint64_t join2TotalNs =
        stats.join2AddInputNs + stats.join2GetOutputNs;

    const uint64_t totalMeasuredNs =
        join1TotalNs + join2TotalNs;

    std::cout
        << "\n=== TraditionalTwoStepJoin Runtime Stats ===\n"

        << "[Input]\n"
        << "Input rows                 : " << stats.inputRows << "\n"
        << "Input batches              : " << stats.inputBatches << "\n"

        << "[Join1 A->B]\n"
        << "join1 AddInput calls       : " << stats.join1AddInputCalls << "\n"
        << "join1 AddInput ms          : " << NsToMs(stats.join1AddInputNs) << "\n"
        << "join1 GetOutput calls      : " << stats.join1GetOutputCalls << "\n"
        << "join1 GetOutput ms         : " << NsToMs(stats.join1GetOutputNs) << "\n"
        << "Intermediate rows (A->B)   : " << stats.intermediateRows << "\n"
        << "Intermediate batches       : " << stats.intermediateBatches << "\n"
        << "join1 total measured ms    : " << NsToMs(join1TotalNs) << "\n"

        << "[Join2 AB->C]\n"
        << "join2 AddInput calls       : " << stats.join2AddInputCalls << "\n"
        << "join2 AddInput ms          : " << NsToMs(stats.join2AddInputNs) << "\n"
        << "join2 GetOutput calls      : " << stats.join2GetOutputCalls << "\n"
        << "join2 GetOutput ms         : " << NsToMs(stats.join2GetOutputNs) << "\n"
        << "Final output rows (AB->C)  : " << stats.finalOutputRows << "\n"
        << "Final output batches       : " << stats.finalOutputBatches << "\n"
        << "join2 total measured ms    : " << NsToMs(join2TotalNs) << "\n"

        << "[Total]\n"
        << "Measured join total ms     : " << NsToMs(totalMeasuredNs) << "\n"
        << "============================================\n";
}

} // namespace op
} // namespace omniruntime
