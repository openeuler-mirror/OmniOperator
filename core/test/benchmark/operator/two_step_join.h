#ifndef __TWO_STEP_JOIN_H__
#define __TWO_STEP_JOIN_H__

#include <cstdint>
#include <memory>
#include <vector>

#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/join/lookup_join.h"

namespace omniruntime {
namespace op {

struct TwoStepJoinRuntimeStats {
    uint64_t inputRows = 0;
    uint64_t inputBatches = 0;

    // Join1: A -> B
    uint64_t join1AddInputCalls = 0;
    uint64_t join1AddInputNs = 0;

    uint64_t join1GetOutputCalls = 0;
    uint64_t join1GetOutputNs = 0;

    uint64_t intermediateRows = 0;
    uint64_t intermediateBatches = 0;

    // Join2: AB -> C
    uint64_t join2AddInputCalls = 0;
    uint64_t join2AddInputNs = 0;

    uint64_t join2GetOutputCalls = 0;
    uint64_t join2GetOutputNs = 0;

    uint64_t finalOutputRows = 0;
    uint64_t finalOutputBatches = 0;
};

/**
 * Two-step join operator that executes:
 * Step 1: A JOIN B -> AB
 * Step 2: AB JOIN C -> ABC
 */
class TwoStepJoinOperator : public Operator {
public:
    TwoStepJoinOperator(
        LookupJoinOperator* join1,
        LookupJoinOperator* join2,
        std::shared_ptr<TwoStepJoinRuntimeStats> runtimeStats);

    ~TwoStepJoinOperator() override;

    int32_t AddInput(VectorBatch* input) override;

    int32_t GetOutput(VectorBatch** output) override;

private:
    int32_t DrainJoin1();
    int32_t DrainActiveJoin2Input(VectorBatch** output);
    void RecordFinalOutput(VectorBatch* output);

    LookupJoinOperator* join1;
    LookupJoinOperator* join2;

    std::vector<VectorBatch*> intermediateResults;
    size_t intermediateIndex;

    bool join1DrainedForCurrentInput;
    bool join2HasActiveInput;

    std::shared_ptr<TwoStepJoinRuntimeStats> runtimeStats;
};

/**
 * Factory for creating two-step join operators.
 *
 * Does NOT own factory1/factory2.
 */
class TwoStepJoinOperatorFactory : public OperatorFactory {
public:
    TwoStepJoinOperatorFactory(
        LookupJoinOperatorFactory* factory1,
        LookupJoinOperatorFactory* factory2);

    ~TwoStepJoinOperatorFactory() override;

    Operator* CreateOperator() override;

    const TwoStepJoinRuntimeStats& GetRuntimeStats() const;

    void PrintRuntimeStats() const;

private:
    LookupJoinOperatorFactory* factory1;
    LookupJoinOperatorFactory* factory2;

    std::shared_ptr<TwoStepJoinRuntimeStats> runtimeStats;
};

} // namespace op
} // namespace omniruntime

#endif // __TWO_STEP_JOIN_H__
