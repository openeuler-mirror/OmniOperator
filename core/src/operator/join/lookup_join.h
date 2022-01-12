/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join implementations
 */
#ifndef __LOOKUP_JOIN_H__
#define __LOOKUP_JOIN_H__

#include <memory>
#include "join_hash_table.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../../vector/vector_types.h"
#include "../../vector/vector_type.h"
#include "hash_builder.h"
#include "common_join.h"

namespace omniruntime {
namespace op {
class LookupJoinOperatorFactory : public OperatorFactory {
public:
    LookupJoinOperatorFactory(const vec::VecTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
        const vec::VecTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables);
    ~LookupJoinOperatorFactory() override;
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const vec::VecTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType,
        int64_t hashBuilderFactoryAddr);
    Operator *CreateOperator() override;

private:
    std::unique_ptr<vec::VecTypes> probeTypes; // all types for probe
    std::vector<int32_t> probeOutputCols;      // output columns for probe
    std::vector<int32_t> probeHashCols;        // join columns for probe
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;            // output columns for build
    std::unique_ptr<vec::VecTypes> buildOutputTypes; // output column types for build
    JoinType joinType;
    JoinHashTables *hashTables;
    int32_t rowSize;
};

class JoinProbe;
class LookupJoinOutputBuilder;

class LookupJoinOperator : public Operator {
public:
    LookupJoinOperator(const vec::VecTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType,
        JoinHashTables *hashTables, int32_t outputRowSize);
    ~LookupJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

private:
    void ProcessProbe();
    void JoinCurrentPositionWithFilter();
    void JoinCurrentPosition();
    bool AdvanceProbePosition();
    int64_t GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const;

    const vec::VecTypes &probeTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> probeHashCols;
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;
    const vec::VecTypes &buildOutputTypes;
    bool probeOnOuterSide;
    bool currentProbePositionProducedRow;
    JoinHashTables *hashTables;
    JoinProbe *joinProbe;
    int64_t partitionedJoinPosition; // the addressIndex combined partition for build, it is encoded by ((addressIndex
    // << shiftSize) | partition)
    std::unique_ptr<LookupJoinOutputBuilder> outputBuilder;
    ExecutionContext *executionContext = nullptr;
};

class JoinProbe {
public:
    JoinProbe(omniruntime::vec::VectorBatch *input, int32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes,
        int32_t hashColsCount);
    ~JoinProbe();

    int32_t GetPosition() const
    {
        return position;
    }

    omniruntime::vec::Vector **GetProbeAllColumns() const
    {
        return probeAllColumns;
    }

    int32_t GetProbeAllColsCount() const
    {
        return probeAllColsCount;
    }

    bool AdvanceNextPosition();
    int64_t GetCurrentJoinPosition(const JoinHashTables *hashTables) const;

private:
    bool CurrentRowContainsNull() const;

    omniruntime::vec::Vector **probeAllColumns;
    int32_t probeAllColsCount;
    int32_t positionCount;
    omniruntime::vec::Vector **probeHashColumns; // Vector *[join column count]
    int32_t *probeHashColTypes;
    int32_t probeHashColsCount;
    int32_t position;
    int64_t *hashes;
    bool *nulls;
};

class LookupJoinOutputBuilder {
public:
    LookupJoinOutputBuilder(const int32_t *probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, int32_t outputRowSize);
    ~LookupJoinOutputBuilder() {}
    void AppendRow(int32_t probePosition, int64_t partitionedJoinPosition);
    void BuildOutput(VectorAllocator *vecAllocator, const JoinProbe *joinProbe, const JoinHashTables *hashTables,
        std::vector<VectorBatch *> &outputTables);

private:
    const int32_t *probeTypes;
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *buildOutputCols;
    const vec::VecTypes &buildOutputTypes;
    int32_t outputRowSize;
    std::vector<int32_t> probeIndex;
    std::vector<int64_t> buildIndex;
    bool isSequentialProbeIndices;
};
} // end of op
} // end of omniruntime
#endif
