/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join implementations
 */
#ifndef __LOOKUP_JOIN_H__
#define __LOOKUP_JOIN_H__

#include <memory>
#include "join_hash_table.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "type/data_type.h"
#include "hash_builder.h"
#include "common_join.h"

namespace omniruntime {
namespace op {
class LookupJoinOperatorFactory : public OperatorFactory {
public:
    LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
        const type::DataTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables);
    ~LookupJoinOperatorFactory() override;
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, const DataTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr);
    Operator *CreateOperator() override;

private:
    DataTypes probeTypes;                 // all types for probe
    std::vector<int32_t> probeOutputCols; // output columns for probe
    std::vector<int32_t> probeHashCols;   // join columns for probe
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols; // output columns for build
    DataTypes buildOutputTypes;           // output column types for build
    JoinType joinType;
    JoinHashTables *hashTables;
    int32_t rowSize;
};

class JoinProbe;
class LookupJoinOutputBuilder;

class LookupJoinOperator : public Operator {
public:
    LookupJoinOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, const type::DataTypes &buildOutputTypes, JoinType joinType,
        JoinHashTables *hashTables, int32_t outputRowSize);
    ~LookupJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

private:
    void ProcessProbe();
    void JoinCurrentPositionWithFilter();
    void JoinCurrentPosition();
    bool AdvanceProbePosition();
    uint64_t GetNextJoinPosition(uint64_t currentJoinPosition) const;

    DataTypes probeTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> probeHashCols;
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;
    DataTypes buildOutputTypes;
    bool probeOnOuterSide;
    bool needTrackPosition;
    bool currentProbePositionProducedRow;
    JoinHashTables *hashTables;
    JoinProbe *joinProbe;
    uint64_t partitionedJoinPosition; // the addressIndex combined partition for build, it is encoded by ((addressIndex
    // << shiftSize) | partition)
    std::unique_ptr<LookupJoinOutputBuilder> outputBuilder;
    ExecutionContext *executionContext = nullptr;
    omniruntime::vec::VectorBatch *input = nullptr;
};

class JoinProbe {
public:
    JoinProbe(omniruntime::vec::VectorBatch *input, uint32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes,
        uint32_t hashColsCount);
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
    uint64_t GetCurrentJoinPosition(const JoinHashTables *hashTables) const;

private:
    omniruntime::vec::Vector **probeAllColumns;
    uint32_t probeAllColsCount;
    uint32_t positionCount;
    omniruntime::vec::Vector **probeHashColumns; // Vector *[join column count]
    int32_t *probeHashColTypes;
    uint32_t probeHashColsCount;
    int32_t position;
    int64_t *hashes;
    bool *nulls;
};

class LookupJoinOutputBuilder {
public:
    LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols,
        const type::DataTypes &buildOutputTypes, int32_t outputRowSize);
    ~LookupJoinOutputBuilder() = default;
    void AppendRow(int32_t probePosition, uint64_t partitionedJoinPosition);
    void BuildOutput(VectorAllocator *vecAllocator, const JoinProbe *joinProbe, const JoinHashTables *hashTables,
        std::vector<VectorBatch *> &outputVecBatches);

private:
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *buildOutputCols;
    DataTypes buildOutputTypes;
    int32_t outputRowSize;
    std::vector<int32_t> probeIndex;
    std::vector<uint64_t> buildIndex;
    bool isSequentialProbeIndices;
};
} // end of op
} // end of omniruntime
#endif
