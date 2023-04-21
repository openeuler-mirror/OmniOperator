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
class LookupJoinOutputBuilder {
public:
    LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols,
        int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, int32_t outputRowSize);
    ~LookupJoinOutputBuilder() = default;
    void AppendRow(int32_t probePosition, const JoinHashTable *hashtable, uint32_t joinPosition);
    void BuildOutput(VectorAllocator *vecAllocator, BaseVector **probeOutputColumns, VectorBatch **outputVecBatch);
    void ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeAllColumns);
    void ConstructBuildColumns(VectorBatch *vectorBatch, VectorAllocator *vecAllocator);

    ALWAYS_INLINE bool IsFull()
    {
        return probeRowCount >= maxRowCount;
    }

    ALWAYS_INLINE bool IsEmpty()
    {
        return probeRowCount == 0;
    }

private:
    ALWAYS_INLINE void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, Vector **probeOutputColumns)
    {
        BaseVector *probeColumn = nullptr;
        for (int32_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            // we want to keep only one level dictionary vector here
            // if the data is non-dictionary, we build dictionary to avoid data copy
            if (column->GetEncoding() == vec::OMNI_DICTIONARY) {
                probeColumn = column->CopyPositions(&probeIndex[0], 0, probeRowCount);
            } else {
                probeColumn = new DictionaryVector(column, &probeIndex[0], probeRowCount);
            }
            vectorBatch->SetVector(j, probeColumn);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromReuse(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
    {
        for (int32_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            column = column->Slice(0, column->GetSize());
            vectorBatch->SetVector(j, column);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromSlice(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
    {
        for (int32_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            auto probeColumn = column->Slice(probeIndex[0], probeRowCount);
            vectorBatch->SetVector(j, probeColumn);
        }
    }

    int32_t maxRowCount = 0;
    int32_t *probeOutputCols;
    int32_t probeOutputColsCount;
    int32_t *buildOutputCols;
    int32_t buildOutputColsCount;
    DataTypes buildOutputTypes;
    int32_t probeRowCount = 0;
    std::vector<int32_t> probeIndex;
    std::vector<std::pair<BaseVector ***, uint64_t>> buildIndex;
};

class LookupJoinOperatorFactory : public OperatorFactory {
public:
    LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables,
        OverflowConfig *overflowConfig);
    LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables,
        int32_t originalProbeColsCount, OverflowConfig *overflowConfig);
    ~LookupJoinOperatorFactory() override;
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes, JoinType joinType,
        int64_t hashBuilderFactoryAddr, OverflowConfig *overflowConfig);
    // this is only for LookupJoinWithExprOperator
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
        JoinType inputJoinType, int64_t hashBuilderFactoryAddr, int32_t originalProbeColsCount,
        OverflowConfig *overflowConfig);
    Operator *CreateOperator() override;

private:
    void CommonInitActions(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes);

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

class LookupJoinOperator : public Operator {
public:
    LookupJoinOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, const type::DataTypes &buildOutputTypes, JoinType joinType,
        JoinHashTables *hashTables, int32_t outputRowSize);
    ~LookupJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

private:
    template <bool hasJoinFilter> void ProbeBatchForInnerJoin();
    template <bool hasJoinFilter> void ProbeBatchForLeftJoin();
    template <bool hasJoinFilter> void ProbeBatchForRightJoin();
    template <bool hasJoinFilter> void ProbeBatchForFullJoin();
    template <bool hasJoinFilter> void ProbeBatchForLeftSemiJoin();
    template <bool hasJoinFilter> void ProbeBatchForLeftAntiJoin();
    bool IsJoinPositionEligible(uint32_t partition, uint32_t joinPosition, uint32_t probeRow);
    void PrepareCurrentProbe();
    void PopulateProbeHashes();
    void ProcessProbe(bool hasFilter);

    JoinType joinType;
    DataTypes probeTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> probeHashCols;
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;
    DataTypes buildOutputTypes;
    JoinHashTables *hashTables;

    omniruntime::vec::BaseVector **probeHashColumns = nullptr; // Vector *[join column count]
    omniruntime::vec::BaseVector **probeOutputColumns = nullptr;
    std::vector<int64_t> curProbeHashes;
    std::vector<int8_t> curProbeNulls;

    std::unique_ptr<LookupJoinOutputBuilder> outputBuilder;
    ExecutionContext *executionContext = nullptr;
    omniruntime::vec::VectorBatch *curInputBatch = nullptr;
    int32_t curProbePosition = 0;
    omniruntime::vec::VectorBatch *curOutputBatch = nullptr;

    // this is for join filter
    SimpleFilter *simpleFilter = nullptr;
    omniruntime::vec::BaseVector **probeFilterColumns = nullptr;
    int64_t *values = nullptr;
    bool *nulls = nullptr;
    int32_t *lengths = nullptr;
    std::vector<int32_t> probeFilterCols;
    std::vector<int32_t> buildFilterCols;
    bool firstVecBatch = false;
};
} // end of op
} // end of omniruntime
#endif
