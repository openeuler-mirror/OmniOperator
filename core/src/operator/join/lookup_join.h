/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * @Description: lookup join implementations
 */
#ifndef __LOOKUP_JOIN_H__
#define __LOOKUP_JOIN_H__

#include <memory>
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/filter/filter_and_project.h"
#include "type/data_types.h"
#include "type/data_type.h"
#include "hash_builder.h"
#include "common_join.h"

namespace omniruntime {
namespace op {
class LookupJoinOutputBuilder {
public:
    LookupJoinOutputBuilder(std::vector<int32_t> &probeOutputCols, const int32_t *probeOutputTypes,
        std::vector<int32_t> &buildOutputCols, const int32_t *buildOutputTypes, int32_t outputRowSize);
    ~LookupJoinOutputBuilder() = default;
    void AppendRow(int32_t probePosition, BaseVector ***array, uint64_t address);
    void BuildOutput(BaseVector **probeOutputColumns, JoinType joinType, VectorBatch **outputVecBatch);
    void ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeAllColumns, int32_t rowCount);
    void ConstructBuildColumns(VectorBatch *vectorBatch, int32_t rowCount);
    template<bool isMatched> void AppendExistenceRow(int32_t probePosition);
    void ConstructExistenceColumn(VectorBatch *vectorBatch);

    ALWAYS_INLINE bool IsFull()
    {
        return probeRowCount >= maxRowCount;
    }

    ALWAYS_INLINE bool IsEmpty()
    {
        return probeRowCount <= 0;
    }

    ALWAYS_INLINE void Clear()
    {
        probeRowOffset = 0;
        probeRowCount = 0;
        probeIndex.clear();
        buildIndex.clear();
        existJoinBuildIndex.clear();
    }

    static const uint32_t SHIFT_SIZE_32 = 32;
    static ALWAYS_INLINE uint64_t EncodeAddress(uint32_t rowId, uint32_t vectorBatchId)
    {
        return (static_cast<uint64_t>(rowId) << SHIFT_SIZE_32) | vectorBatchId;
    }

    static ALWAYS_INLINE uint32_t DecodeRowId(uint64_t sliceAddress)
    {
        return static_cast<uint32_t>(sliceAddress >> SHIFT_SIZE_32);
    }

    static ALWAYS_INLINE uint32_t DecodeVectorBatchId(uint64_t sliceAddress)
    {
        return static_cast<uint32_t>(sliceAddress);
    }

private:
    ALWAYS_INLINE void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, BaseVector **probeOutputColumns,
        int32_t rowCount)
    {
        auto probeOutputColsCount = probeOutputCols.size();
        auto probePositions = probeIndex.data() + probeRowOffset;
        for (size_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            auto type = probeOutputTypes[j];
            // we want to keep only one level dictionary vector here
            // if the data is non-dictionary, we build dictionary to avoid data copy
            BaseVector *probeColumn = nullptr;
            if (column->GetEncoding() == vec::OMNI_DICTIONARY) {
                probeColumn = VectorHelper::CopyPositionsVector(column, probePositions, 0, rowCount);
            } else {
                probeColumn = VectorHelper::CreateDictionaryVector(probePositions, rowCount, column, type);
            }
            vectorBatch->Append(probeColumn);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromReuse(VectorBatch *vectorBatch, BaseVector **probeOutputColumns,
        int32_t rowCount)
    {
        auto probeOutputColsCount = probeOutputCols.size();
        for (size_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            auto resultColumn = VectorHelper::SliceVector(column, 0, rowCount);
            vectorBatch->Append(resultColumn);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromSlice(VectorBatch *vectorBatch, BaseVector **probeOutputColumns,
        int32_t rowCount)
    {
        auto probeOutputColsCount = probeOutputCols.size();
        auto offset = probeIndex[probeRowOffset];
        for (size_t j = 0; j < probeOutputColsCount; ++j) {
            auto column = probeOutputColumns[j];
            auto resultColumn = VectorHelper::SliceVector(column, offset, rowCount);
            vectorBatch->Append(resultColumn);
        }
    }

    int32_t maxRowCount = 0;
    std::vector<int32_t> probeOutputCols;
    const int32_t *probeOutputTypes;
    std::vector<int32_t> buildOutputCols;
    const int32_t *buildOutputTypes;
    int32_t probeRowCount = 0;
    int32_t probeRowOffset = 0;
    std::vector<int32_t> probeIndex;
    std::vector<std::pair<BaseVector ***, uint64_t>> buildIndex;
    std::vector<bool> existJoinBuildIndex;
};

class LookupJoinOperatorFactory : public OperatorFactory {
public:
    LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
        omniruntime::expressions::Expr *filterExpr, OverflowConfig *overflowConfig);
    LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
        omniruntime::expressions::Expr *filterExpr, int32_t originalProbeColsCount, OverflowConfig *overflowConfig);
    ~LookupJoinOperatorFactory() override;
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
        int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr, OverflowConfig *overflowConfig);
    // this is only for LookupJoinWithExprOperator
    static LookupJoinOperatorFactory *CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
        int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
        int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr, int32_t originalProbeColsCount,
        OverflowConfig *overflowConfig);
    Operator *CreateOperator() override;

private:
    void CommonInitActions(const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols, int32_t buildOutputColsCount,
        const type::DataTypes &buildOutputTypes);
    void JoinFilterCodeGen(Expr *filterExpr, OverflowConfig *overflowConfig);

    DataTypes probeTypes;                 // all types for probe
    std::vector<int32_t> probeOutputCols; // output columns for probe
    std::vector<int32_t> probeHashCols;   // join columns for probe
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols; // output columns for build
    DataTypes buildOutputTypes;           // output column types for build
    HashTableVariants *hashTables;
    int32_t rowSize; // estimation of rowSize
    SimpleFilter *simpleFilter = nullptr;
    // this is for lookup join with expression operator when join key and join filter both are expressions
    int32_t originalProbeColsCount;
};

class LookupJoinOperator : public Operator {
public:
    LookupJoinOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
        std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes,
        std::vector<int32_t> &buildOutputCols, const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
        SimpleFilter *simpleFilter, int32_t originalProbeColsCount, int32_t outputRowSize);
    ~LookupJoinOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;
    OmniStatus Close() override;

private:
    void InitFirst();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForInnerJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForOppositeSideOuterJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForSameSideOuterJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForFullJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForLeftSemiJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForLeftAntiJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForExistenceJoin();
    template <bool hasJoinFilter> void ProbeJoinPosition(int32_t probePosition);
    bool BuildJoinPosition(uint32_t partition, uint32_t buildRowIdx,
                           uint32_t buildBatchIdx, ExecutionContext *contextPtr);
    void PrepareCurrentProbe();
    void PrepareSerializers();
    void PopulateProbeHashes();
    void PopulateProbeNulls();
    void ProcessProbe();

    template <bool hasJoinFilter>
    ALWAYS_INLINE void InitForProbe(uint32_t partition, bool initSize=true)
    {
        if constexpr (hasJoinFilter) {
            if (initSize) {
                probeFilterColsSize = probeFilterCols.size();
                buildFilterColsSize = buildFilterCols.size();
            }
            buildFilterColPtrs = GetBuildFilterColPtrs(partition);
        }
    }

    ALWAYS_INLINE void AppendRow(int32_t probePosition, BaseVector ***array,
                                 uint64_t address, bool appendRow, bool& hasProduceRow)
    {
        if (appendRow) {
            outputBuilder->AppendRow(probePosition, array, address);
            hasProduceRow = true;
        }
    }

    ALWAYS_INLINE std::vector<BaseVector **> *GetBuildFilterColPtrs(uint32_t partition)
    {
        return &tableBuildFilterColPtrs[partition];
    }

    void PushBackProbeSerializer(VectorSerializerIgnoreNull &serializer)
    {
        probeSerializers.push_back(serializer);
    }

    DataTypes probeTypes;
    std::vector<int32_t> probeOutputCols;
    std::vector<int32_t> probeHashCols;
    std::vector<int32_t> probeHashColTypes;
    std::vector<int32_t> buildOutputCols;
    DataTypes probeOutputTypes;
    DataTypes buildOutputTypes;
    HashTableVariants *hashTables;

    omniruntime::vec::BaseVector **probeHashColumns = nullptr; // Vector *[join column count]
    omniruntime::vec::BaseVector **probeOutputColumns = nullptr;
    std::vector<int64_t> curProbeHashes;
    std::vector<int8_t> curProbeNulls;

    std::unique_ptr<LookupJoinOutputBuilder> outputBuilder;
    omniruntime::vec::VectorBatch *curInputBatch = nullptr;
    int32_t curProbePosition = 0;
    JoinType joinType;
    BuildSide buildSide;
    uint32_t partitionMask = 0;
    bool isSingleHT;

    // this is for join filter
    SimpleFilter *simpleFilter = nullptr;
    int32_t originalProbeColsCount;
    std::vector<int32_t> probeFilterCols;
    std::vector<int32_t> buildFilterCols;
    int32_t *probeFilterTypeIds = nullptr;
    int32_t *buildFilterTypeIds = nullptr;
    omniruntime::vec::BaseVector **probeFilterColumns = nullptr;
    int64_t *values = nullptr;
    bool *nulls = nullptr;
    int32_t *lengths = nullptr;
    std::vector<std::vector<BaseVector **>> tableBuildFilterColPtrs;
    bool firstVecBatch = true;
    std::vector<VectorSerializerIgnoreNull> probeSerializers;
    std::vector<BaseVector **> *buildFilterColPtrs = nullptr;
    size_t probeFilterColsSize = 0;
    size_t buildFilterColsSize = 0;
};
} // end of op
} // end of omniruntime
#endif
