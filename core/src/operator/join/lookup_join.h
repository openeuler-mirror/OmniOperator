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
    void BuildOutput(BaseVector **probeOutputColumns, VectorBatch **outputVecBatch);
    void ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeAllColumns);
    void ConstructBuildColumns(VectorBatch *vectorBatch);

    ALWAYS_INLINE bool IsFull()
    {
        return probeRowCount >= maxRowCount;
    }

    ALWAYS_INLINE bool IsEmpty()
    {
        return probeRowCount == 0;
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
    ALWAYS_INLINE void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
    {
        BaseVector *probeColumn = nullptr;
        auto probePositions = probeIndex.data();
        for (size_t j = 0; j < probeOutputCols.size(); ++j) {
            auto column = probeOutputColumns[j];
            auto type = probeOutputTypes[j];
            // we want to keep only one level dictionary vector here
            // if the data is non-dictionary, we build dictionary to avoid data copy
            if (column->GetEncoding() == vec::OMNI_DICTIONARY) {
                probeColumn = VectorHelper::CopyPositionsVector(column, probePositions, 0, probeRowCount);
            } else {
                probeColumn = VectorHelper::CreateDictionaryVector(probePositions, probeRowCount, column, type);
            }
            vectorBatch->Append(probeColumn);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromReuse(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
    {
        for (size_t j = 0; j < probeOutputCols.size(); ++j) {
            auto column = probeOutputColumns[j];
            auto resultColumn = VectorHelper::SliceVector(column, 0, column->GetSize());
            vectorBatch->Append(resultColumn);
        }
    }

    ALWAYS_INLINE void ConstructProbeColumnsFromSlice(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
    {
        for (size_t j = 0; j < probeOutputCols.size(); ++j) {
            auto column = probeOutputColumns[j];
            auto resultColumn = VectorHelper::SliceVector(column, probeIndex[0], probeRowCount);
            vectorBatch->Append(resultColumn);
        }
    }

    int32_t maxRowCount = 0;
    std::vector<int32_t> probeOutputCols;
    const int32_t *probeOutputTypes;
    std::vector<int32_t> buildOutputCols;
    const int32_t *buildOutputTypes;
    int32_t probeRowCount = 0;
    std::vector<int32_t> probeIndex;
    std::vector<std::pair<BaseVector ***, uint64_t>> buildIndex;
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
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForInnerJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForLeftJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForRightJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForFullJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForLeftSemiJoin();
    template <bool hasJoinFilter, bool singleHT> void ProbeBatchForLeftAntiJoin();
    bool IsJoinPositionEligible(uint32_t partition, uint64_t address, uint32_t probeRow,
        ExecutionContext *executionContextPtr);
    void PrepareCurrentProbe();
    void PrepareSerializers();
    void PopulateProbeHashes();
    void PopulateProbeNulls();
    void ProcessProbe(bool hasFilter, bool singleHT);

    ALWAYS_INLINE std::vector<BaseVector **> &GetBuildFilterColPtrs(uint32_t partition)
    {
        return tableBuildFilterColPtrs[partition];
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
    omniruntime::vec::VectorBatch *curOutputBatch = nullptr;

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
    bool firstVecBatch = false;
    std::vector<VectorSerializerIgnoreNull> probeSerializers;
};
} // end of op
} // end of omniruntime
#endif
