/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "definitions.h"
#include "aggregation.h"
#include "type/data_types.h"
#include "operator/hash_util.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregator/only_aggregator_factory.h"
#include "operator/util/operator_util.h"
#include "operator/hashmap/column_marshaller.h"
#include "operator/hashmap/array_map.h"
#include "operator/hashmap/vector_analyzer.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/config/operator_config.h"
#include "operator/filter/filter_and_project.h"
#include "operator/pages_index.h"
#include "operator/spill/spiller.h"
#include "group_aggregation_sort.h"

namespace omniruntime::op {
using namespace vec;

class HashAggregationOperatorFactory;

class HashAggregationOperator;

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> &groupByCols, std::vector<std::vector<int32_t>> &aggInputCols,
        uint32_t aggInputColsSize, std::vector<DataTypes> &aggInputTypes, std::vector<DataTypes> &aggOutputTypes,
        std::vector<std::unique_ptr<Aggregator>> &&aggs, std::vector<bool> &inputRaws,
        std::vector<bool> &outputPartials, const std::vector<int8_t> &hasAggFilters,
        const OperatorConfig &operatorConfig)
        : AggregationCommonOperator(std::move(aggs), inputRaws, outputPartials),
          groupByCols(groupByCols),
          aggInputCols(aggInputCols),
          aggInputColsSize(aggInputColsSize),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          hasAggFilters(hasAggFilters),
          operatorConfig(operatorConfig)
    {
        for (auto hasFilter: hasAggFilters) {
            if (hasFilter == 1) {
                aggFiltersCount++;
            }
        }
    }

    ~HashAggregationOperator() override = default;

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(VectorBatch **outputVecBatch) override;

    OmniStatus Init() override;

    OmniStatus Close() override;

    template<typename Serialize>
    void Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, BaseVector **groupVectors, int32_t groupColNum);

    void EmplaceToArrayMap(VectorBatch *vecBatch, BaseVector *groupVector);

    template<typename T, typename GroupMap>
    void InsertValueToArrayMap(GroupMap &arrayMap, BaseVector *groupVector,
                               int32_t rowIdx);

    uint64_t GetSpilledBytes() override;

    uint64_t GetUsedMemBytes() override;

    uint64_t GetTotalMemBytes() override;

    std::vector<uint64_t> GetSpecialMetricsInfo() override;

    uint64_t GetHashMapUniqueKeys() override;

    VectorBatch *AlignSchema(VectorBatch *inputVecBatch) override;

    void SetRowCountPerBatch(int32_t rowCount)
    {
        this->rowsPerBatch = rowCount;
    }

private:
    int32_t InitMaxRowCountAndOutputTypes();

    void InitSpillInfos();

    ErrorCode SpillHashMap();

    ErrorCode SpillToDisk();

    void SetVectors(VectorBatch *output, const std::vector<DataTypePtr> &types, int32_t rowCount);

    template<typename Deserialize>
    int32_t Output(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch);

    void SetGroupByColumnsHandleType(HandleType t);

    friend class HashAggregationOperatorFactory;

    template<typename V, typename D>
    friend void FillValueImpl(BaseVector *vector, int32_t rowIndex, const AggregateState &state);

    friend void FillVarcharValue(BaseVector *vector, int32_t rowIndex, const AggregateState &state);

    void InitState(int64_t aggStateAddress);

    void ProcessStates(VectorBatch *vecBatch);

    template<typename T>
    inline void InsertAggStatesToArrayMap(T *hashes, int32_t vecLanes, bool *isAssigned,
                                    int64_t *slots, mem::SimpleArenaAllocator &arenaAllocator,
                                   int32_t probePosition);

    template<typename T, bool hasNull>
    void ArrayGroupProbeSIMD(BaseVector *groupVector, VectorBatch *vecBatch);

    void ResizeArrayMap(int64_t oldMin);

    void MoveEntryArrayTableToHashMap(int64_t minValue);

    template<bool hasAgg, typename T>
    void TraverseArrayMapGetOutput(BaseVector *groupVector,
                                   std::vector<AggregateState *> *states, int64_t minValue);

    template<bool hasAgg>
    void TraverseArrayMap(BaseVector *groupVector, std::vector<AggregateState *> *states);

    std::unique_ptr<GroupbySingleFixHandler<DefaultHashMap<int16_t, AggregateState *>, int16_t>> fixedInt16 = nullptr;

    void TraverseArrayMapToGetOneResult(VectorBatch *output);

    void GetOutputFromDisk(VectorBatch **outputVecBatch);

    VectorBatch *GetOutputFromDiskWithoutAgg(VectorBatch *output);

    VectorBatch *GetOutputFromDiskWithAgg(VectorBatch *output);

    void SetStateOutputVecBatch(VectorBatch *outputVecBatch, int32_t rowCount, int32_t groupColNum, int32_t aggNum);

    void CalcAndSetStatesSize();

    ALWAYS_INLINE size_t GetElementsSize();

    ALWAYS_INLINE void ResetHashmap();

    std::vector<ColumnIndex> groupByCols;
    std::vector<std::vector<int32_t>> aggInputCols;
    uint32_t aggInputColsSize;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::vector<type::DataTypePtr> outputTypes;
    HandleType groupByColumnsHandleType = HandleType::serialize;
    std::unique_ptr<ColumnSerializeHandler<DefaultHashMap<StringRef, AggregateState *>>> serialize = nullptr;
    std::unique_ptr<DefaultArrayMap<AggregateState>> arrayTable = nullptr;

    std::unique_ptr<GroupbySingleFixHandler<DefaultHashMap<int32_t, AggregateState *>, int32_t>> fixedInt32 = nullptr;
    std::unique_ptr<GroupbySingleFixHandler<DefaultHashMap<int64_t, AggregateState *>, int64_t>> fixedInt64 = nullptr;
    bool isInited = false;

    OutputState outputState;

    template<typename Deserialize>
    void TraverseHashmapToGetOneResult(Deserialize &deserializeHashmap, VectorBatch *output);

    int32_t rowsPerBatch;
    std::vector<int8_t> hasAggFilters;
    int32_t aggFiltersCount = 0;
    int64_t memoryChunkSize = 0;

    // for spill
    OperatorConfig operatorConfig;
    SpillMerger *spillMerger = nullptr;
    Spiller *spiller = nullptr;
    std::vector<SortOrder> sortOrders;
    std::vector<int32_t> groupByCloIdx;
    uint64_t spillTotalRowCount = 0;
    uint64_t spillRowOffset = 0;
    std::unique_ptr<AggregateState[]> groupStates = nullptr;
    std::vector<AggregateState *> rowStates;
    uint64_t spilledBytes = 0;
    uint64_t usedMemBytes = 0;
    uint64_t totalMemBytes = 0;
    std::vector<type::DataTypePtr> aggTypes;
    std::vector<type::DataTypePtr> spillTypes;
    OutputState spillOutputState;
    bool hasSpill = false;
    std::unique_ptr<AggregationSort> aggregationSort = nullptr;
    int32_t totalAggStatesSize = 0;
    VectorAnalyzer *vectorAnalyzer = nullptr;
    std::vector<AggregateState *> rowsAggStates;
    int8_t resizeArrayMapCnt = 0;
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    /*
     * @param groupByCol      the col index which is used as group column in VectorBatch
     * @param groupInputTypes all the group types
     * @param aggsCols        aggsCols contains all aggregators 's all agg col index
     * @param aggInputTypes   input types of all aggregators
     * @param aggOutputTypes  output types of all aggregators
     * @param aggFuncTypes    func types of aggregators
     * @param maskColsVector  mask col index in VectorBatch
     * @param inputRaws       whether the input VectorBatch is raw, the input raw is true in the first stage
     * @param outputPartials  whether the output VectorBatch is partial result
     * @param operatorConfig  the operator config
     */
    HashAggregationOperatorFactory(std::vector<uint32_t> &groupByCol, const DataTypes &groupInputTypes,
        std::vector<std::vector<uint32_t>> &aggsCols, std::vector<DataTypes> &aggInputTypes,
        std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes,
        std::vector<uint32_t> &maskColsVector, std::vector<bool> inputRaws, std::vector<bool> outputPartials,
        const OperatorConfig &operatorConfig)
        : AggregationCommonOperatorFactory(inputRaws, outputPartials, maskColsVector,
        operatorConfig.GetOverflowConfig()->IsOverflowAsNull(), operatorConfig.IsStatisticalAggregate()),
          groupByColsVector(groupByCol),
          groupByTypes(groupInputTypes),
          aggsInputColsVector(aggsCols),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypesVector(aggFuncTypes),
          operatorConfig(operatorConfig)
    {}

    HashAggregationOperatorFactory(std::vector<uint32_t> &groupByCol, const DataTypes &groupInputTypes,
        std::vector<std::vector<uint32_t>> &aggsCols, std::vector<DataTypes> &aggInputTypes,
        std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes,
        std::vector<uint32_t> &maskColsVector, std::vector<bool> inputRaws, std::vector<bool> outputPartials,
        bool overflowAsNull = false, bool isStatisticalAggregate = false)
        : AggregationCommonOperatorFactory(inputRaws, outputPartials, maskColsVector, overflowAsNull),
          groupByColsVector(groupByCol),
          groupByTypes(groupInputTypes),
          aggsInputColsVector(aggsCols),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypesVector(aggFuncTypes)
    {}

    /*
     * @param groupByCol      the col index which is used as group column in VectorBatch
     * @param groupInputTypes all the group types
     * @param aggsCols        aggsCols contains all aggregators 's all agg col index
     * @param aggInputTypes   input types of all aggregators
     * @param aggOutputTypes  output types of all aggregators
     * @param aggFuncTypes    func types of aggregators
     * @param maskColsVector  mask col index in VectorBatch
     * @param inputRaws       whether the input VectorBatch is raw, the input raw is true in the first stage
     * @param outputPartials  whether the output VectorBatch is partial result
     * @param hasAggFilters   whether handle the agg filter when AddInput
     * @param operatorConfig  the operator config
     * this is for HashAggregationWithExprOperatorFactory
     */
    HashAggregationOperatorFactory(std::vector<uint32_t> &groupByCol, const DataTypes &groupInputTypes,
        std::vector<std::vector<uint32_t>> &aggsCols, std::vector<DataTypes> &aggInputTypes,
        std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes,
        std::vector<uint32_t> &maskColsVector, std::vector<bool> inputRaws, std::vector<bool> outputPartials,
        const std::vector<int8_t> &hasAggFilters, const OperatorConfig &operatorConfig)
        : AggregationCommonOperatorFactory(inputRaws, outputPartials, maskColsVector,
        operatorConfig.GetOverflowConfig()->IsOverflowAsNull(), operatorConfig.IsStatisticalAggregate()),
          groupByColsVector(groupByCol),
          groupByTypes(groupInputTypes),
          aggsInputColsVector(aggsCols),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypesVector(aggFuncTypes),
          hasAggFilters(hasAggFilters),
          operatorConfig(operatorConfig)
    {}

    ~HashAggregationOperatorFactory() override = default;

    Operator *CreateOperator() override;

    OmniStatus Init() override;

    OmniStatus Close() override;

private:
    std::vector<uint32_t> groupByColsVector;
    std::vector<int32_t> groupByColIndices;
    DataTypes groupByTypes;
    std::vector<std::vector<uint32_t>> aggsInputColsVector;
    std::vector<std::vector<int32_t>> aggsInputCols;
    std::vector<std::vector<uint32_t>> aggOutputColsVector;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypesVector;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
    HandleType handleType;
    std::vector<int8_t> hasAggFilters;
    OperatorConfig operatorConfig;
    void ChooseGroupByType();
};
} // end of namespace omniruntime::op
#endif