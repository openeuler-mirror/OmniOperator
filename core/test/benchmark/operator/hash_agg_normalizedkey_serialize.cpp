/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include "common/common.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "common/vector_util.h"

using namespace benchmark;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace om_benchmark {
class HashAggNormalizedKeySerialize : public BaseOperatorFixture {
protected:
    OperatorFactory *createOperatorFactory(const State &state) override
    {
        // Compare two hash modes on exactly the same input schema and data.
        std::vector<uint32_t> groupByCols{ 0, 1, 2 };
        DataTypes groupByTypes(std::vector<DataTypePtr>{
            IntType(),
            LongType(),
            ShortType()
        });
        auto aggCols = AggregatorUtil::WrapWithVector(std::vector<uint32_t>{ 3 });
        auto aggInputTypes = AggregatorUtil::WrapWithVector(DataTypes(std::vector<DataTypePtr>{ LongType() }));
        auto aggOutputTypes = AggregatorUtil::WrapWithVector(DataTypes(std::vector<DataTypePtr>{ LongType() }));
        std::vector<uint32_t> aggFuncTypes{ OMNI_AGGREGATION_TYPE_SUM };
        std::vector<uint32_t> maskCols{ static_cast<uint32_t>(-1) };
        std::vector<bool> inputsRaw{ true };
        std::vector<bool> outputsPartial{ false };

        auto *factory = new HashAggregationOperatorFactory(groupByCols, groupByTypes, aggCols, aggInputTypes,
            aggOutputTypes, aggFuncTypes, maskCols, inputsRaw, outputsPartial);
        factory->SetNormalizedKeyEnabledForFactory(HashMode(state) == "multiNormalize");
        factory->Init();
        return factory;
    }

    std::vector<VectorBatchSupplier> createVecBatch(const State &state) override
    {
        std::vector<VectorBatch *> batches(TotalPages(state));
        for (int32_t pageId = 0; pageId < TotalPages(state); ++pageId) {
            auto *vectorBatch = new VectorBatch(RowsPerPage(state));
            vectorBatch->Append(createGroupKeyIntVector(state, pageId));
            vectorBatch->Append(createGroupKeyLongVector(state, pageId));
            vectorBatch->Append(createGroupKeyShortVector(state, pageId));
            vectorBatch->Append(createAggInputVector(state, pageId));
            batches[pageId] = vectorBatch;
        }
        return VectorBatchToVectorBatchSupplier(batches);
    }

private:
    OMNI_BENCHMARK_PARAM(std::string, HashMode, "multiNormalize", "columnSerialize");
    OMNI_BENCHMARK_PARAM(int32_t, TotalPages, 400);
    OMNI_BENCHMARK_PARAM(int32_t, RowsPerPage, 65536);
    OMNI_BENCHMARK_PARAM(int32_t, NumGroups, 256*16, 1024*16, 4096*16);

    ALWAYS_INLINE int64_t BuildGroupId(const State &state, int32_t pageId, int32_t rowId) const
    {
        return static_cast<int64_t>((static_cast<int64_t>(pageId) * RowsPerPage(state) + rowId) % NumGroups(state));
    }

    BaseVector *createGroupKeyIntVector(const State &state, int32_t pageId)
    {
        auto *group = new Vector<int32_t>(RowsPerPage(state));
        for (int32_t r = 0; r < RowsPerPage(state); ++r) {
            auto gid = BuildGroupId(state, pageId, r);
            group->SetValue(r, static_cast<int32_t>(gid * 3 + 13));
        }
        return group;
    }

    BaseVector *createGroupKeyLongVector(const State &state, int32_t pageId)
    {
        auto *group = new Vector<int64_t>(RowsPerPage(state));
        for (int32_t r = 0; r < RowsPerPage(state); ++r) {
            auto gid = BuildGroupId(state, pageId, r);
            group->SetValue(r, gid * 5 + 97);
        }
        return group;
    }

    BaseVector *createGroupKeyShortVector(const State &state, int32_t pageId)
    {
        auto *group = new Vector<int16_t>(RowsPerPage(state));
        for (int32_t r = 0; r < RowsPerPage(state); ++r) {
            auto gid = BuildGroupId(state, pageId, r);
            group->SetValue(r, static_cast<int16_t>(gid % 32767));
        }
        return group;
    }

    BaseVector *createAggInputVector(const State &state, int32_t pageId)
    {
        auto *agg = new Vector<int64_t>(RowsPerPage(state));
        for (int32_t r = 0; r < RowsPerPage(state); ++r) {
            agg->SetValue(r, static_cast<int64_t>((pageId + r) & 1023));
        }
        return agg;
    }
};

OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(HashAggNormalizedKeySerialize);
}
