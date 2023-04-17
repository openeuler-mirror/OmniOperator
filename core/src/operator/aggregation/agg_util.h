/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: Hash Aggregation WithExpr Header
 */


#ifndef OMNI_RUNTIME_AGG_UTIL_H
#define OMNI_RUNTIME_AGG_UTIL_H

#include "vector/vector_common.h"
#include "operator/execution_context.h"
#include "operator/filter/filter_and_project.h"
#include "operator/util/operator_util.h"

namespace omniruntime {
namespace op {
class AggUtil {
public:
    static bool IsAggPositionEligible(int32_t rowId, VectorBatch *inputVecBatch, SimpleFilter *aggSimpleFilters,
        ExecutionContext *executionContext)
    {
        if (!aggSimpleFilters) {
            return true;
        }
        const int32_t allColsCount = inputVecBatch->GetVectorCount();
        int64_t values[allColsCount];
        bool nulls[allColsCount];
        int32_t lengths[allColsCount];
        std::set<int32_t> usedVectors = aggSimpleFilters->GetVectorIndexes();
        for (auto iter = usedVectors.begin(); iter != usedVectors.end(); ++iter) {
            auto vecIdx = *iter;
            auto vector = inputVecBatch->GetVector(vecIdx);
            nulls[vecIdx] = vector->IsValueNull(rowId);
            values[vecIdx] = VectorHelper::GetValuePtrAndLength(vector, rowId, lengths + vecIdx);
        }

        return aggSimpleFilters->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(&executionContext));
    }

    static VectorBatch *AggFilterRequiredVectors(VectorBatch *inputVecBatch, const DataTypes &inputTypes,
        const std::vector<ProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols, int32_t aggFilterNum,
        VectorAllocator *allocator)
    {
        auto projectColsCount = static_cast<int32_t>(projectCols.size());
        int32_t vecCount = projectColsCount + aggFilterNum;
        int32_t rowCount = inputVecBatch->GetRowCount();
        VectorBatch *newInputVecBatch = new VectorBatch(vecCount, rowCount);

        for (int32_t i = 0; i < projectColsCount; i++) {
            int32_t sourceColId = projectCols[i];
            if (sourceColId >= 0) {
                // source col
                Vector *inputVector = inputVecBatch->GetVector(sourceColId);
                Vector *newInputVec = inputVector->Slice(0, rowCount);
                newInputVecBatch->SetVector(i, newInputVec);
            }
        }

        auto projectFuncsCount = projectFuncs.size();
        if (projectFuncsCount <= 0) {
            return newInputVecBatch;
        }

        int32_t originVecCount = inputVecBatch->GetVectorCount();
        int64_t valueAddresses[originVecCount];
        int64_t valueNulls[originVecCount];
        int64_t valueOffsets[originVecCount];
        int64_t dictVectorAddrs[originVecCount];
        for (int32_t i = 0; i < originVecCount; i++) {
            Vector *inputVector = inputVecBatch->GetVector(i);
            if (inputVector->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
                valueAddresses[i] = VectorHelper::GetValuesAddr(inputVector);
                dictVectorAddrs[i] = 0;
            } else {
                valueAddresses[i] = 0;
                dictVectorAddrs[i] = reinterpret_cast<int64_t>(reinterpret_cast<void *>(inputVector));
            }
            valueNulls[i] = VectorHelper::GetNullsAddr(inputVector);
            valueOffsets[i] = VectorHelper::GetOffsetsAddr(inputVector);
        }

        op::OperatorUtil::ProjectRequiredVectors(inputTypes, projectFuncs, projectCols, valueAddresses, valueNulls,
            valueOffsets, dictVectorAddrs, rowCount, newInputVecBatch, allocator);
        return newInputVecBatch;
    }

    static void AddFilterColumn(VectorBatch *inputVecBatch, VectorBatch *newInputVecBatch,
        std::vector<int32_t> &projectCols, vec::VectorAllocator *vecAllocator,
        std::vector<SimpleFilter *> &aggSimpleFilters, ExecutionContext *context)
    {
        auto aggFilterNum = aggSimpleFilters.size();
        auto rowCount = inputVecBatch->GetRowCount();
        auto projectColsCount = static_cast<int32_t>(projectCols.size());
        for (size_t i = 0; i < aggFilterNum; ++i) {
            BooleanVector *booleanVector = new BooleanVector(vecAllocator, rowCount);
            for (int j = 0; j < rowCount; ++j) {
                if (AggUtil::IsAggPositionEligible(j, inputVecBatch, aggSimpleFilters[i], context)) {
                    booleanVector->SetValue(j, true);
                    continue;
                }
                booleanVector->SetValue(j, false);
            }
            newInputVecBatch->SetVector(projectColsCount + i, booleanVector);
        }
    }
};
}
}


#endif // OMNI_RUNTIME_AGG_UTIL_H
