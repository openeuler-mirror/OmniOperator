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
        ExecutionContext *executionContext, DataTypes &originTypes)
    {
        if (!aggSimpleFilters) {
            return true;
        }
        const int32_t allColsCount = inputVecBatch->GetVectorCount();
        int64_t values[allColsCount];
        bool nulls[allColsCount];
        int32_t lengths[allColsCount];
        std::set<int32_t> &usedVectors = aggSimpleFilters->GetVectorIndexes();
        for (auto iter = usedVectors.begin(); iter != usedVectors.end(); ++iter) {
            auto vecIdx = *iter;
            auto vector = inputVecBatch->Get(vecIdx);
            nulls[vecIdx] = vector->IsNull(rowId);
            values[vecIdx] = OperatorUtil::GetValuePtrAndLength(vector, rowId, lengths + vecIdx,
                originTypes.GetType(vecIdx)->GetId());
        }

        return aggSimpleFilters->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(&executionContext));
    }

    static VectorBatch *AggFilterRequiredVectors(VectorBatch *inputVecBatch, const DataTypes &originTypes,
        const DataTypes &inputTypes, const std::vector<ProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols)
    {
        int32_t vecCount = projectCols.size();
        int32_t rowCount = inputVecBatch->GetRowCount();
        auto newInputVecBatch = new VectorBatch(rowCount);
        // short-circuit logic for column projections
        // no need to go through codegen
        if (rowCount == 0) {
            VectorHelper::AppendVectors(newInputVecBatch, inputTypes, rowCount);
            return newInputVecBatch;
        }

        auto projectFuncsCount = projectFuncs.size();

        int32_t originVecCount = inputVecBatch->GetVectorCount();
        int64_t valueAddresses[originVecCount];
        int64_t valueNulls[originVecCount];
        int64_t valueOffsets[originVecCount];
        int64_t dictVectorAddrs[originVecCount];

        for (int32_t i = 0; i < originVecCount; i++) {
            auto inputVector = inputVecBatch->Get(i);
            if (inputVector->GetEncoding() != OMNI_DICTIONARY) {
                valueAddresses[i] =
                    DYNAMIC_TYPE_DISPATCH(OperatorUtil::GetRawAddr, originTypes.GetType(i)->GetId(), inputVector);
                dictVectorAddrs[i] = 0;
            } else {
                valueAddresses[i] = 0;
                dictVectorAddrs[i] = reinterpret_cast<int64_t>(reinterpret_cast<void *>(inputVector));
            }
            valueNulls[i] = reinterpret_cast<int64_t>(unsafe::UnsafeBaseVector::GetNulls(inputVector));
            valueOffsets[i] = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetOffsetsAddr(inputVector));
        }

        for (int32_t i = 0, projectFuncsIndex = 0; i < vecCount; i++) {
            int32_t sourceColId = projectCols[i];
            if (sourceColId >= 0) {
                // source col append project colmun
                auto inputVector = inputVecBatch->Get(sourceColId);
                BaseVector *newInputVec = VectorHelper::SliceVector(inputVector, 0, rowCount);
                newInputVecBatch->Append(newInputVec);
            } else if (sourceColId == -1 && projectFuncsCount > 0) {
                // append withexpr colmun
                newInputVecBatch->Append(DYNAMIC_TYPE_DISPATCH(OperatorUtil::ProjectVector,
                    inputTypes.GetType(i)->GetId(), projectFuncs[projectFuncsIndex++], valueAddresses, valueNulls,
                    valueOffsets, dictVectorAddrs, rowCount));
            }
        }
        return newInputVecBatch;
    }

    static void AddFilterColumn(VectorBatch *inputVecBatch, VectorBatch *newInputVecBatch,
        std::vector<int32_t> &projectCols, std::vector<SimpleFilter *> &aggSimpleFilters, ExecutionContext *context,
        DataTypes &originTypes)
    {
        auto aggFilterNum = aggSimpleFilters.size();
        auto rowCount = inputVecBatch->GetRowCount();

        for (size_t i = 0; i < aggFilterNum; ++i) {
            auto *booleanVector = new Vector<bool>(rowCount);
            for (int j = 0; j < rowCount; ++j) {
                if (AggUtil::IsAggPositionEligible(j, inputVecBatch, aggSimpleFilters[i], context, originTypes)) {
                    booleanVector->SetValue(j, true);
                    continue;
                }
                booleanVector->SetValue(j, false);
            }
            newInputVecBatch->Append(booleanVector);
        }
    }
};
}
}


#endif // OMNI_RUNTIME_AGG_UTIL_H
