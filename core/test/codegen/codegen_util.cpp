/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: CodegenUtil Class
 */

#include "gtest/gtest.h"

#include <string>
#include <vector>
#include "operator/filter/filter_and_project.h"

using namespace std;
using namespace omniruntime::vec;

namespace CodegenUtil {
void GetDataFromVecBatch(VectorBatch &vecBatch, int64_t valueAddrs[], int64_t nullAddrs[], int64_t offsetAddrs[],
    int64_t dictionaries[])
{
    int64_t valuesAddress;
    int64_t dictVecAddress;
    int32_t vectorCount = vecBatch.GetVectorCount();
    for (int32_t i = 0; i < vectorCount; i++) {
        Vector *colVec = vecBatch.GetVector(i);
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_LAZY) {
            colVec = static_cast<LazyVector *>(colVec)->GetLoadedVector();
        }
        dictVecAddress = 0;
        valuesAddress = 0;
        if (colVec->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            dictVecAddress = reinterpret_cast<int64_t>(reinterpret_cast<void *>(colVec));
        } else {
            valuesAddress = VectorHelper::GetValuesAddr(colVec);
        }

        dictionaries[i] = dictVecAddress;
        valueAddrs[i] = valuesAddress;

        nullAddrs[i] = VectorHelper::GetNullsAddr(colVec);

        offsetAddrs[i] = VectorHelper::GetOffsetsAddr(colVec);
    }
}

VectorBatch *FilterAndProject(std::unique_ptr<omniruntime::op::Filter> &filter,
    std::vector<std::unique_ptr<omniruntime::op::Projection>> &projections, int32_t numCols, VectorBatch *vecBatch,
    int32_t &numSelectedRows, VectorAllocator *vecAllocator)
{
    int32_t numRows = vecBatch->GetRowCount();
    int32_t selectedRows[numRows];
    int64_t dictionaries[numCols];
    int64_t valueAddrs[numCols];
    int64_t nullAddrs[numCols];
    int64_t offsetAddrs[numCols];
    GetDataFromVecBatch(*vecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaries);

    auto context = new omniruntime::op::ExecutionContext();
    if (filter.get() != nullptr) {
        numSelectedRows = filter->apply(valueAddrs, numRows, selectedRows, nullAddrs, offsetAddrs,
            reinterpret_cast<int64_t>(context), dictionaries);
    }

    auto ret = (projections.size() > 0) ? new VectorBatch(projections.size(), numSelectedRows) : nullptr;
    for (uint32_t i = 0; i < projections.size(); i++) {
        Vector *col =
            projections[i]->Project(vecAllocator, vecBatch, (filter.get() != nullptr) ? selectedRows : nullptr,
            numSelectedRows, valueAddrs, nullAddrs, offsetAddrs, context, dictionaries);
        ret->SetVector(i, col);
    }
    context->GetArena()->Reset();
    delete context;
    return ret;
}
}