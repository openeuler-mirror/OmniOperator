/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_H
#define OMNI_RUNTIME_TOPN_H

#include <queue>
#include <memory>
#include <vector>
#include "../operator.h"
#include "../operator_factory.h"
#include "../../vector/vector_common.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

class RowComparator {
public:
    RowComparator(const int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortColCount, omniruntime::vec::VectorBatch *vectorBatch);

    ~RowComparator();

    const int32_t *GetSourceTypes() const;

    int32_t *GetSortAscendings() const;

    int32_t *GetSortNullFirsts() const;

    int32_t GetSortColCount() const;

    int32_t *GetSortCols() const;

    omniruntime::vec::VectorBatch *GetVecBatch() const;

    void Update(int32_t i);

private:
    const int32_t *sourceTypes;
    int32_t *sortCols = nullptr;
    int32_t *sortAscendings = nullptr;
    int32_t *sortNullFirsts = nullptr;
    int32_t sortColCount = 0;
    omniruntime::vec::VectorBatch *vectorBatch = nullptr;
};

bool operator < (const RowComparator &left, const RowComparator &right);

int CompareVectorBatch(int32_t leftPosition, vec::VectorBatch *left, int32_t rightPosition, vec::VectorBatch *right,
    int32_t sortColCount, const int32_t *sortCols, const int32_t *sourceTypeIds, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts);

class TopNOperatorFactory : public OperatorFactory {
public:
    TopNOperatorFactory(const vec::VecTypes &sourceTypes, int32_t n, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount);

    ~TopNOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    const vec::VecTypes sourceTypes;
    std::vector<int32_t> sortCols;
    int32_t n = 0;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount = 0;
};

class TopNOperator : public Operator {
public:
    TopNOperator(const vec::VecTypes &sourceTypes, int32_t n, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, int32_t sortColCount);

    ~TopNOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

private:
    const vec::VecTypes &sourceTypes;
    int32_t sourceTypesCount = 0;
    std::vector<int32_t> sortCols;
    int32_t n = 0;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount = 0;
    std::priority_queue<RowComparator, std::vector<RowComparator>, std::less<std::vector<RowComparator>::value_type>>
        pq;
    std::vector<omniruntime::vec::VectorBatch *> singleRowVectorBatchList;
    std::vector<RowComparator *> rowComparatorList;

    vec::VectorBatch *CreateSingleRowVecBatch(omniruntime::vec::VectorBatch *vectorBatch, int32_t position) const;

    void HandleVarchar(int64_t positionCount, vec::VectorBatch *tmpVecBatch) const;

    void SetValueForVectorBatch(int32_t typeId, int64_t index, vec::Vector *pqVector, vec::Vector *tmpVector) const;

    void SetVarcharValueForVectorBatch(int64_t rowNum, vec::VarcharVector *pqVector,
        vec::VarcharVector *tmpVector) const;

    void UpdateSingleRowVectorBatch(vec::VectorBatch *vectorBatch, vec::VectorBatch *singleRowVecBatch,
        int32_t position) const;
};

template <typename T>
void ALWAYS_INLINE SetVectorForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex, Vector *vector,
    int32_t position)
{
    singleRowVecBatch->SetVector(colIndex, (static_cast<T *>(vector))->CopyRegion(position, 1));
}

template <typename T>
static void ALWAYS_INLINE SetValueForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex, Vector *vector,
    int32_t position)
{
    static_cast<T *>(singleRowVecBatch->GetVector(colIndex))
        ->SetValueNull(0, (static_cast<T *>(vector))->IsValueNull(position));
    static_cast<T *>(singleRowVecBatch->GetVector(colIndex))
        ->SetValue(0, (static_cast<T *>(vector))->GetValue(position));
}

static void ALWAYS_INLINE SetVarCharForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex,
    Vector *vector, int32_t position)
{
    VarcharVector *single = static_cast<VarcharVector *>(singleRowVecBatch->GetVector(colIndex));
    // we just need to set value null
    if (static_cast<VarcharVector *>(vector)->IsValueNull(position)) {
        single->SetValueNull(0, true);
        return;
    }
    // we need to delete then re-allocate;
    delete static_cast<VarcharVector *>(singleRowVecBatch->GetVector(colIndex));
    singleRowVecBatch->SetVector(colIndex, (static_cast<VarcharVector *>(vector))->CopyRegion(position, 1));
}
}
}
#endif // OMNI_RUNTIME_TOPN_H
