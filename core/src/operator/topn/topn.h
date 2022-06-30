/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_H
#define OMNI_RUNTIME_TOPN_H

#include <queue>
#include <memory>
#include <vector>
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "vector/vector_common.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
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

private:
    const int32_t *sourceTypes;
    int32_t *sortCols = nullptr;
    int32_t *sortAscendings = nullptr;
    int32_t *sortNullFirsts = nullptr;
    int32_t sortColCount = 0;
    omniruntime::vec::VectorBatch *vectorBatch = nullptr;
};

bool operator < (const RowComparator &left, const RowComparator &right);

class TopNOperatorFactory : public OperatorFactory {
public:
    TopNOperatorFactory(const type::DataTypes &sourceTypes, int32_t n, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount);

    ~TopNOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    type::DataTypes sourceTypes;
    std::vector<int32_t> sortCols;
    int32_t n = 0;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount = 0;
};

class TopNOperator : public Operator {
public:
    TopNOperator(const type::DataTypes &sourceTypes, int32_t n, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, int32_t sortColCount);

    ~TopNOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vectorBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

private:
    type::DataTypes sourceTypes;
    int32_t sourceTypesCount = 0;
    std::vector<int32_t> sortCols;
    int32_t n = 0;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount = 0;
    std::priority_queue<RowComparator, std::vector<RowComparator>, std::less<std::vector<RowComparator>::value_type>>
        pq;
    std::vector<omniruntime::vec::VectorBatch *> singleRowVectorBatchList;

    vec::VectorBatch *CreateSingleRowVecBatch(omniruntime::vec::VectorBatch *vectorBatch, int32_t position) const;

    void HandleVarchar(int64_t positionCount, vec::VectorBatch *tmpVecBatch) const;

    void SetValueForVectorBatch(int32_t typeId, int64_t index, vec::Vector *pqVector, vec::Vector *tmpVector) const;

    void SetVarcharValueForVectorBatch(int64_t rowNum, vec::VarcharVector *pqVector,
        vec::VarcharVector *tmpVector) const;

    void UpdateSingleRowVectorBatch(vec::VectorBatch *vectorBatch, vec::VectorBatch *singleRowVecBatch,
        int32_t position) const;
};
}
}
#endif // OMNI_RUNTIME_TOPN_H
