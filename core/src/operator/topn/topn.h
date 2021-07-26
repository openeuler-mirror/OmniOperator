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

namespace omniruntime {
namespace op {
class RowComparator {
public:
    RowComparator(int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings, int32_t sortColCount,
                  omniruntime::vec::VectorBatch* vectorBatch);

    RowComparator() {};

    ~RowComparator();

    int32_t *GetSourceTypes() const;

    int32_t *GetSortAscendings() const;

    int32_t GetSortColCount() const;

    omniruntime::vec::VectorBatch* GetVecBatch() const;

private:
    int32_t *sourceTypes = nullptr;
    int32_t *sortCols = nullptr;
    int32_t *sortAscendings = nullptr;
    int32_t sortColCount = 0;
    omniruntime::vec::VectorBatch* vectorBatch = nullptr;
};

bool operator < (const RowComparator &left, const RowComparator &right);

class TopNOperatorFactory : public OperatorFactory {
public:
    TopNOperatorFactory(int32_t *sourceTypes, int32_t typesCount, int32_t n, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount);

    ~TopNOperatorFactory() override;


    Operator *CreateOperator() override;

private:
    int32_t *sourceTypes = nullptr;
    int32_t sourceTypesCount = 0;
    int32_t *sortCols = nullptr;
    int32_t n = 0;
    int32_t *sortAscendings = nullptr;
    int32_t *sortNullFirsts = nullptr;
    int32_t sortColCount = 0;
};

class TopNOperator : public Operator {
public:
    TopNOperator(int32_t *sourceTypes, int32_t typesCount, int32_t n, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount);

    ~TopNOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatch) override;

    int32_t Compare(int32_t position, omniruntime::vec::VectorBatch *table,
        omniruntime::vec::VectorBatch *currentMaxVectorBatch, int32_t sortColCount,
        const int32_t *sourceTypes, const int32_t *sortAscendings) const;

private:
    int32_t *sourceTypes = nullptr;
    int32_t sourceTypesCount = 0;
    int32_t *sortCols = nullptr;
    int32_t n = 0;
    int32_t *sortAscendings = nullptr;
    int32_t *sortNullFirsts = nullptr;
    int32_t sortColCount = 0;
    std::priority_queue<RowComparator, std::vector<RowComparator>,
                            std::less<std::vector<RowComparator>::value_type>> pq;

    void SetValueForSingleRowTable(omniruntime::vec::VectorBatch *vectorBatch, int32_t position,
        omniruntime::vec::VectorBatch *singleRowTable) const;
};
}
}
#endif // OMNI_RUNTIME_TOPN_H
