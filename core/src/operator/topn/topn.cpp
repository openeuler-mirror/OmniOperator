/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "topn.h"
#include <vector>
#include "../../jit/annotation.h"
#include "../../vector/long_vector.h"
#include "../../vector/vector_helper.h"
#include "../optimization.h"
#include "../sort/sort.h"
#include "../util/operator_util.h"

using namespace std;
namespace omniruntime {
namespace op {
    using namespace omniruntime::vec;
TopNOperatorFactory::TopNOperatorFactory(int32_t *sourceTypes, int32_t sourceTypesCount, int32_t n, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    this->sourceTypes = sourceTypes;
    this->sourceTypesCount = sourceTypesCount;
    this->n = n;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
}

TopNOperatorFactory::~TopNOperatorFactory() {}

Operator *TopNOperatorFactory::CreateOperator()
{
    return new TopNOperator(sourceTypes, sourceTypesCount, n, sortCols, sortAscendings, sortNullFirsts, sortColCount);
}

TopNOperator::TopNOperator(int32_t *sourceTypes, int32_t sourceTypesCount, int32_t n, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    this->sourceTypes = sourceTypes;
    this->sourceTypesCount = sourceTypesCount;
    this->n = n;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
}

TopNOperator::~TopNOperator()
{
    // since Java will cover the life cycle of sourceTypes and so on, so we don't do anything here
}

int32_t TopNOperator::AddInput(VectorBatch *vectorBatch)
{
    for (int32_t position = 0; position < vectorBatch->GetRowCount(); ++position) {
        if ((pq.size() < n) ||
            Compare(position, vectorBatch, pq.top().GetVecBatch(), sortColCount,
                    sortCols, sourceTypes, sortAscendings) < 0) {
            VectorBatch* singleRowTable = new VectorBatch(sourceTypesCount, 1);
            singleRowTable->SetVectors(sourceTypes);
            SetValueForSingleRowTable(vectorBatch, position, singleRowTable);
            RowComparator* rowComparator = new RowComparator(sourceTypes, sortCols, sortAscendings,
                                                             sortColCount, singleRowTable);
            pq.push(*rowComparator);
            while (pq.size() > n) {
                pq.pop();
            }
        }
    }
    return 0;
}

void TopNOperator::SetValueForSingleRowTable(VectorBatch *vectorBatch, int32_t position,
                                             VectorBatch *singleRowTable) const
{
    for (int i = 0; i < sourceTypesCount; ++i) {
        switch (sourceTypes[i]) {
            case OMNI_VEC_TYPE_INT:
                (dynamic_cast<IntVector *>(singleRowTable->GetVector(i)))
                    ->SetValue(0, (dynamic_cast<IntVector *>(vectorBatch->GetVector(i)))->GetValue(position));
                break;
            case OMNI_VEC_TYPE_LONG:
                (dynamic_cast<LongVector *>(singleRowTable->GetVector(i)))
                    ->SetValue(0, (dynamic_cast<LongVector *>(vectorBatch->GetVector(i)))->GetValue(position));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                (dynamic_cast<DoubleVector *>(singleRowTable->GetVector(i)))
                    ->SetValue(0,
                    (dynamic_cast<DoubleVector *>(vectorBatch->GetVector(i)))->GetValue(position));
                break;
            default:
                break;
        }
    }
}

    int32_t TopNOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatch)
{
    int64_t positionCount = pq.size();
    if (positionCount <= 0) {
        return 0;
    }
    VectorBatch* tmpVecBatch = new VectorBatch(sourceTypesCount, pq.size());
    tmpVecBatch->SetVectors(sourceTypes);
    int32_t outputCols[sourceTypesCount];
    for (int32_t i = 0; i < sourceTypesCount; ++i) {
        outputCols[i] = i;
    }
    int64_t rowNum = 0;

    while (!pq.empty()) {
        VectorBatch *pqVecBatch = pq.top().GetVecBatch();
        for (int i = 0; i < sourceTypesCount; ++i) {
            switch (sourceTypes[i]) {
                case OMNI_VEC_TYPE_INT:
                    (dynamic_cast<IntVector *>(tmpVecBatch->GetVector(i)))->SetValue(positionCount - rowNum - 1,
                        (dynamic_cast<IntVector *>(pqVecBatch->GetVector(i)))->GetValue(0));
                    break;
                case OMNI_VEC_TYPE_LONG:
                    (dynamic_cast<LongVector *>(tmpVecBatch->GetVector(i)))->SetValue(positionCount - rowNum - 1,
                        (dynamic_cast<LongVector *>(pqVecBatch->GetVector(i)))->GetValue(0));
                    break;
                case OMNI_VEC_TYPE_DOUBLE:
                    (dynamic_cast<DoubleVector *>(tmpVecBatch->GetVector(i)))->SetValue(positionCount - rowNum - 1,
                        (dynamic_cast<DoubleVector *>(pqVecBatch->GetVector(i)))->GetValue(0));
                    break;
                default:
                    break;
            }
        }
        rowNum++;
        pq.pop();
    }
    outputVecBatch.push_back(tmpVecBatch);
    return 0;
}

SPECIALIZE(OMNIJIT_TOPN_COMPARE)
int32_t TopNOperator::Compare(int32_t position, VectorBatch *vectorBatch, VectorBatch *currentMaxVectorBatch,
    int32_t sortColCount, const int32_t *sortCols, const int32_t *sourceTypes, const int32_t *sortAscendings) const
{
    int compare = 0;

    for (int i = 0; i < sortColCount; ++i) {
        int32_t sortCol = sortCols[i];
        int32_t colType = sourceTypes[sortCol];
        compare = OperatorUtil::CompareVectorAtPosition(static_cast<VecType>(colType), vectorBatch->GetVector(sortCol),
                                                        position, currentMaxVectorBatch->GetVector(sortCol), 0);
        if (sortAscendings[i] == 0) {
            compare = -compare;
        }

        if (compare != 0) {
            break;
        }
    }
    return compare;
}

RowComparator::RowComparator(int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings,
                             int32_t sortColCount, VectorBatch* vectorBatch)
{
    this->sourceTypes = sourceTypes;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortColCount = sortColCount;
    this->vectorBatch = vectorBatch;
}

RowComparator::~RowComparator() {
}

int32_t *RowComparator::GetSourceTypes() const
{
    return sourceTypes;
}

int32_t *RowComparator::GetSortAscendings() const
{
    return sortAscendings;
}

int32_t RowComparator::GetSortColCount() const
{
    return sortColCount;
}

VectorBatch* RowComparator::GetVecBatch() const
{
    return vectorBatch;
}

int32_t *RowComparator::GetSortCols() const
{
    return sortCols;
}

bool operator < (const RowComparator &left, const RowComparator &right)
{
    int compare = 0;

    for (int i = 0; i < left.GetSortColCount(); ++i) {
        int32_t sortCol = left.GetSortCols()[i];
        int32_t colType = left.GetSourceTypes()[sortCol];

        compare = OperatorUtil::CompareVectorAtPosition(static_cast<VecType>(colType),
                                                        left.GetVecBatch()->GetVector(sortCol), 0,
                                                        right.GetVecBatch()->GetVector(sortCol), 0);

        if (left.GetSortAscendings()[i] == 0 && right.GetSortAscendings()[i] == 0) {
            compare = -compare;
        }

        if (compare != 0) {
            break;
        }
    }
    // priority_queue is desc, return 1 means left smaller than right,so
    // priority_queue will swap. suppose output is asc,compare>0 means left bigger
    // than right so pq shouldn't swap,so return 0
    if (compare >= 0) {
        return 0;
    } else {
        return 1;
    }
}
} // namespace op
} // namespace omniruntime