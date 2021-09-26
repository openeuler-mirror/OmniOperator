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
TopNOperatorFactory::TopNOperatorFactory(const vec::VecTypes &sourceTypes, int32_t n, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
    : sourceTypes(sourceTypes)
{
    this->n = n;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
}

TopNOperatorFactory::~TopNOperatorFactory() {}

Operator *TopNOperatorFactory::CreateOperator()
{
    return new TopNOperator(sourceTypes, n, sortCols, sortAscendings, sortNullFirsts, sortColCount);
}

TopNOperator::TopNOperator(const vec::VecTypes &sourceTypes, int32_t n, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypes.GetSize())
{
    this->n = n;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
}

TopNOperator::~TopNOperator()
{
    for (const auto &item : singleRowVectorBatchList) {
        item->ReleaseAllVectors();
        delete item;
    }
}

int32_t TopNOperator::AddInput(VectorBatch *vectorBatch)
{
    auto typeIds = sourceTypes.GetIds();
    for (int32_t position = 0; position < vectorBatch->GetRowCount(); ++position) {
        if ((pq.size() < n) || CompareVectorBatch(position, vectorBatch, 0, pq.top().GetVecBatch(), sortColCount,
            sortCols, typeIds, sortAscendings, sortNullFirsts) < 0) {
            VectorBatch *singleRowVecBatch = CreateSingleRowVecBatch(vectorBatch, position);
            RowComparator *rowComparator =
                new RowComparator(typeIds, sortCols, sortAscendings, sortNullFirsts, sortColCount, singleRowVecBatch);
            pq.push(*rowComparator);
            while (pq.size() > n) {
                pq.pop();
            }
            singleRowVectorBatchList.push_back(singleRowVecBatch);
        }
    }
    return 0;
}

template <typename T>
void ALWAYS_INLINE SetVectorForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex, Vector *vector,
    int32_t position)
{
    singleRowVecBatch->SetVector(colIndex, (static_cast<T *>(vector))->CopyRegion(position, 1));
}

VectorBatch *TopNOperator::CreateSingleRowVecBatch(VectorBatch *vectorBatch, int32_t position) const
{
    auto typeIds = sourceTypes.GetIds();
    VectorBatch *singleRowVecBatch = new VectorBatch(sourceTypesCount);
    for (int i = 0; i < sourceTypesCount; ++i) {
        int32_t originalPosition;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(i), position, originalPosition);
        switch (typeIds[i]) {
            case OMNI_VEC_TYPE_BOOLEAN:
                SetVectorForSingleRowVecBatch<BooleanVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                SetVectorForSingleRowVecBatch<IntVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                SetVectorForSingleRowVecBatch<LongVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                SetVectorForSingleRowVecBatch<DoubleVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                SetVectorForSingleRowVecBatch<VarcharVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
                SetVectorForSingleRowVecBatch<Decimal128Vector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            default:
                break;
        }
    }
    return singleRowVecBatch;
}

template <typename T> void ALWAYS_INLINE SetValueForVector(Vector *pqVector, Vector *tmpVector, int64_t index)
{
    (static_cast<T *>(tmpVector))->SetValue(index, (static_cast<T *>(pqVector))->GetValue(0));
}

int32_t TopNOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatch)
{
    int64_t positionCount = pq.size();
    if (positionCount <= 0) {
        return 0;
    }
    VectorBatch *tmpVecBatch = new VectorBatch(sourceTypesCount, pq.size());
    tmpVecBatch->NewVectors(vecAllocator, sourceTypes.Get());
    int64_t rowNum = 0;
    auto typeIds = sourceTypes.GetIds();
    while (!pq.empty()) {
        VectorBatch *pqVecBatch = pq.top().GetVecBatch();
        int64_t index = positionCount - rowNum - 1;
        for (int i = 0; i < sourceTypesCount; ++i) {
            Vector *pqVector = pqVecBatch->GetVector(i);
            Vector *tmpVector = tmpVecBatch->GetVector(i);
            if (pqVector->IsValueNull(0)) {
                tmpVector->SetValueNull(index);
            }
            SetValueForVectorBatch(rowNum, typeIds, index, i, pqVector, tmpVector);
        }
        rowNum++;
        pq.pop();
    }
    HandleVarchar(positionCount, tmpVecBatch);
    outputVecBatch.push_back(tmpVecBatch);
    return 0;
}

void TopNOperator::SetValueForVectorBatch(int64_t rowNum, const int32_t *typeIds, int64_t index, int i,
    Vector *pqVector, Vector *tmpVector) const
{
    switch (typeIds[i]) {
        case OMNI_VEC_TYPE_BOOLEAN:
            SetValueForVector<BooleanVector>(pqVector, tmpVector, index);
            break;
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            SetValueForVector<IntVector>(pqVector, tmpVector, index);
            break;
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            SetValueForVector<LongVector>(pqVector, tmpVector, index);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            SetValueForVector<DoubleVector>(pqVector, tmpVector, index);
            break;
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *value = nullptr;
            int32_t valueLength = (static_cast<VarcharVector *>(pqVector))->GetValue(0, &value);
            (static_cast<VarcharVector *>(tmpVector))
                ->SetValue(rowNum, reinterpret_cast<const uint8_t *>(value), valueLength);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128:
            SetValueForVector<Decimal128Vector>(pqVector, tmpVector, index);
            break;
        default:
            break;
    }
}

void TopNOperator::HandleVarchar(int64_t positionCount, VectorBatch *tmpVecBatch) const
{
    int vecIndex = 0;
    for (const VecType &item : sourceTypes.Get()) {
        if (item.GetId() == OMNI_VEC_TYPE_VARCHAR) {
            auto vecType = (VarcharVecType &)item;
            VarcharVector *varcharVector =
                new VarcharVector(vecAllocator, positionCount * vecType.GetWidth(), positionCount);
            for (int i = 0; i < positionCount; ++i) {
                uint8_t *value = nullptr;
                int32_t valueLength = (static_cast<VarcharVector *>(tmpVecBatch->GetVector(vecIndex)))
                                          ->GetValue(positionCount - i - 1, &value);
                varcharVector->SetValue(i, value, valueLength);
            }
            delete tmpVecBatch->GetVector(vecIndex);
            tmpVecBatch->SetVector(vecIndex, varcharVector);
        }
        vecIndex++;
    }
}

SPECIALIZE(OMNIJIT_TOPN_COMPARE)
int CompareVectorBatch(int32_t leftPosition, VectorBatch *left, int32_t rightPosition, VectorBatch *right,
    int32_t sortColCount, const int32_t *sortCols, const int32_t *sourceTypeIds, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts)
{
    int compare = 0;

    for (int i = 0; i < sortColCount; ++i) {
        int32_t sortCol = sortCols[i];
        int32_t colTypeId = sourceTypeIds[sortCol];

        Vector *leftVector = left->GetVector(sortCol);
        Vector *rightVector = right->GetVector(sortCol);

        int32_t originalLeftPosition, originalRightPosition;
        leftVector = VectorHelper::ExpandVectorAndIndex(leftVector, leftPosition, originalLeftPosition);
        rightVector = VectorHelper::ExpandVectorAndIndex(rightVector, rightPosition, originalRightPosition);

        compare = OperatorUtil::CompareNull(leftVector, originalLeftPosition, rightVector,
                                            originalRightPosition, sortNullFirsts[i]);
        if (compare == OperatorUtil::COMPARE_STATUS_GREATER_THAN || compare == OperatorUtil::COMPARE_STATUS_LESS_THAN) {
            break;
        } else if (compare == OperatorUtil::COMPARE_STATUS_EQUAL) {
            continue;
        }

        compare = OperatorUtil::CompareVectorAtPosition(colTypeId, leftVector, originalLeftPosition, rightVector,
            originalRightPosition);
        if (sortAscendings[i] == 0) {
            compare = -compare;
        }

        if (compare != 0) {
            break;
        }
    }
    return compare;
}

RowComparator::RowComparator(const int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount, omniruntime::vec::VectorBatch *vectorBatch)
    : sourceTypes(sourceTypes)
{
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
    this->vectorBatch = vectorBatch;
}

RowComparator::~RowComparator() {}

const int32_t *RowComparator::GetSourceTypes() const
{
    return sourceTypes;
}

int32_t *RowComparator::GetSortAscendings() const
{
    return sortAscendings;
}

int32_t *RowComparator::GetSortNullFirsts() const
{
    return sortNullFirsts;
}

int32_t RowComparator::GetSortColCount() const
{
    return sortColCount;
}

VectorBatch *RowComparator::GetVecBatch() const
{
    return vectorBatch;
}

int32_t *RowComparator::GetSortCols() const
{
    return sortCols;
}

bool operator < (const RowComparator &left, const RowComparator &right)
{
    int compare = CompareVectorBatch(0, left.GetVecBatch(), 0, right.GetVecBatch(), left.GetSortColCount(),
        left.GetSortCols(), left.GetSourceTypes(), left.GetSortAscendings(), left.GetSortNullFirsts());
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