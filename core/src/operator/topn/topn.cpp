/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "topn.h"
#include <vector>
#include "operator/sort/sort.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace std;

TopNOperatorFactory::TopNOperatorFactory(const type::DataTypes &sourceTypes, int32_t n, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
    : sourceTypes(sourceTypes)
{
    this->n = n;
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
    this->sortColCount = sortColCount;
}

TopNOperatorFactory::~TopNOperatorFactory() = default;

Operator *TopNOperatorFactory::CreateOperator()
{
    return new TopNOperator(sourceTypes, n, sortCols, sortAscendings, sortNullFirsts, sortColCount);
}

TopNOperator::TopNOperator(const type::DataTypes &sourceTypes, int32_t n, std::vector<int32_t> &sortCols,
    std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, int32_t sortColCount)
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

        compare = OperatorUtil::CompareNull(leftVector, originalLeftPosition, rightVector, originalRightPosition,
            sortNullFirsts[i]);
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

int32_t TopNOperator::AddInput(VectorBatch *vectorBatch)
{
    auto typeIds = sourceTypes.GetIds();
    int32_t position = 0;
    for (; (static_cast<int32_t>(pq.size()) < n) && (position < vectorBatch->GetRowCount()); ++position) {
        VectorBatch *singleRowVecBatch = CreateSingleRowVecBatch(vectorBatch, position);
        pq.emplace(typeIds, sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount,
            singleRowVecBatch);
        singleRowVectorBatchList.push_back(singleRowVecBatch);
    }
    for (; position < vectorBatch->GetRowCount(); ++position) {
        VectorBatch *top = pq.top().GetVecBatch();
        if (CompareVectorBatch(position, vectorBatch, 0, top, sortColCount, sortCols.data(), typeIds,
            sortAscendings.data(), sortNullFirsts.data()) < 0) {
            pq.pop();
            UpdateSingleRowVectorBatch(vectorBatch, top, position);
            pq.emplace(typeIds, sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, top);
        }
    }
    VectorHelper::FreeVecBatch(vectorBatch);
    return 0;
}

template <typename T>
static void ALWAYS_INLINE SetValueForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex, Vector *vector,
    int32_t position)
{
    auto resultVector = static_cast<T *>(singleRowVecBatch->GetVector(colIndex));
    auto inputVector = static_cast<T *>(vector);
    resultVector->SetValueNull(0, inputVector->IsValueNull(position));
    resultVector->SetValue(0, inputVector->GetValue(position));
}

static void ALWAYS_INLINE SetVarCharForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex,
    Vector *vector, int32_t position)
{
    auto resultVector = static_cast<VarcharVector *>(singleRowVecBatch->GetVector(colIndex));
    auto inputVector = static_cast<VarcharVector *>(vector);
    // we just need to set value null
    if (inputVector->IsValueNull(position)) {
        resultVector->SetValueNull(0, true);
        return;
    }
    // we need to delete then re-allocate;
    delete resultVector;
    singleRowVecBatch->SetVector(colIndex, inputVector->CopyRegion(position, 1));
}

void TopNOperator::UpdateSingleRowVectorBatch(VectorBatch *vectorBatch, VectorBatch *singleRowVecBatch,
    int32_t position) const
{
    auto typeIds = sourceTypes.GetIds();
    for (int i = 0; i < sourceTypesCount; ++i) {
        int32_t originalPosition;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(i), position, originalPosition);
        switch (typeIds[i]) {
            case OMNI_BOOLEAN:
                SetValueForSingleRowVecBatch<BooleanVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                SetValueForSingleRowVecBatch<IntVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                SetValueForSingleRowVecBatch<LongVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_DOUBLE:
                SetValueForSingleRowVecBatch<DoubleVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                SetVarCharForSingleRowVecBatch(singleRowVecBatch, i, vector, originalPosition);
                break;
            }
            case OMNI_DECIMAL128:
                SetValueForSingleRowVecBatch<Decimal128Vector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            default:
                break;
        }
    }
}

template <typename T>
static void ALWAYS_INLINE SetVectorForSingleRowVecBatch(VectorBatch *singleRowVecBatch, int32_t colIndex,
    Vector *vector, int32_t position)
{
    singleRowVecBatch->SetVector(colIndex, (static_cast<T *>(vector))->CopyRegion(position, 1));
}

VectorBatch *TopNOperator::CreateSingleRowVecBatch(VectorBatch *vectorBatch, int32_t position) const
{
    auto typeIds = sourceTypes.GetIds();
    auto singleRowVecBatch = new VectorBatch(sourceTypesCount, 1);
    for (int i = 0; i < sourceTypesCount; ++i) {
        int32_t originalPosition;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(i), position, originalPosition);
        switch (typeIds[i]) {
            case OMNI_BOOLEAN:
                SetVectorForSingleRowVecBatch<BooleanVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                SetVectorForSingleRowVecBatch<IntVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                SetVectorForSingleRowVecBatch<LongVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_DOUBLE:
                SetVectorForSingleRowVecBatch<DoubleVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                SetVectorForSingleRowVecBatch<VarcharVector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            }
            case OMNI_DECIMAL128:
                SetVectorForSingleRowVecBatch<Decimal128Vector>(singleRowVecBatch, i, vector, originalPosition);
                break;
            default:
                break;
        }
    }
    return singleRowVecBatch;
}

template <typename T> static void ALWAYS_INLINE SetValueForVector(Vector *pqVector, Vector *tmpVector, int64_t index)
{
    (static_cast<T *>(tmpVector))->SetValue(index, (static_cast<T *>(pqVector))->GetValue(0));
}

int32_t TopNOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatch)
{
    int64_t positionCount = static_cast<int64_t>(pq.size());
    if (positionCount <= 0) {
        return 0;
    }
    auto tmpVecBatch = new VectorBatch(sourceTypesCount, pq.size());
    tmpVecBatch->NewVectors(vecAllocator, sourceTypes.Get());
    int64_t rowNum = 0;
    auto typeIds = sourceTypes.GetIds();
    while (!pq.empty()) {
        VectorBatch *pqVecBatch = pq.top().GetVecBatch();
        int64_t index = positionCount - rowNum - 1;
        for (int i = 0; i < sourceTypesCount; ++i) {
            Vector *pqVector = pqVecBatch->GetVector(i);
            Vector *tmpVector = tmpVecBatch->GetVector(i);
            if (typeIds[i] == OMNI_VARCHAR || typeIds[i] == OMNI_CHAR) {
                SetVarcharValueForVectorBatch(rowNum, static_cast<VarcharVector *>(pqVector),
                    static_cast<VarcharVector *>(tmpVector));
            } else {
                SetValueForVectorBatch(typeIds[i], index, pqVector, tmpVector);
            }
        }
        rowNum++;
        pq.pop();
    }
    HandleVarchar(positionCount, tmpVecBatch);
    outputVecBatch.push_back(tmpVecBatch);
    return 0;
}

void TopNOperator::SetValueForVectorBatch(int32_t typeId, int64_t index, Vector *pqVector, Vector *tmpVector) const
{
    if (pqVector->IsValueNull(0)) {
        tmpVector->SetValueNull(index);
        return;
    }
    switch (typeId) {
        case OMNI_BOOLEAN:
            SetValueForVector<BooleanVector>(pqVector, tmpVector, index);
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            SetValueForVector<IntVector>(pqVector, tmpVector, index);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            SetValueForVector<LongVector>(pqVector, tmpVector, index);
            break;
        case OMNI_DOUBLE:
            SetValueForVector<DoubleVector>(pqVector, tmpVector, index);
            break;
        case OMNI_DECIMAL128:
            SetValueForVector<Decimal128Vector>(pqVector, tmpVector, index);
            break;
        default:
            break;
    }
}

void TopNOperator::SetVarcharValueForVectorBatch(int64_t rowNum, VarcharVector *pqVector,
    VarcharVector *tmpVector) const
{
    if (pqVector->IsValueNull(0)) {
        tmpVector->SetValueNull(rowNum);
        return;
    }
    uint8_t *value = nullptr;
    int32_t valueLength = pqVector->GetValue(0, &value);
    tmpVector->SetValue(rowNum, reinterpret_cast<const uint8_t *>(value), valueLength);
}

void TopNOperator::HandleVarchar(int64_t positionCount, VectorBatch *tmpVecBatch) const
{
    int vecIndex = 0;
    for (const DataTypeRawPtr dataTypeRawPtr : sourceTypes.Get()) {
        if (dataTypeRawPtr->GetId() != OMNI_VARCHAR && dataTypeRawPtr->GetId() != OMNI_CHAR) {
            vecIndex++;
            continue;
        }

        auto varcharVector = new VarcharVector(vecAllocator, positionCount * dataTypeRawPtr->GetWidth(), positionCount);
        auto tempVarcharVec = static_cast<VarcharVector *>(tmpVecBatch->GetVector(vecIndex));
        for (int32_t i = 0; i < positionCount; ++i) {
            if (tempVarcharVec->IsValueNull(positionCount - i - 1)) {
                varcharVector->SetValueNull(i);
            } else {
                uint8_t *value = nullptr;
                int32_t valueLength = tempVarcharVec->GetValue(positionCount - i - 1, &value);
                varcharVector->SetValue(i, value, valueLength);
            }
        }
        delete tmpVecBatch->GetVector(vecIndex);
        tmpVecBatch->SetVector(vecIndex, varcharVector);
        vecIndex++;
    }
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

RowComparator::~RowComparator() = default;

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
        return false;
    } else {
        return true;
    }
}
} // namespace op
} // namespace omniruntime
