/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "distinct_limit.h"
#include <memory>
#include <algorithm>
#include "vector/vector_helper.h"
#include "vector/container_vector.h"
#include "vector/vector_batch.h"
#include "operator/hash_util.h"
#include "operator/util/operator_util.h"
#include "operator/aggregation/group_aggregation.h"

using namespace std;
using namespace omniruntime::type;
namespace omniruntime {
namespace op {
static void DoubleCheckEqualFuncImp(Vector *inputColVector, const uint32_t rowIndex, AggregateState &existedRow,
    bool &isSame)
{
    auto *typeVector = static_cast<DoubleVector *>(inputColVector);
    if (std::abs(*(static_cast<double *>(existedRow.val)) - typeVector->GetValue(rowIndex)) < __DBL_EPSILON__) {
        isSame = true;
        return;
    }

    isSame = false;
    return;
}

template <typename VT, typename DT>
static void FillOutputFuncImp(VectorBatch *outBatch, std::vector<AggregateState> &srcRowVector, int32_t rowIndex,
    int32_t colIndex)
{
    // nullptr handle
    if (srcRowVector[colIndex].val == nullptr) {
        Vector *resultCol = outBatch->GetVector(colIndex);
        resultCol->SetValueNull(rowIndex);
        return;
    }

    VT *typeVector = static_cast<VT *>(outBatch->GetVector(colIndex));
    typeVector->SetValue(rowIndex, *(static_cast<DT *>(srcRowVector[colIndex].val)));
}

static void FillVarcharFuncImp(VectorBatch *resultBatch, std::vector<AggregateState> &rowVector, int32_t rowIndex,
    int32_t colIndex)
{
    // nullptr handle
    if (rowVector[colIndex].val == nullptr) {
        Vector *resultCol = resultBatch->GetVector(colIndex);
        resultCol->SetValueNull(rowIndex);
        return;
    }

    uint8_t *existedStr = reinterpret_cast<uint8_t *>(rowVector[colIndex].strVal);
    auto varcharVector = reinterpret_cast<VarcharVector *>(resultBatch->GetVector(colIndex));
    varcharVector->SetValue(rowIndex, existedStr, rowVector[colIndex].strLen);
}

static constexpr DistinctLimitFuncSet DISTINCT_LIMIT_FUNC_SET[DATA_TYPE_MAX_COUNT] = {
    {OMNI_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INT, DuplicateKeyValueImpl<IntVector, int32_t>, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector,
     int32_t>,
     IsSameNodeFuncImpl<IntVector, int32_t>,
     FillOutputFuncImp<IntVector, int32_t>
    },
    {OMNI_LONG, DuplicateKeyValueImpl<LongVector, int64_t>, HashFuncImpl<LongVector, int64_t>,
     HashFuncVectImpl<LongVector, int64_t>,  IsSameNodeFuncImpl<LongVector, int64_t>,
     FillOutputFuncImp<LongVector, int64_t>
    },
    {OMNI_DOUBLE, DuplicateKeyValueImpl<DoubleVector, double>, HashFuncImpl<DoubleVector, double>,
     HashFuncVectImpl<DoubleVector, double>, DoubleCheckEqualFuncImp, FillOutputFuncImp<DoubleVector, double>
    },
    {OMNI_BOOLEAN, DuplicateKeyValueImpl<BooleanVector, bool>, HashFuncImpl<BooleanVector, bool>,
     HashFuncVectImpl<BooleanVector, bool>,  IsSameNodeFuncImpl<BooleanVector, bool>,
     FillOutputFuncImp<BooleanVector, bool>
    },
    {OMNI_SHORT, DuplicateKeyValueImpl<ShortVector, int16_t>, HashFuncImpl<ShortVector, int16_t>,
     HashFuncVectImpl<ShortVector, int16_t>, IsSameNodeFuncImpl<ShortVector, int16_t>,
     FillOutputFuncImp<ShortVector, int16_t>},
    {
        OMNI_DECIMAL64, DuplicateKeyValueImpl<LongVector, int64_t>, HashFuncImpl<LongVector, int64_t>,
        HashFuncVectImpl<LongVector, int64_t>,  IsSameNodeFuncImpl<LongVector, int64_t>,
        FillOutputFuncImp<LongVector, int64_t>
    },
    {OMNI_DECIMAL128, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>, HashDecimalFunc,
     HashDecimalVectFunc, IsSameNodeFuncImpl<Decimal128Vector, Decimal128>,
     FillOutputFuncImp<Decimal128Vector, Decimal128>
    },
    {OMNI_DATE32, DuplicateKeyValueImpl<IntVector, int32_t>, HashFuncImpl<IntVector, int32_t>,
     HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>,
     FillOutputFuncImp<IntVector, int32_t>
    },
    {OMNI_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_MONTHS,   nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VARCHAR, DuplicateVarcharKeyValue, HashVarcharFuncImpl, HashVarcharVectFuncImpl,
     IsSameNodeFuncVarcharImpl, FillVarcharFuncImp
    },
    {OMNI_CHAR, DuplicateVarcharKeyValue, HashVarcharFuncImpl, HashVarcharVectFuncImpl, IsSameNodeFuncVarcharImpl,
     FillVarcharFuncImp },
    {OMNI_CONTAINER, nullptr, nullptr, nullptr, nullptr, nullptr},
};

DistinctLimitOperatorFactory::DistinctLimitOperatorFactory(const type::DataTypes &sourceTypes,
    const int32_t *distinctCols, int32_t distinctColsCount, int32_t hashCol, int64_t limitVal)
    : sourceTypes(sourceTypes),
      distinctCols(distinctCols, distinctCols + distinctColsCount),
      distinctColsCount(distinctColsCount),
      hashCol(hashCol),
      limit(limitVal)
{}

DistinctLimitOperatorFactory::~DistinctLimitOperatorFactory() = default;

DistinctLimitOperatorFactory *DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
    const type::DataTypes &inSourceTypes, const int32_t *inDistinctCols, int32_t inDistinctColsCount,
    int32_t inHashColumn, int64_t inLimit)
{
    return new DistinctLimitOperatorFactory(inSourceTypes, inDistinctCols, inDistinctColsCount, inHashColumn, inLimit);
}

Operator *DistinctLimitOperatorFactory::CreateOperator()
{
    const int32_t *vectorTypeIds = sourceTypes.GetIds();
    int32_t typeId;
    for (int i = 0; i < distinctColsCount; ++i) {
        typeId = vectorTypeIds[distinctCols[i]];
        if (typeId >= OMNI_INVALID) {
            return nullptr;
        }

        if (DISTINCT_LIMIT_FUNC_SET[typeId].duplicateValueFunc == nullptr) {
            return nullptr;
        }
    }

    return new DistinctLimitOperator(sourceTypes, distinctCols, distinctColsCount, hashCol, limit);
}

void FillPrecomputedHash(Vector **inputVectors, const int32_t *inputTypeIds, const int32_t start, int32_t rowCount,
    uint64_t *combineHashVal, int32_t preComputedHashCol)
{
    int32_t originalRowIndex;
    Vector *originalVector = nullptr;

    switch (inputTypeIds[preComputedHashCol]) {
        case OMNI_INT:
        case OMNI_DATE32: {
            for (int i = 0; i < rowCount; ++i) {
                originalVector =
                    VectorHelper::ExpandVectorAndIndex(inputVectors[preComputedHashCol], start + i, originalRowIndex);
                combineHashVal[i] =
                    static_cast<uint64_t>((static_cast<IntVector *>(originalVector))->GetValue(originalRowIndex));
            }
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            for (int i = 0; i < rowCount; ++i) {
                originalVector =
                    VectorHelper::ExpandVectorAndIndex(inputVectors[preComputedHashCol], start + i, originalRowIndex);
                combineHashVal[i] =
                    static_cast<uint64_t>((static_cast<LongVector *>(originalVector))->GetValue(originalRowIndex));
            }
            break;
        }
        default:
            LogError("Invalid hash data type(%d) for precomputed hash column.", inputTypeIds[preComputedHashCol]);
            break;
    }
}

void GenerateCombinedHash(Vector **inputVectors, const int32_t start, int32_t rowCount, const int32_t *inputTypeIds,
    std::vector<int32_t> &distinctColumns, const int32_t distinctColNum, uint64_t *combineHashVal,
    int32_t preComputedHashCol)
{
    for (int i = 0; i < rowCount; ++i) {
        combineHashVal[i] = 0;
    }

    if (preComputedHashCol != DistinctLimitOperator::INVALID_DISTINCT_COL_ID) {
        FillPrecomputedHash(inputVectors, inputTypeIds, start, rowCount, combineHashVal, preComputedHashCol);
        return;
    }

    int32_t colIndex;
    int32_t typeId;
    for (int32_t i = 0; i < distinctColNum; ++i) {
        colIndex = distinctColumns[i];
        typeId = inputTypeIds[colIndex];
        if (DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFuncVect == nullptr) {
            LogError("No such data type %d", typeId);
            break;
        }

        if (inputVectors[colIndex]->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            auto newIndexes = new int32_t[rowCount];
            Vector *originalVector = static_cast<DictionaryVector *>(inputVectors[colIndex])
                                         ->ExtractDictionaryAndIds(start, rowCount, newIndexes);
            DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFunc(originalVector, rowCount, newIndexes, combineHashVal);
            delete[] newIndexes;
        } else {
            DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFuncVect(inputVectors[colIndex], start, rowCount,
                combineHashVal);
        }
    }
}

DistinctLimitOperator::DistinctLimitOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &distinctCols,
    int32_t distinctColsCount, int32_t hashCol, int64_t limit)
    : executionContext(new ExecutionContext()),
      sourceTypes(sourceTypes),
      distinctCols(distinctCols),
      distinctColsCount(distinctColsCount),
      hashCol(hashCol),
      outColsCount(distinctColsCount + ((hashCol == INVALID_DISTINCT_COL_ID) ? 0 : 1)),
      remainingLimit(limit),
      limit(limit),
      resultBatch(nullptr)
{
    auto &sourceDataType = this->sourceTypes.Get();
    for (int i = 0; i < distinctColsCount; ++i) {
        int colIndex = distinctCols[i];
        outTypes.push_back(sourceDataType[colIndex]);
        outCols.push_back(colIndex);
    }

    if (hashCol != INVALID_DISTINCT_COL_ID) {
        outTypes.push_back(sourceDataType[hashCol]);
        outCols.push_back(hashCol);
    }
}

DistinctLimitOperator::~DistinctLimitOperator()
{
    delete executionContext;
}

bool IsExistSameRow(const type::DataTypes &inputTypes, Vector **inputVectors, std::vector<int32_t> &distinctCols,
    int32_t distinctColsCount, std::vector<std::vector<AggregateState>> &bucket, int rowIndex)
{
    bool isSame = true;
    int32_t columnId;
    int32_t typeId;
    Vector *inputVector = nullptr;

    for (uint32_t i = 0; i < bucket.size(); i++) {
        isSame = true;

        std::vector<AggregateState> &rowVector = bucket[i];
        int32_t originalRowIndex;
        Vector *originalVector = nullptr;
        for (int32_t column = 0; ((column < distinctColsCount) && isSame); ++column) {
            columnId = distinctCols[column];
            typeId = inputTypes.GetType(columnId)->GetId();
            inputVector = inputVectors[columnId];

            originalVector = VectorHelper::ExpandVectorAndIndex(inputVector, rowIndex, originalRowIndex);
            if ((rowVector[column].val == nullptr) || (originalVector->IsValueNull(originalRowIndex))) {
                isSame = ((rowVector[column].val == nullptr) && (originalVector->IsValueNull(originalRowIndex)));
                continue;
            }

            DISTINCT_LIMIT_FUNC_SET[typeId].checkEqualFunc(originalVector, originalRowIndex, rowVector[column], isSame);
        }

        // all distinct cols are same as bucket[i]
        if (isSame) {
            return true;
        }
    }

    return false;
}

void DistinctLimitOperator::FillDistinctedTuple(Vector **inputVectors, int rowIndex, std::vector<AggregateState> &tuple)
{
    const int32_t *vectorTypes = sourceTypes.GetIds();
    Vector *inputVector = nullptr;
    int32_t originalRowIndex;
    Vector *originalVector = nullptr;

    for (int32_t colIndex = 0; colIndex < outColsCount; ++colIndex) {
        int32_t colId = outCols[colIndex];
        int32_t typeId = vectorTypes[colId];
        tuple[colIndex].val = nullptr;
        tuple[colIndex].count = 0;
        inputVector = inputVectors[colId];
        originalVector = VectorHelper::ExpandVectorAndIndex(inputVector, rowIndex, originalRowIndex);
        if (!(originalVector->IsValueNull(rowIndex))) {
            DISTINCT_LIMIT_FUNC_SET[typeId].duplicateValueFunc(tuple[colIndex], originalVector, originalRowIndex,
                executionContext);
        }
    }
}

void DistinctLimitOperator::InLoop(VectorBatch *vecBatch, uint64_t *combineHashVal)
{
    bool existed = false;
    uint64_t hashValue;
    int32_t pickedRows = 0;
    Vector **inputVectors = vecBatch->GetVectors();

    for (int rowIndex = 0; rowIndex < vecBatch->GetRowCount(); ++rowIndex) {
        hashValue = combineHashVal[rowIndex];
        auto &bucket = distinctedTable[hashValue];

        existed = IsExistSameRow(sourceTypes, inputVectors, distinctCols, distinctColsCount, bucket, rowIndex);
        if (!existed) {
            std::vector<AggregateState> distinctedTuple(outColsCount);
            FillDistinctedTuple(inputVectors, rowIndex, distinctedTuple);
            bucket.push_back(distinctedTuple);

            auto rowInfo = new DistinctRowInfo();
            rowInfo->hashValue = hashValue;
            rowInfo->slotIndex = static_cast<int32_t>(bucket.size() - 1);
            distinctRowInfo.push_back(rowInfo);

            pickedRows++;
            if (pickedRows >= remainingLimit) {
                break;
            }
        }
    }

    this->remainingLimit -= pickedRows;
}

int32_t DistinctLimitOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch == nullptr) {
        return 0;
    }
    if ((vecBatch->GetRowCount() == 0) || (remainingLimit < 0)) {
        VectorHelper::FreeVecBatch(vecBatch);
        return 0;
    }

    int32_t rowCount = vecBatch->GetRowCount();
    Vector **inputVectors = vecBatch->GetVectors();

    const int32_t *inputTypeIds = sourceTypes.GetIds();
    auto combineHashVal = std::make_unique<uint64_t[]>(rowCount);

    GenerateCombinedHash(inputVectors, 0, rowCount, inputTypeIds, distinctCols, distinctColsCount, combineHashVal.get(),
        this->hashCol);

    this->InLoop(vecBatch, combineHashVal.get());
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

void FillOutPutValue(VectorBatch *resultBatch, std::vector<AggregateState> &rowVector,
    std::vector<type::DataTypePtr> &outTypes, int32_t rowIndex)
{
    for (int i = 0; i < static_cast<int>(outTypes.size()); ++i) {
        // nullptr handle
        if (rowVector[i].val == nullptr) {
            Vector *resultCol = resultBatch->GetVector(i);
            resultCol->SetValueNull(rowIndex);
            continue;
        }

        DISTINCT_LIMIT_FUNC_SET[outTypes[i]->GetId()].fillOutputFunc(resultBatch, rowVector, rowIndex, i);
    }
}

int32_t DistinctLimitOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    if (distinctRowInfo.size() > 0) {
        resultBatch = new VectorBatch(outColsCount, distinctRowInfo.size());

        resultBatch->NewVectors(vecAllocator, outTypes);

        int32_t rowIndex = 0;
        for (auto item : distinctRowInfo) {
            std::vector<AggregateState> &rowVector = distinctedTable[item->hashValue][item->slotIndex];
            FillOutPutValue(resultBatch, rowVector, outTypes, rowIndex++);
        }

        outputPages.push_back(resultBatch);
        resultBatch = nullptr;
        ReleaseRowInfo(distinctRowInfo);
    }

    if (remainingLimit <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
    }

    return 0;
}

void DistinctLimitOperator::ReleaseRowInfo(std::vector<DistinctRowInfo *> &rowInfo)
{
    for (auto item : rowInfo) {
        delete item;
    }
    rowInfo.clear();
}

OmniStatus DistinctLimitOperator::Close()
{
    if (resultBatch != nullptr) {
        VectorHelper::FreeVecBatch(resultBatch);
        resultBatch = nullptr;
    }

    // releaseMemory: memory managed/free by ExecutionContext
    distinctedTable.clear();

    ReleaseRowInfo(distinctRowInfo);

    return OMNI_STATUS_NORMAL;
}
}
}
