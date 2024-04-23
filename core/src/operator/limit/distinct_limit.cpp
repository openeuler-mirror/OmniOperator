/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "distinct_limit.h"
#include <memory>
#include <algorithm>
#include "vector/vector_helper.h"
#include "vector/vector_batch.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/vector_getter.h"

using namespace std;
using namespace omniruntime::type;
namespace omniruntime::op {
template <typename T>
static void DoubleCheckEqualFuncImp(BaseVector *inputColVector, const uint32_t rowIndex,
    const AggregateState &existedRow, bool &isSame)
{
    auto *typeVector = dynamic_cast<T *>(inputColVector);
    if (std::abs(*(static_cast<double *>(existedRow.val)) - typeVector->GetValue(rowIndex)) < __DBL_EPSILON__) {
        isSame = true;
        return;
    }

    isSame = false;
}

template <typename VT, typename DT>
static void FillOutputFuncImp(VectorBatch *outBatch, std::vector<AggregateState> &srcRowVector, int32_t rowIndex,
    int32_t colIndex)
{
    // nullptr handle
    if (srcRowVector[colIndex].val == nullptr) {
        BaseVector *resultCol = outBatch->Get(colIndex);
        resultCol->SetNull(rowIndex);
        return;
    }

    auto typeVector = static_cast<VT *>(outBatch->Get(colIndex));
    typeVector->SetValue(rowIndex, *(static_cast<DT *>(srcRowVector[colIndex].val)));
}

static void FillVarcharFuncImp(VectorBatch *resultBatch, std::vector<AggregateState> &rowVector, int32_t rowIndex,
    int32_t colIndex)
{
    using VarcharVector = NativeAndVectorType<type::DataTypeId::OMNI_VARCHAR>::vector;
    // nullptr handle
    if (rowVector[colIndex].val == nullptr) {
        VarcharVector *resultCol = reinterpret_cast<VarcharVector *>(resultBatch->Get(colIndex));
        resultCol->SetNull(rowIndex);
        return;
    }

    char *existedStr = reinterpret_cast<char *>(rowVector[colIndex].val);
    std::string_view rowVal(existedStr, rowVector[colIndex].count);
    auto varcharVector = reinterpret_cast<VarcharVector *>(resultBatch->Get(colIndex));
    varcharVector->SetValue(rowIndex, rowVal);
}

static constexpr DistinctLimitFuncSet DISTINCT_LIMIT_FUNC_SET[DATA_TYPE_MAX_COUNT] = {
    {OMNI_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INT,
     DuplicateKeyValueImpl<Vector<int32_t>, int32_t>,
     HashFuncImpl<Vector<int32_t>, int32_t>,
     HashFuncVectImplProxy<Vector<int32_t>, int32_t>,
     IsSameNodeFuncImpl<Vector<int32_t>, int32_t>,
     FillOutputFuncImp<Vector<int32_t>, int32_t>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     HashFuncImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int32_t>>, int32_t>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     FillOutputFuncImp<Vector<DictionaryContainer<int32_t>>, int32_t>},
    {OMNI_LONG,
     DuplicateKeyValueImpl<Vector<int64_t>, int64_t>,
     HashFuncImpl<Vector<int64_t>, int64_t>,
     HashFuncVectImplProxy<Vector<int64_t>, int64_t>,
     IsSameNodeFuncImpl<Vector<int64_t>, int64_t>,
     FillOutputFuncImp<Vector<int64_t>, int64_t>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     HashFuncImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int64_t>>, int64_t>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     FillOutputFuncImp<Vector<DictionaryContainer<int64_t>>, int64_t>},
    {OMNI_DOUBLE,
     DuplicateKeyValueImpl<Vector<double>, double>,
     HashFuncImpl<Vector<double>, double>,
     HashFuncVectImplProxy<Vector<double>, double>,
     DoubleCheckEqualFuncImp<Vector<double>>,
     FillOutputFuncImp<Vector<double>, double>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<double>>, double>,
     HashFuncImpl<Vector<DictionaryContainer<double>>, double>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<double>>, double>,
     DoubleCheckEqualFuncImp<Vector<DictionaryContainer<double>>>,
     FillOutputFuncImp<Vector<DictionaryContainer<double>>, double>},
    {OMNI_BOOLEAN,
     DuplicateKeyValueImpl<Vector<bool>, bool>,
     HashFuncImpl<Vector<bool>, bool>,
     HashFuncVectImplProxy<Vector<bool>, bool>,
     IsSameNodeFuncImpl<Vector<bool>, bool>,
     FillOutputFuncImp<Vector<bool>, bool>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int8_t>>, bool>,
     HashFuncImpl<Vector<DictionaryContainer<int8_t>>, bool>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int8_t>>, bool>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int8_t>>, bool>,
     FillOutputFuncImp<Vector<DictionaryContainer<int8_t>>, bool>},
    {OMNI_SHORT,
     DuplicateKeyValueImpl<Vector<int16_t>, int16_t>,
     HashFuncImpl<Vector<int16_t>, int16_t>,
     HashFuncVectImplProxy<Vector<int16_t>, int16_t>,
     IsSameNodeFuncImpl<Vector<int16_t>, int16_t>,
     FillOutputFuncImp<Vector<int16_t>, int16_t>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int16_t>>, int16_t>,
     HashFuncImpl<Vector<DictionaryContainer<int16_t>>, int16_t>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int16_t>>, int16_t>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int16_t>>, int16_t>,
     FillOutputFuncImp<Vector<DictionaryContainer<int16_t>>, int16_t>},
    {OMNI_DECIMAL64,
     DuplicateKeyValueImpl<Vector<int64_t>, int64_t>,
     HashFuncImpl<Vector<int64_t>, int64_t>,
     HashFuncVectImplProxy<Vector<int64_t>, int64_t>,
     IsSameNodeFuncImpl<Vector<int64_t>, int64_t>,
     FillOutputFuncImp<Vector<int64_t>, int64_t>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     HashFuncImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int64_t>>, int64_t>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int64_t>>, int64_t>,
     FillOutputFuncImp<Vector<DictionaryContainer<int64_t>>, int64_t>},
    {OMNI_DECIMAL128,
     DuplicateKeyValueImpl<Vector<Decimal128>, Decimal128>,
     HashDecimalFunc,
     HashDecimalVectFuncProxy,
     IsSameNodeFuncImpl<Vector<Decimal128>, Decimal128>,
     FillOutputFuncImp<Vector<Decimal128>, Decimal128>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<Decimal128>>, Decimal128>,
     HashDecimalFunc<Vector<DictionaryContainer<Decimal128>>>,
     HashDecimalVectFuncProxy<Vector<DictionaryContainer<Decimal128>>>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<Decimal128>>, Decimal128>,
     FillOutputFuncImp<Vector<DictionaryContainer<Decimal128>>, Decimal128>},
    {OMNI_DATE32,
     DuplicateKeyValueImpl<Vector<int32_t>, int32_t>,
     HashFuncImpl<Vector<int32_t>, int32_t>,
     HashFuncVectImplProxy<Vector<int32_t>, int32_t>,
     IsSameNodeFuncImpl<Vector<int32_t>, int32_t>,
     FillOutputFuncImp<Vector<int32_t>, int32_t>,
     DuplicateKeyValueImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     HashFuncImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     HashFuncVectImplProxy<Vector<DictionaryContainer<int32_t>>, int32_t>,
     IsSameNodeFuncImpl<Vector<DictionaryContainer<int32_t>>, int32_t>,
     FillOutputFuncImp<Vector<DictionaryContainer<int32_t>>, int32_t>},
    {OMNI_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VARCHAR,
     DuplicateVarcharKeyValue,
     HashVarcharFuncImpl,
     HashVarcharVectFuncImplProxy,
     IsSameNodeFuncVarcharImpl,
     FillVarcharFuncImp,
     DuplicateVarcharKeyValue<Vector<DictionaryContainer<std::string_view>>>,
     HashVarcharFuncImpl<Vector<DictionaryContainer<std::string_view>>>,
     HashVarcharVectFuncImplProxy<Vector<DictionaryContainer<std::string_view>>>,
     IsSameNodeFuncVarcharImpl<Vector<DictionaryContainer<std::string_view>>>,
     nullptr},
    {OMNI_CHAR,
     DuplicateVarcharKeyValue,
     HashVarcharFuncImpl<Vector<LargeStringContainer<std::string_view>>>,
     HashVarcharVectFuncImplProxy,
     IsSameNodeFuncVarcharImpl<Vector<LargeStringContainer<std::string_view>>>,
     FillVarcharFuncImp,
     DuplicateVarcharKeyValue<Vector<DictionaryContainer<std::string_view>>>,
     HashVarcharFuncImpl<Vector<DictionaryContainer<std::string_view>>>,
     HashVarcharVectFuncImplProxy<Vector<DictionaryContainer<std::string_view>>>,
     IsSameNodeFuncVarcharImpl<Vector<DictionaryContainer<std::string_view>>>,
     nullptr},
    {OMNI_CONTAINER, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
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
        if (typeId >= type::OMNI_INVALID) {
            return nullptr;
        }

        if (DISTINCT_LIMIT_FUNC_SET[typeId].duplicateValueFunc == nullptr) {
            return nullptr;
        }
    }

    return new DistinctLimitOperator(sourceTypes, distinctCols, distinctColsCount, hashCol, limit);
}

void FillPrecomputedHash(VectorBatch *vectorBatch, const int32_t *inputTypeIds, const int32_t start, int32_t rowCount,
    uint64_t *combineHashVal, int32_t preComputedHashCol)
{
    switch (inputTypeIds[preComputedHashCol]) {
        case OMNI_INT:
        case OMNI_DATE32: {
            for (int i = 0; i < rowCount; ++i) {
                if (vectorBatch->Get(preComputedHashCol)->GetEncoding() != OMNI_DICTIONARY) {
                    combineHashVal[i] = static_cast<uint64_t>(
                        (dynamic_cast<Vector<int32_t> *>(vectorBatch->Get(preComputedHashCol)))->GetValue(start + i));
                } else {
                    combineHashVal[i] = static_cast<uint64_t>(
                        (dynamic_cast<Vector<DictionaryContainer<int32_t>> *>(vectorBatch->Get(preComputedHashCol)))
                            ->GetValue(start + i));
                }
            }
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            for (int i = 0; i < rowCount; ++i) {
                if (vectorBatch->Get(preComputedHashCol)->GetEncoding() != OMNI_DICTIONARY) {
                    combineHashVal[i] = static_cast<uint64_t>(
                        (dynamic_cast<Vector<int64_t> *>(vectorBatch->Get(preComputedHashCol)))->GetValue(start + i));
                } else {
                    combineHashVal[i] = static_cast<uint64_t>(
                        (dynamic_cast<Vector<DictionaryContainer<int64_t>> *>(vectorBatch->Get(preComputedHashCol)))
                            ->GetValue(start + i));
                }
            }
            break;
        }
        default:
            std::string omniExceptionInfo = "Error in FillPrecomputedHash, Invalid hash data type " +
                std::to_string(inputTypeIds[preComputedHashCol]) + " for precomputed hash column.";
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
    }
}

void GenerateCombinedHash(VectorBatch *vectorBatch, const int32_t start, int32_t rowCount, const int32_t *inputTypeIds,
    std::vector<int32_t> &distinctColumns, const int32_t distinctColNum, uint64_t *combineHashVal,
    int32_t preComputedHashCol)
{
    for (int i = 0; i < rowCount; ++i) {
        combineHashVal[i] = 0;
    }

    if (preComputedHashCol != DistinctLimitOperator::INVALID_DISTINCT_COL_ID) {
        FillPrecomputedHash(vectorBatch, inputTypeIds, start, rowCount, combineHashVal, preComputedHashCol);
        return;
    }

    int32_t colIndex;
    int32_t typeId;
    for (int32_t i = 0; i < distinctColNum; ++i) {
        colIndex = distinctColumns[i];
        typeId = inputTypeIds[colIndex];
        if (DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFuncVect == nullptr) {
            std::string omniExceptionInfo =
                "Error in GenerateCombinedHash, No such data type " + std::to_string(typeId);
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }

        if (vectorBatch->Get(colIndex)->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY) {
            DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFuncVectFromDict(vectorBatch->Get(colIndex), start, rowCount,
                combineHashVal);
        } else {
            DISTINCT_LIMIT_FUNC_SET[typeId].generateHashFuncVect(vectorBatch->Get(colIndex), start, rowCount,
                combineHashVal);
        }
    }
}

DistinctLimitOperator::DistinctLimitOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &distinctCols,
    int32_t distinctColsCount, int32_t hashCol, int64_t limit)
    : sourceTypes(sourceTypes),
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
        outputTypes.push_back(sourceDataType[colIndex]);
        outCols.push_back(colIndex);
    }

    if (hashCol != INVALID_DISTINCT_COL_ID) {
        outputTypes.push_back(sourceDataType[hashCol]);
        outCols.push_back(hashCol);
    }
}

DistinctLimitOperator::~DistinctLimitOperator() {}

bool IsExistSameRow(const type::DataTypes &inputTypes, VectorBatch *vectorBatch, std::vector<int32_t> &distinctCols,
    int32_t distinctColsCount, std::vector<std::vector<AggregateState>> &bucket, int rowIndex)
{
    bool isSame = true;
    int32_t columnId;
    int32_t typeId;
    BaseVector *inputVector;

    for (auto &rowVector : bucket) {
        isSame = true;

        for (int32_t column = 0; ((column < distinctColsCount) && isSame); ++column) {
            columnId = distinctCols[column];
            typeId = inputTypes.GetType(columnId)->GetId();
            inputVector = vectorBatch->Get(columnId);
            if ((rowVector[column].val == nullptr) || (inputVector->IsNull(rowIndex))) {
                isSame = ((rowVector[column].val == nullptr) && (inputVector->IsNull(rowIndex)));
                continue;
            }
            if (inputVector->GetEncoding() != OMNI_DICTIONARY) {
                DISTINCT_LIMIT_FUNC_SET[typeId].checkEqualFunc(inputVector, rowIndex, rowVector[column], isSame);
            } else {
                DISTINCT_LIMIT_FUNC_SET[typeId].checkEqualFuncFromDict(inputVector, rowIndex, rowVector[column],
                    isSame);
            }
        }

        // all distinct cols are same as bucket[i]
        if (isSame) {
            return true;
        }
    }

    return false;
}

void DistinctLimitOperator::FillDistinctedTuple(VectorBatch *vectorBatch, int rowIndex,
    std::vector<AggregateState> &tuple)
{
    const int32_t *vectorTypes = sourceTypes.GetIds();
    BaseVector *inputVector;

    for (int32_t colIndex = 0; colIndex < outColsCount; ++colIndex) {
        int32_t colId = outCols[colIndex];
        int32_t typeId = vectorTypes[colId];
        tuple[colIndex].val = nullptr;
        tuple[colIndex].count = 0;
        inputVector = vectorBatch->Get(colId);
        if (!(inputVector->IsNull(rowIndex))) {
            if (inputVector->GetEncoding() != vec::OMNI_DICTIONARY) {
                DISTINCT_LIMIT_FUNC_SET[typeId].duplicateValueFunc(tuple[colIndex], inputVector, rowIndex,
                    executionContext.get());
            } else {
                DISTINCT_LIMIT_FUNC_SET[typeId].duplicateValueFuncFromDict(tuple[colIndex], inputVector, rowIndex,
                    executionContext.get());
            }
        }
    }
}

void DistinctLimitOperator::InLoop(VectorBatch *vectorBatch, const int32_t rowCount, const uint64_t *combineHashVal)
{
    bool existed = false;
    uint64_t hashValue;
    int32_t pickedRows = 0;

    for (int32_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        hashValue = combineHashVal[rowIndex];
        auto &bucket = distinctedTable[hashValue];

        existed = IsExistSameRow(sourceTypes, vectorBatch, distinctCols, distinctColsCount, bucket, rowIndex);
        if (!existed) {
            std::vector<AggregateState> distinctedTuple(outColsCount);
            FillDistinctedTuple(vectorBatch, rowIndex, distinctedTuple);
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
        ResetInputVecBatch();
        return 0;
    }

    int32_t rowCount = vecBatch->GetRowCount();

    const int32_t *inputTypeIds = sourceTypes.GetIds();
    auto combineHashVal = std::make_unique<uint64_t[]>(rowCount);

    GenerateCombinedHash(vecBatch, 0, rowCount, inputTypeIds, distinctCols, distinctColsCount, combineHashVal.get(),
        this->hashCol);

    this->InLoop(vecBatch, rowCount, combineHashVal.get());
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    return 0;
}

void FillOutPutValue(VectorBatch *resultBatch, std::vector<AggregateState> &rowVector,
    std::vector<type::DataTypePtr> &outTypes, int32_t rowIndex)
{
    for (int i = 0; i < static_cast<int>(outTypes.size()); ++i) {
        // nullptr handle
        if (rowVector[i].val == nullptr) {
            BaseVector *resultCol = resultBatch->Get(i);
            if (outTypes[i]->GetId() == OMNI_VARCHAR || outTypes[i]->GetId() == OMNI_CHAR) {
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(resultCol)->SetNull(rowIndex);
            } else {
                resultCol->SetNull(rowIndex);
            }
            continue;
        }

        DISTINCT_LIMIT_FUNC_SET[outTypes[i]->GetId()].fillOutputFunc(resultBatch, rowVector, rowIndex, i);
    }
}

int32_t DistinctLimitOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (!distinctRowInfo.empty()) {
        resultBatch = new VectorBatch(distinctRowInfo.size());

        type::DataTypes outDataTypes(outputTypes);
        VectorHelper::AppendVectors(resultBatch, outDataTypes, resultBatch->GetRowCount());

        int32_t rowIndex = 0;
        for (auto item : distinctRowInfo) {
            std::vector<AggregateState> &rowVector = distinctedTable[item->hashValue][item->slotIndex];
            FillOutPutValue(resultBatch, rowVector, outputTypes, rowIndex++);
        }

        *outputVecBatch = resultBatch;
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
