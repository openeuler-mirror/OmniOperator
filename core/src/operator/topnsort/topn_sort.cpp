/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#include "topn_sort.h"
#include "operator/util/operator_util.h"
#include "type/data_type.h"

using namespace omniruntime::op;
using namespace omniruntime::type;

template <type::DataTypeId typeId>
void SetValueFromFlat(vec::BaseVector *inputVec, int32_t inputPos, vec::BaseVector *outputVec, int32_t outputPos)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    if (inputVec->IsNull(inputPos)) {
        if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
            static_cast<VarcharVector *>(outputVec)->SetNull(outputPos);
        } else {
            outputVec->SetNull(outputPos);
        }
        return;
    }

    using RawDataType = typename NativeAndVectorType<typeId>::type;
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        auto outputVarcharVec = static_cast<VarcharVector *>(outputVec);
        auto inputVarcharVec = static_cast<VarcharVector *>(inputVec);
        auto value = inputVarcharVec->GetValue(inputPos);
        outputVarcharVec->SetValue(outputPos, value);
    } else {
        auto value = static_cast<Vector<RawDataType> *>(inputVec)->GetValue(inputPos);
        static_cast<Vector<RawDataType> *>(outputVec)->SetValue(outputPos, value);
    }
}

template <type::DataTypeId typeId>
void SetValueFromDictionary(vec::BaseVector *inputVec, int32_t inputPos, vec::BaseVector *outputVec, int32_t outputPos)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    if (inputVec->IsNull(inputPos)) {
        if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
            static_cast<VarcharVector *>(outputVec)->SetNull(outputPos);
        } else {
            outputVec->SetNull(outputPos);
        }
        return;
    }

    using RawDataType = typename NativeAndVectorType<typeId>::type;
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        auto outputVarcharVec = static_cast<VarcharVector *>(outputVec);
        auto inputVarcharVec = static_cast<Vector<DictionaryContainer<std::string_view>> *>(inputVec);
        auto value = inputVarcharVec->GetValue(inputPos);
        outputVarcharVec->SetValue(outputPos, value);
    } else {
        auto value = static_cast<Vector<DictionaryContainer<RawDataType>> *>(inputVec)->GetValue(inputPos);
        static_cast<Vector<RawDataType> *>(outputVec)->SetValue(outputPos, value);
    }
}

template <type::DataTypeId typeId> static BaseVector *CreateVectorFromFlat(BaseVector *inputVec, int32_t inputPos)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    BaseVector *outputVec;
    if constexpr (typeId == OMNI_CHAR || typeId == OMNI_VARCHAR) {
        outputVec = new Vector<LargeStringContainer<std::string_view>>(1);
    } else {
        outputVec = new Vector<RawDataType>(1);
    }
    SetValueFromFlat<typeId>(inputVec, inputPos, outputVec, 0);
}

template <type::DataTypeId typeId> static BaseVector *CreateVectorFromDictionary(BaseVector *inputVec, int32_t inputPos)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    BaseVector *outputVec;
    if constexpr (typeId == OMNI_CHAR || typeId == OMNI_VARCHAR) {
        outputVec = new Vector<LargeStringContainer<std::string_view>>(1);
    } else {
        outputVec = new Vector<RawDataType>(1);
    }
    SetValueFromDictionary<typeId>(inputVec, inputPos, outputVec, 0);
}

template <type::DataTypeId typeId>
static int32_t CompareValueFromFlat(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        auto leftValue = static_cast<Vector<RawDataType> *>(leftVec)->GetValue(leftPos);
        auto rightValue = static_cast<Vector<RawDataType> *>(rightVec)->GetValue(rightPos);
        return leftValue > leftValue ? 1 : leftValue < leftValue ? -1 : 0;
}

template <type::DataTypeId typeId>
static int32_t CompareValueFromDictionary(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    auto leftValue = static_cast<Vector<DictionaryContainer<RawDataType>> *>(leftVec)->GetValue(leftPos);
    auto rightValue = static_cast<Vector<RawDataType> *>(rightVec)->GetValue(rightPos);
    return leftValue > leftValue ? 1 : leftValue < leftValue ? -1 : 0;
}

static int32_t CompareVarcharValueFromFlat(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
    auto leftValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
    auto rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
    auto leftLength = leftValue.length();
    auto rightLength = rightValue.length();
    int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
    if (result != 0) {
        return result;
    }

    return leftLength - rightLength;
}

static int32_t CompareVarcharValueFromDictionary(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
    auto leftValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
    auto rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
    auto leftLength = leftValue.length();
    auto rightLength = rightValue.length();
    int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
    if (result != 0) {
        return result;
    }

    return leftLength - rightLength;
}

static std::vector<CompareFunc> compareFromFlatFuncs = {
    nullptr,                               // OMNI_NONE,
    CompareValueFromFlat<OMNI_INT>,        // OMNI_INT
    CompareValueFromFlat<OMNI_LONG>,       // OMNI_LONG
    CompareValueFromFlat<OMNI_DOUBLE>,     // OMNI_DOUBLE
    CompareValueFromFlat<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    CompareValueFromFlat<OMNI_SHORT>,      // OMNI_SHORT
    CompareValueFromFlat<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    CompareValueFromFlat<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    CompareValueFromFlat<OMNI_DATE32>,     // OMNI_DATE32
    CompareValueFromFlat<OMNI_DATE64>,     // OMNI_DATE64
    CompareValueFromFlat<OMNI_TIME32>,     // OMNI_TIME32
    CompareValueFromFlat<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                               // OMNI_TIMESTAMP
    nullptr,                               // OMNI_INTERVAL_MONTHS
    nullptr,                               // OMNI_INTERVAL_DAY_TIME
    CompareVarcharValueFromFlat,           // OMNI_VARCHAR
    CompareVarcharValueFromFlat,           // OMNI_CHAR,
    nullptr                                // OMNI_CONTAINER,
};

static std::vector<CompareFunc> compareFromDictionaryFuncs = {
    nullptr,                                     // OMNI_NONE,
    CompareValueFromDictionary<OMNI_INT>,        // OMNI_INT
    CompareValueFromDictionary<OMNI_LONG>,       // OMNI_LONG
    CompareValueFromDictionary<OMNI_DOUBLE>,     // OMNI_DOUBLE
    CompareValueFromDictionary<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    CompareValueFromDictionary<OMNI_SHORT>,      // OMNI_SHORT
    CompareValueFromDictionary<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    CompareValueFromDictionary<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    CompareValueFromDictionary<OMNI_DATE32>,     // OMNI_DATE32
    CompareValueFromDictionary<OMNI_DATE64>,     // OMNI_DATE64
    CompareValueFromDictionary<OMNI_TIME32>,     // OMNI_TIME32
    CompareValueFromDictionary<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                                     // OMNI_TIMESTAMP
    nullptr,                                     // OMNI_INTERVAL_MONTHS
    nullptr,                                     // OMNI_INTERVAL_DAY_TIME
    CompareVarcharValueFromDictionary,           // OMNI_VARCHAR
    CompareVarcharValueFromDictionary,           // OMNI_CHAR,
    nullptr                                      // OMNI_CONTAINER,
};

static std::vector<CreateVectorFunc> createVectorFromFlatFuncs = {
    nullptr,                               // OMNI_NONE,
    CreateVectorFromFlat<OMNI_INT>,        // OMNI_INT
    CreateVectorFromFlat<OMNI_LONG>,       // OMNI_LONG
    CreateVectorFromFlat<OMNI_DOUBLE>,     // OMNI_DOUBLE
    CreateVectorFromFlat<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    CreateVectorFromFlat<OMNI_SHORT>,      // OMNI_SHORT
    CreateVectorFromFlat<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    CreateVectorFromFlat<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    CreateVectorFromFlat<OMNI_DATE32>,     // OMNI_DATE32
    CreateVectorFromFlat<OMNI_DATE64>,     // OMNI_DATE64
    CreateVectorFromFlat<OMNI_TIME32>,     // OMNI_TIME32
    CreateVectorFromFlat<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                               // OMNI_TIMESTAMP
    nullptr,                               // OMNI_INTERVAL_MONTHS
    nullptr,                               // OMNI_INTERVAL_DAY_TIME
    CreateVectorFromFlat<OMNI_VARCHAR>,    // OMNI_VARCHAR
    CreateVectorFromFlat<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                                // OMNI_CONTAINER,
};

static std::vector<CreateVectorFunc> createVectorFromDictionaryFuncs = {
    nullptr,                                     // OMNI_NONE,
    CreateVectorFromDictionary<OMNI_INT>,        // OMNI_INT
    CreateVectorFromDictionary<OMNI_LONG>,       // OMNI_LONG
    CreateVectorFromDictionary<OMNI_DOUBLE>,     // OMNI_DOUBLE
    CreateVectorFromDictionary<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    CreateVectorFromDictionary<OMNI_SHORT>,      // OMNI_SHORT
    CreateVectorFromDictionary<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    CreateVectorFromDictionary<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    CreateVectorFromDictionary<OMNI_DATE32>,     // OMNI_DATE32
    CreateVectorFromDictionary<OMNI_DATE64>,     // OMNI_DATE64
    CreateVectorFromDictionary<OMNI_TIME32>,     // OMNI_TIME32
    CreateVectorFromDictionary<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                                     // OMNI_TIMESTAMP
    nullptr,                                     // OMNI_INTERVAL_MONTHS
    nullptr,                                     // OMNI_INTERVAL_DAY_TIME
    CreateVectorFromDictionary<OMNI_VARCHAR>,    // OMNI_VARCHAR
    CreateVectorFromDictionary<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                                      // OMNI_CONTAINER,
};

static std::vector<SetValueFunc> setValueFromFlatFuncs = {
    nullptr,                           // OMNI_NONE,
    SetValueFromFlat<OMNI_INT>,        // OMNI_INT
    SetValueFromFlat<OMNI_LONG>,       // OMNI_LONG
    SetValueFromFlat<OMNI_DOUBLE>,     // OMNI_DOUBLE
    SetValueFromFlat<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    SetValueFromFlat<OMNI_SHORT>,      // OMNI_SHORT
    SetValueFromFlat<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    SetValueFromFlat<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    SetValueFromFlat<OMNI_DATE32>,     // OMNI_DATE32
    SetValueFromFlat<OMNI_DATE64>,     // OMNI_DATE64
    SetValueFromFlat<OMNI_TIME32>,     // OMNI_TIME32
    SetValueFromFlat<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                           // OMNI_TIMESTAMP
    nullptr,                           // OMNI_INTERVAL_MONTHS
    nullptr,                           // OMNI_INTERVAL_DAY_TIME
    SetValueFromFlat<OMNI_VARCHAR>,    // OMNI_VARCHAR
    SetValueFromFlat<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                            // OMNI_CONTAINER,
};

static std::vector<SetValueFunc> setValueFromDictionaryFuncs = {
    nullptr,                                 // OMNI_NONE,
    SetValueFromDictionary<OMNI_INT>,        // OMNI_INT
    SetValueFromDictionary<OMNI_LONG>,       // OMNI_LONG
    SetValueFromDictionary<OMNI_DOUBLE>,     // OMNI_DOUBLE
    SetValueFromDictionary<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    SetValueFromDictionary<OMNI_SHORT>,      // OMNI_SHORT
    SetValueFromDictionary<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    SetValueFromDictionary<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    SetValueFromDictionary<OMNI_DATE32>,     // OMNI_DATE32
    SetValueFromDictionary<OMNI_DATE64>,     // OMNI_DATE64
    SetValueFromDictionary<OMNI_TIME32>,     // OMNI_TIME32
    SetValueFromDictionary<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                                 // OMNI_TIMESTAMP
    nullptr,                                 // OMNI_INTERVAL_MONTHS
    nullptr,                                 // OMNI_INTERVAL_DAY_TIME
    SetValueFromDictionary<OMNI_VARCHAR>,    // OMNI_VARCHAR
    SetValueFromDictionary<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                                  // OMNI_CONTAINER,
};

TopNSortOperator::TopNSortOperator(const type::DataTypes &sourceTypes, int32_t n,
    const std::vector<int32_t> &partitionCols, const std::vector<int32_t> &sortCols,
    const std::vector<int32_t> &sortAscendings, const std::vector<int32_t> &sortNullFirsts)
    : sourceTypes(sourceTypes),
      n(n),
      partitionCols(partitionCols),
      sortCols(sortCols),
      sortAscendings(sortAscendings),
      sortNullFirsts(sortNullFirsts),
      sortColNum(sortCols.size())
{
    auto sourceTypeIds = sourceTypes.GetIds();
    for (size_t i = 0; i < sortColNum; i++) {
        auto type = sourceTypeIds[sortCols[i]];
        sortColTypes.emplace_back(type);
    }
    sortCompareFuncs.resize(sortColNum);

    executionContext = std::make_unique<op::ExecutionContext>();
    auto partitionColNum = partitionCols.size();
    serializers.resize(partitionColNum);
    deserializers.resize(partitionColNum);

    auto inputColNum = sourceTypes.GetSize();
    createVectorFuncs.resize(inputColNum);
    updatePartitionValueFuncs.resize(inputColNum);
    setOutputValueFuncs.resize(inputColNum);
    for (int32_t i = 0; i < inputColNum; i++) {
        setOutputValueFuncs[i] = setValueFromFlatFuncs[sourceTypeIds[i]];
    }

    int32_t eachRowSize = OperatorUtil::GetRowSize(sourceTypes.Get());
    maxRowCount = OperatorUtil::GetMaxRowCount(eachRowSize);
}

type::StringRef TopNSortOperator::GeneratePartitionKey(BaseVector **partitionVectors, int32_t partitionColNum,
    int32_t rowIdx, mem::SimpleArenaAllocator &arenaAllocator)
{
    type::StringRef key;
    for (int32_t colIdx = 0; colIdx < partitionColNum; colIdx++) {
        auto curVector = partitionVectors[colIdx];
        auto &curFunc = serializers[colIdx];
        curFunc(curVector, rowIdx, arenaAllocator, key);
    }
    return key;
}

int32_t TopNSortOperator::FindInsertPosition(BaseVector **insertSortVectors, int32_t insertRowIdx,
    VectorBatch **vecBatches, int32_t position)
{
    while (position >= 0) {
        auto left = vecBatches[position];
        // the value to bo inserted is less than the current position value, then go on
        int32_t result = CompareForSortCols(insertSortVectors, insertRowIdx, left);
        if (result >= 0) {
            break;
        }
        position--;
    }
    return position + 1;
}

int32_t TopNSortOperator::AddInput(omniruntime::vec::VectorBatch *inputVecBatch)
{
    auto sourceTypeIds = sourceTypes.GetIds();
    auto partitionColNum = static_cast<int32_t>(partitionCols.size());
    BaseVector *partitionVectors[partitionColNum];
    for (int32_t i = 0; i < partitionColNum; ++i) {
        auto partitionCol = partitionCols[i];
        auto curVector = inputVecBatch->Get(partitionCol);
        auto curTypeId = sourceTypeIds[partitionCol];
        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            serializers[i] = dicVectorSerializerCenter[curTypeId];
        } else {
            serializers[i] = vectorSerializerCenter[curTypeId];
        }
        deserializers[i] = vectorDeSerializerCenter[curTypeId];
        partitionVectors[i] = curVector;
    }

    for (int32_t i = 0; i < sortColNum; i++) {
        auto sortCol = sortCols[i];
        if (inputVecBatch->Get(sortCol)->GetEncoding() == OMNI_DICTIONARY) {
            sortCompareFuncs[i] = compareFromDictionaryFuncs[sourceTypeIds[sortCol]];
        } else {
            sortCompareFuncs[i] = compareFromFlatFuncs[sourceTypeIds[sortCol]];
        }
    }

    auto inputColNum = sourceTypes.GetSize();
    for (int32_t i = 0; i < inputColNum; i++) {
        if (inputVecBatch->Get(i)->GetEncoding() == OMNI_DICTIONARY) {
            createVectorFuncs[i] = createVectorFromDictionaryFuncs[i];
            updatePartitionValueFuncs[i] = setValueFromDictionaryFuncs[i];
        } else {
            createVectorFuncs[i] = createVectorFromFlatFuncs[i];
            updatePartitionValueFuncs[i] = setValueFromFlatFuncs[i];
        }
    }

    auto &arenaAllocator = *(executionContext->GetArena());
    auto sortColTypesPtr = sortColTypes.data();
    auto sortColsPtr = sortCols.data();
    auto sortCompareFuncsPtr = sortCompareFuncs.data();

    BaseVector *sortVectors[sortColNum];
    for (int32_t i = 0; i < sortColNum; i++) {
        sortVectors[i] = inputVecBatch->Get(sortColsPtr[i]);
    }
    auto inputRowCount = inputVecBatch->GetRowCount();
    for (int32_t rowIdx = 0; rowIdx < inputRowCount; rowIdx++) {
        // first, serialize partition row
        type::StringRef key = GeneratePartitionKey(partitionVectors, partitionColNum, rowIdx, arenaAllocator);

        // second, check whether the key is inserted
        auto keyPos = partitionedMap.find(key);
        if (keyPos == partitionedMap.end()) {
            // this is a new partition
            // construct vector batch which has only one row
            auto singleRow = new VectorBatch(1);
            for (int32_t i = 0; i < inputColNum; i++) {
                auto vector = createVectorFuncs[i](inputVecBatch->Get(i), rowIdx);
                singleRow->Append(vector);
            }
            auto value = new PartitionValue(sortColTypesPtr, sortColsPtr, sortCompareFuncsPtr, sortColNum, n);
            value->vecBatches[0] = singleRow;
            value->currentSize = 1;
            partitionedMap[key] = value;
        } else {
            // this is an existing partition
            auto value = keyPos->second;
            auto vecBatches = value->vecBatches;
            auto lastPosition = value->currentSize - 1;

            if (value->currentSize < n) {
                // case 1, the vecBatches is not full
                // find insert position
                auto insertPos = FindInsertPosition(sortVectors, rowIdx, vecBatches, lastPosition);
                for (int32_t pos = value->currentSize; pos > insertPos; pos--) {
                    vecBatches[pos] = vecBatches[pos - 1];
                }

                // construct vector batch which has only one row
                auto singleRow = new VectorBatch(1);
                for (int32_t i = 0; i < inputColNum; i++) {
                    auto vector = createVectorFuncs[i](inputVecBatch->Get(i), rowIdx);
                    singleRow->Append(vector);
                }
                vecBatches[insertPos] = singleRow;
                value->currentSize++;
            } else {
                // case 2, the vecBatches is full
                // if the value to be inserted is greater or equals to the last, then skip
                int32_t result = CompareForSortCols(sortVectors, rowIdx, vecBatches[lastPosition]);
                if (result >= 0) {
                    continue;
                }

                auto insertPos = FindInsertPosition(sortVectors, rowIdx, vecBatches, lastPosition - 1);
                for (int32_t pos = lastPosition; pos > insertPos; pos--) {
                    vecBatches[pos] = vecBatches[pos - 1];
                }

                // directly write the values to be inserted into the insert position
                auto singleRow = vecBatches[insertPos];
                for (int32_t i = 0; i < inputColNum; i++) {
                    updatePartitionValueFuncs[i](inputVecBatch->Get(i), rowIdx, singleRow->Get(i), 0);
                }
            }
        }
    }
    currentIter = partitionedMap.begin();

    VectorHelper::FreeVecBatch(inputVecBatch);
    return 0;
}

int32_t TopNSortOperator::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    int32_t outputRowCount = 0;
    std::unordered_map<type::StringRef, PartitionValue *, PartitionHash>::iterator mapPos;
    for (mapPos = currentIter; mapPos != partitionedMap.end(); ++mapPos) {
        auto value = mapPos->second;
        outputRowCount += value->currentSize;
        if (outputRowCount >= maxRowCount) {
            break;
        }
    }
    if (outputRowCount == 0) {
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }

    auto end = mapPos++;
    auto outputColNum = sourceTypes.GetSize();
    auto result = new VectorBatch(outputRowCount);
    int32_t resultRowIdx = 0;
    VectorHelper::AppendVectors(result, sourceTypes, outputRowCount);
    for (auto iter = currentIter; iter != end; ++iter) {
        auto &mapValue = iter->second;
        if (mapValue->currentSize == 0) {
            continue;
        }
        auto vecBatches = mapValue->vecBatches;
        for (int32_t i = 0; i < mapValue->currentSize; i++) {
            auto vecBatch = vecBatches[i];
            for (int32_t j = 0; j < outputColNum; j++) {
                setOutputValueFuncs[i](vecBatch->Get(i), 0, result->Get(i), resultRowIdx);
            }
            resultRowIdx++;
        }
    }
    *outputVecBatch = result;
    currentIter = end;
    return 0;
}

OmniStatus TopNSortOperator::Close()
{
    for (auto iter = partitionedMap.begin(); iter != partitionedMap.end(); ++iter) {
        auto value = iter->second;
        delete value;
    }
    return OMNI_STATUS_NORMAL;
}
