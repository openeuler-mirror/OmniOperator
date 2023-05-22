/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#include "topn_sort.h"
#include "operator/util/operator_util.h"
#include "type/data_type.h"

using namespace omniruntime::op;
using namespace omniruntime::type;

template <type::DataTypeId typeId> int64_t HashValueFromFlat(vec::BaseVector *vec, int32_t rowIdx)
{
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        auto value = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue((int8_t *)value.data(), value.length());
    } else if constexpr (typeId == OMNI_DECIMAL128) {
        auto value = static_cast<Vector<Decimal128> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue(value.LowBits(), value.HighBits());
    } else if constexpr (typeId == OMNI_DECIMAL64) {
        auto value = static_cast<Vector<int64_t> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashDecimal64Value(value);
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        auto value = static_cast<Vector<RawDataType> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue(value);
    }
}

template <type::DataTypeId typeId> int64_t HashValueFromDictionary(vec::BaseVector *vec, int32_t rowIdx)
{
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        auto value = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue((int8_t *)value.data(), value.length());
    } else if constexpr (typeId == OMNI_DECIMAL128) {
        auto value = static_cast<Vector<DictionaryContainer<Decimal128>> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue(value.LowBits(), value.HighBits());
    } else if constexpr (typeId == OMNI_DECIMAL64) {
        auto value = static_cast<Vector<DictionaryContainer<int64_t>> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashDecimal64Value(value);
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        auto value = static_cast<Vector<DictionaryContainer<RawDataType>> *>(vec)->GetValue(rowIdx);
        return HashUtil::HashValue(value);
    }
}

template <type::DataTypeId typeId>
bool EqualValueTemplate(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
    auto leftNull = leftVec->IsNull(leftPos);
    auto rightNull = rightVec->IsNull(rightPos);
    if (leftNull && rightNull) {
        return true;
    }
    if (leftNull || rightNull) {
        return false;
    }

    if constexpr (typeId == OMNI_CHAR || typeId == OMNI_VARCHAR) {
        std::string_view leftValue;
        std::string_view rightValue;
        if (leftVec->GetEncoding() == OMNI_DICTIONARY) {
            leftValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
        } else {
            leftValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
        }
        if (rightVec->GetEncoding() == OMNI_DICTIONARY) {
            rightValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
        } else {
            rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
        }
        auto leftLength = leftValue.length();
        auto rightLength = rightValue.length();
        if (leftLength != rightLength) {
            return false;
        } else {
            return memcmp(leftValue.data(), rightValue.data(), leftLength) == 0;
        }
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        RawDataType leftValue;
        RawDataType rightValue;
        if (leftVec->GetEncoding() == OMNI_DICTIONARY) {
            leftValue = static_cast<Vector<DictionaryContainer<RawDataType>> *>(leftVec)->GetValue(leftPos);
        } else {
            leftValue = static_cast<Vector<RawDataType> *>(leftVec)->GetValue(leftPos);
        }
        if (rightVec->GetEncoding() == OMNI_DICTIONARY) {
            rightValue = static_cast<Vector<DictionaryContainer<RawDataType>> *>(rightVec)->GetValue(rightPos);
        } else {
            rightValue = static_cast<Vector<RawDataType> *>(rightVec)->GetValue(rightPos);
        }

        if constexpr (typeId == OMNI_DOUBLE) {
            if (std::abs(leftValue - rightValue) < __DBL_EPSILON__) {
                return true;
            } else {
                return false;
            }
        } else {
            return leftValue == rightValue;
        }
    }
}

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
    if constexpr (typeId == OMNI_CHAR || typeId == OMNI_VARCHAR) {
        auto leftValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
        auto rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
        auto leftLength = leftValue.length();
        auto rightLength = rightValue.length();
        int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        }

        return leftLength - rightLength;
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        auto leftValue = static_cast<Vector<RawDataType> *>(leftVec)->GetValue(leftPos);
        auto rightValue = static_cast<Vector<RawDataType> *>(rightVec)->GetValue(rightPos);
        return leftValue > leftValue ? 1 : leftValue < leftValue ? -1 : 0;
    }
}

template <type::DataTypeId typeId>
static int32_t CompareValueFromDictionary(BaseVector *leftVec, int32_t leftPos, BaseVector *rightVec, int32_t rightPos)
{
    if constexpr (typeId == OMNI_CHAR || typeId == OMNI_VARCHAR) {
        auto leftValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(leftVec)->GetValue(leftPos);
        auto rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVec)->GetValue(rightPos);
        auto leftLength = leftValue.length();
        auto rightLength = rightValue.length();
        int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        }

        return leftLength - rightLength;
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        auto leftValue = static_cast<Vector<DictionaryContainer<RawDataType>> *>(leftVec)->GetValue(leftPos);
        auto rightValue = static_cast<Vector<RawDataType> *>(rightVec)->GetValue(rightPos);
        return leftValue > leftValue ? 1 : leftValue < leftValue ? -1 : 0;
    }
}

static std::vector<HashFunc> hashFromFlatFuncs = {
    nullptr,                            // OMNI_NONE,
    HashValueFromFlat<OMNI_INT>,        // OMNI_INT
    HashValueFromFlat<OMNI_LONG>,       // OMNI_LONG
    HashValueFromFlat<OMNI_DOUBLE>,     // OMNI_DOUBLE
    HashValueFromFlat<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    HashValueFromFlat<OMNI_SHORT>,      // OMNI_SHORT
    HashValueFromFlat<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    HashValueFromFlat<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    HashValueFromFlat<OMNI_DATE32>,     // OMNI_DATE32
    HashValueFromFlat<OMNI_DATE64>,     // OMNI_DATE64
    HashValueFromFlat<OMNI_TIME32>,     // OMNI_TIME32
    HashValueFromFlat<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                            // OMNI_TIMESTAMP
    nullptr,                            // OMNI_INTERVAL_MONTHS
    nullptr,                            // OMNI_INTERVAL_DAY_TIME
    HashValueFromFlat<OMNI_VARCHAR>,    // OMNI_VARCHAR
    HashValueFromFlat<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                             // OMNI_CONTAINER,
};

static std::vector<EqualFunc> equalValueFuncs = {
    nullptr,                             // OMNI_NONE,
    EqualValueTemplate<OMNI_INT>,        // OMNI_INT
    EqualValueTemplate<OMNI_LONG>,       // OMNI_LONG
    EqualValueTemplate<OMNI_DOUBLE>,     // OMNI_DOUBLE
    EqualValueTemplate<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    EqualValueTemplate<OMNI_SHORT>,      // OMNI_SHORT
    EqualValueTemplate<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    EqualValueTemplate<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    EqualValueTemplate<OMNI_DATE32>,     // OMNI_DATE32
    EqualValueTemplate<OMNI_DATE64>,     // OMNI_DATE64
    EqualValueTemplate<OMNI_TIME32>,     // OMNI_TIME32
    EqualValueTemplate<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                             // OMNI_TIMESTAMP
    nullptr,                             // OMNI_INTERVAL_MONTHS
    nullptr,                             // OMNI_INTERVAL_DAY_TIME
    EqualValueTemplate<OMNI_VARCHAR>,    // OMNI_VARCHAR
    EqualValueTemplate<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                              // OMNI_CONTAINER,
};

static std::vector<HashFunc> hashFromDictionaryFuncs = {
    nullptr,                                  // OMNI_NONE,
    HashValueFromDictionary<OMNI_INT>,        // OMNI_INT
    HashValueFromDictionary<OMNI_LONG>,       // OMNI_LONG
    HashValueFromDictionary<OMNI_DOUBLE>,     // OMNI_DOUBLE
    HashValueFromDictionary<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    HashValueFromDictionary<OMNI_SHORT>,      // OMNI_SHORT
    HashValueFromDictionary<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    HashValueFromDictionary<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    HashValueFromDictionary<OMNI_DATE32>,     // OMNI_DATE32
    HashValueFromDictionary<OMNI_DATE64>,     // OMNI_DATE64
    HashValueFromDictionary<OMNI_TIME32>,     // OMNI_TIME32
    HashValueFromDictionary<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                                  // OMNI_TIMESTAMP
    nullptr,                                  // OMNI_INTERVAL_MONTHS
    nullptr,                                  // OMNI_INTERVAL_DAY_TIME
    HashValueFromDictionary<OMNI_VARCHAR>,    // OMNI_VARCHAR
    HashValueFromDictionary<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                                   // OMNI_CONTAINER,
};

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
    CompareValueFromFlat<OMNI_VARCHAR>,    // OMNI_VARCHAR
    CompareValueFromFlat<OMNI_CHAR>,       // OMNI_CHAR,
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
    CompareValueFromDictionary<OMNI_VARCHAR>,    // OMNI_VARCHAR
    CompareValueFromDictionary<OMNI_CHAR>,       // OMNI_CHAR,
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
      sortColNum(sortCols.size()),
      partitionColNum(partitionCols.size())
{
    auto sourceTypeIds = sourceTypes.GetIds();
    for (size_t i = 0; i < sortColNum; i++) {
        auto type = sourceTypeIds[sortCols[i]];
        sortColTypes.emplace_back(type);
    }
    sortCompareFuncs.resize(sortColNum);
    partitionHashFuncs.resize(partitionColNum);

    executionContext = std::make_unique<op::ExecutionContext>();

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

void TopNSortOperator::Prepare(VectorBatch *inputVecBatch)
{
    auto sourceTypeIds = sourceTypes.GetIds();
    for (int32_t i = 0; i < partitionColNum; ++i) {
        auto partitionCol = partitionCols[i];
        auto curVector = inputVecBatch->Get(partitionCol);
        auto curTypeId = sourceTypeIds[partitionCol];
        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            partitionHashFuncs[i] = hashFromDictionaryFuncs[curTypeId];
        } else {
            partitionHashFuncs[i] = hashFromFlatFuncs[curTypeId];
        }
        partitionEqualFuncs[i] = equalValueFuncs[curTypeId];
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
}

void TopNSortOperator::InsertNewPartition(const PartitionKey &key, BaseVector **inputVectors, int32_t inputColNum,
    int32_t rowIdx, HashFunc *partitionHashFuncs, EqualFunc *partitionEqualFuncs)
{
    // construct partition key
    auto newPartitionVectors = new BaseVector *[partitionColNum];
    auto partitionVectors = key.partitionVectors;
    for (int32_t i = 0; i < partitionColNum; i++) {
        auto partitionCol = partitionCols[i];
        auto vector = createVectorFuncs[partitionCol](partitionVectors[i], rowIdx);
        newPartitionVectors[i] = vector;
    }

    // construct vector batch which has only one row
    auto singleRow = new VectorBatch(1);
    for (int32_t i = 0; i < inputColNum; i++) {
        auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
        singleRow->Append(vector);
    }
    auto value = new PartitionValue(n);
    value->vecBatches[0] = singleRow;
    value->currentSize = 1;

    PartitionKey newKey(partitionHashFuncs, partitionEqualFuncs, partitionColNum, newPartitionVectors, 0);
    partitionedMap[newKey] = value;
}

void TopNSortOperator::InsertNewValue(PartitionValue &value, BaseVector **inputVectors, int32_t inputColNum,
    BaseVector **sortVectors, int32_t rowIdx)
{
    auto vecBatches = value.vecBatches;
    auto lastPosition = value.currentSize - 1;

    // find insert position
    auto insertPos = FindInsertPosition(sortVectors, rowIdx, vecBatches, lastPosition);
    for (int32_t pos = value.currentSize; pos > insertPos; pos--) {
        vecBatches[pos] = vecBatches[pos - 1];
    }

    // construct vector batch which has only one row
    auto singleRow = new VectorBatch(1);
    for (int32_t i = 0; i < inputColNum; i++) {
        auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
        singleRow->Append(vector);
    }
    vecBatches[insertPos] = singleRow;
    value.currentSize++;
}

void TopNSortOperator::UpdatePartitionValue(PartitionValue &value, BaseVector **inputVectors, int32_t inputColNum,
    BaseVector **sortVectors, int32_t rowIdx)
{
    auto vecBatches = value.vecBatches;
    auto lastPosition = value.currentSize - 1;
    int32_t result = CompareForSortCols(sortVectors, rowIdx, vecBatches[lastPosition]);
    // if the value to be inserted is greater or equals to the last, then skip
    if (result >= 0) {
        return;
    }

    auto insertPos = FindInsertPosition(sortVectors, rowIdx, vecBatches, lastPosition - 1);
    for (int32_t pos = lastPosition; pos > insertPos; pos--) {
        vecBatches[pos] = vecBatches[pos - 1];
    }

    // directly write the values to be inserted into the insert position
    auto singleRow = vecBatches[insertPos];
    for (int32_t i = 0; i < inputColNum; i++) {
        updatePartitionValueFuncs[i](inputVectors[i], rowIdx, singleRow->Get(i), 0);
    }
}

int32_t TopNSortOperator::AddInput(VectorBatch *inputVecBatch)
{
    Prepare(inputVecBatch);

    BaseVector *partitionVectors[partitionColNum];
    for (int32_t i = 0; i < partitionColNum; ++i) {
        partitionVectors[i] = inputVecBatch->Get(partitionCols[i]);
    }
    BaseVector *sortVectors[sortColNum];
    for (int32_t i = 0; i < sortColNum; i++) {
        sortVectors[i] = inputVecBatch->Get(sortCols[i]);
    }

    auto &arenaAllocator = *(executionContext->GetArena());
    auto partitionHashFuncsPtr = partitionHashFuncs.data();
    auto partitionEqualFuncsPtr = partitionEqualFuncs.data();
    auto inputVectors = inputVecBatch->GetVectors();
    auto inputRowCount = inputVecBatch->GetRowCount();
    auto inputColNum = inputVecBatch->GetVectorCount();
    for (int32_t rowIdx = 0; rowIdx < inputRowCount; rowIdx++) {
        // check whether the key is inserted
        PartitionKey key(partitionHashFuncsPtr, partitionEqualFuncsPtr, partitionColNum, partitionVectors, rowIdx);
        auto keyPos = partitionedMap.find(key);
        if (keyPos == partitionedMap.end()) {
            // this is a new partition
            InsertNewPartition(key, inputVectors, inputColNum, rowIdx, partitionHashFuncsPtr, partitionEqualFuncsPtr);
        } else {
            // this is an existing partition
            auto value = keyPos->second;
            if (value->currentSize < n) {
                // case 1, the vecBatches is not full
                InsertNewValue(*value, inputVectors, inputColNum, sortVectors, rowIdx);
            } else {
                // case 2, the vecBatches is full
                UpdatePartitionValue(*value, inputVectors, inputColNum, sortVectors, rowIdx);
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
    std::unordered_map<PartitionKey, PartitionValue *, PartitionKeyHash, PartitionKeyEqual>::iterator mapPos;
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
        auto partitionValue = iter->second;
        if (partitionValue->currentSize == 0) {
            delete partitionValue;
            continue;
        }
        auto vecBatches = partitionValue->vecBatches;
        for (int32_t i = 0; i < partitionValue->currentSize; i++) {
            auto vecBatch = vecBatches[i];
            for (int32_t j = 0; j < outputColNum; j++) {
                setOutputValueFuncs[i](vecBatch->Get(i), 0, result->Get(i), resultRowIdx);
            }
            resultRowIdx++;
        }
        delete partitionValue;
    }
    *outputVecBatch = result;
    currentIter = end;
    return 0;
}

OmniStatus TopNSortOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
