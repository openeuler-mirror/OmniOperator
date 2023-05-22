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
void UpdateValueFromFlat(vec::BaseVector *inputVec, int32_t inputPos, vec::BaseVector *outputVec, int32_t outputPos)
{
    auto inputNull = inputVec->IsNull(inputPos);
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
        auto outputVarcharVec = static_cast<VarcharVector *>(outputVec);
        auto inputVarcharVec = static_cast<VarcharVector *>(inputVec);
        if (inputNull) {
            outputVarcharVec->SetNull(outputPos);
        } else {
            outputVarcharVec->SetNotNull(outputPos);
            auto value = inputVarcharVec->GetValue(inputPos);
            outputVarcharVec->SetValue(outputPos, value);
        }
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        if (inputNull) {
            static_cast<Vector<RawDataType> *>(outputVec)->SetNull(outputPos);
        } else {
            static_cast<Vector<RawDataType> *>(outputVec)->SetNotNull(outputPos);
            auto value = static_cast<Vector<RawDataType> *>(inputVec)->GetValue(inputPos);
            static_cast<Vector<RawDataType> *>(outputVec)->SetValue(outputPos, value);
        }
    }
}

template <type::DataTypeId typeId>
void UpdateValueFromDictionary(vec::BaseVector *inputVec, int32_t inputPos, vec::BaseVector *outputVec,
    int32_t outputPos)
{
    auto inputNull = inputVec->IsNull(inputPos);
    if constexpr (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
        using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
        auto outputVarcharVec = static_cast<VarcharVector *>(outputVec);
        auto inputVarcharVec = static_cast<Vector<DictionaryContainer<std::string_view>> *>(inputVec);
        if (inputNull) {
            outputVarcharVec->SetNull(outputPos);
        } else {
            outputVarcharVec->SetNotNull(outputPos);
            auto value = inputVarcharVec->GetValue(inputPos);
            outputVarcharVec->SetValue(outputPos, value);
        }
    } else {
        using RawDataType = typename NativeAndVectorType<typeId>::type;
        if (inputNull) {
            static_cast<Vector<RawDataType> *>(outputVec)->SetNull(outputPos);
        } else {
            static_cast<Vector<RawDataType> *>(outputVec)->SetNotNull(outputPos);
            auto value = static_cast<Vector<DictionaryContainer<RawDataType>> *>(inputVec)->GetValue(inputPos);
            static_cast<Vector<RawDataType> *>(outputVec)->SetValue(outputPos, value);
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
    return outputVec;
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
    return outputVec;
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
        return leftValue > rightValue ? 1 : leftValue < rightValue ? -1 : 0;
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
        return leftValue > rightValue ? 1 : leftValue < rightValue ? -1 : 0;
    }
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

static std::vector<EqualFunc> equalFromFlatFuncs = {
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

static std::vector<UpdateValueFunc> updateValueFromFlatFuncs = {
    nullptr,                              // OMNI_NONE,
    UpdateValueFromFlat<OMNI_INT>,        // OMNI_INT
    UpdateValueFromFlat<OMNI_LONG>,       // OMNI_LONG
    UpdateValueFromFlat<OMNI_DOUBLE>,     // OMNI_DOUBLE
    UpdateValueFromFlat<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    UpdateValueFromFlat<OMNI_SHORT>,      // OMNI_SHORT
    UpdateValueFromFlat<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    UpdateValueFromFlat<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    UpdateValueFromFlat<OMNI_DATE32>,     // OMNI_DATE32
    UpdateValueFromFlat<OMNI_DATE64>,     // OMNI_DATE64
    UpdateValueFromFlat<OMNI_TIME32>,     // OMNI_TIME32
    UpdateValueFromFlat<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                              // OMNI_TIMESTAMP
    nullptr,                              // OMNI_INTERVAL_MONTHS
    nullptr,                              // OMNI_INTERVAL_DAY_TIME
    UpdateValueFromFlat<OMNI_VARCHAR>,    // OMNI_VARCHAR
    UpdateValueFromFlat<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                               // OMNI_CONTAINER,
};

static std::vector<UpdateValueFunc> updateValueFromDictionaryFuncs = {
    nullptr,                                    // OMNI_NONE,
    UpdateValueFromDictionary<OMNI_INT>,        // OMNI_INT
    UpdateValueFromDictionary<OMNI_LONG>,       // OMNI_LONG
    UpdateValueFromDictionary<OMNI_DOUBLE>,     // OMNI_DOUBLE
    UpdateValueFromDictionary<OMNI_BOOLEAN>,    // OMNI_BOOLEAN
    UpdateValueFromDictionary<OMNI_SHORT>,      // OMNI_SHORT
    UpdateValueFromDictionary<OMNI_DECIMAL64>,  // OMNI_DECIMAL64,
    UpdateValueFromDictionary<OMNI_DECIMAL128>, // OMNI_DECIMAL128
    UpdateValueFromDictionary<OMNI_DATE32>,     // OMNI_DATE32
    UpdateValueFromDictionary<OMNI_DATE64>,     // OMNI_DATE64
    UpdateValueFromDictionary<OMNI_TIME32>,     // OMNI_TIME32
    UpdateValueFromDictionary<OMNI_TIME64>,     // OMNI_TIME64
    nullptr,                                    // OMNI_TIMESTAMP
    nullptr,                                    // OMNI_INTERVAL_MONTHS
    nullptr,                                    // OMNI_INTERVAL_DAY_TIME
    UpdateValueFromDictionary<OMNI_VARCHAR>,    // OMNI_VARCHAR
    UpdateValueFromDictionary<OMNI_CHAR>,       // OMNI_CHAR,
    nullptr                                     // OMNI_CONTAINER,
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

TopNSortOperator::TopNSortOperator(const type::DataTypes &sourceTypes, int32_t n, bool isStrictTopN,
    const std::vector<int32_t> &partitionCols, const std::vector<int32_t> &sortCols,
    const std::vector<int32_t> &sortAscendings, const std::vector<int32_t> &sortNullFirsts)
    : sourceTypes(sourceTypes),
      n(n),
      isStrictTopN(isStrictTopN),
      partitionCols(partitionCols),
      partitionColNum(static_cast<int32_t>(partitionCols.size())),
      sortCols(sortCols),
      sortAscendings(sortAscendings),
      sortNullFirsts(sortNullFirsts),
      sortColNum(static_cast<int32_t>(sortCols.size()))
{
    auto sourceTypeIds = sourceTypes.GetIds();
    for (int32_t i = 0; i < sortColNum; i++) {
        auto type = sourceTypeIds[sortCols[i]];
        sortColTypes.emplace_back(type);
    }
    sortCompareFuncs.resize(sortColNum);
    executionContext = std::make_unique<op::ExecutionContext>();
    serializers.resize(partitionColNum);

    auto inputColNum = sourceTypes.GetSize();
    equalFuncs.resize(inputColNum);
    createVectorFuncs.resize(inputColNum);
    updatePartitionValueFuncs.resize(inputColNum);
    setOutputValueFuncs.resize(inputColNum);
    for (int32_t i = 0; i < inputColNum; i++) {
        setOutputValueFuncs[i] = setValueFromFlatFuncs[sourceTypeIds[i]];
    }

    int32_t eachRowSize = OperatorUtil::GetRowSize(sourceTypes.Get());
    maxRowCount = OperatorUtil::GetMaxRowCount(eachRowSize);
    outputTypes = sourceTypes.Get();
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

int32_t TopNSortOperator::FindInsertPositionWhenNotFull(BaseVector **insertSortVectors, int32_t insertRowIdx,
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

int32_t TopNSortOperator::FindInsertPositionWhenFull(BaseVector **insertSortVectors, int32_t insertRowIdx,
    VectorBatch **vecBatches, int32_t position, bool &needAppend)
{
    while (position >= 0) {
        auto left = vecBatches[position];
        // the value to bo inserted is less than the current position value, then go on
        int32_t result = CompareForSortCols(insertSortVectors, insertRowIdx, left);
        if (result == 0) {
            needAppend = true;
            break;
        } else if (result > 0) {
            needAppend = false;
            break;
        } else {
            position--;
        }
    }
    return position + 1;
}

void TopNSortOperator::Prepare(BaseVector **inputVectors, int32_t inputColNum)
{
    auto sourceTypeIds = sourceTypes.GetIds();
    for (int32_t i = 0; i < partitionColNum; ++i) {
        auto partitionCol = partitionCols[i];
        auto curVector = inputVectors[partitionCol];
        auto curTypeId = sourceTypeIds[partitionCol];
        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            serializers[i] = dicVectorSerializerCenter[curTypeId];
        } else {
            serializers[i] = vectorSerializerCenter[curTypeId];
        }
    }

    for (int32_t i = 0; i < sortColNum; i++) {
        auto sortCol = sortCols[i];
        auto curTypeId = sourceTypeIds[sortCol];
        if (inputVectors[sortCol]->GetEncoding() == OMNI_DICTIONARY) {
            sortCompareFuncs[i] = compareFromDictionaryFuncs[curTypeId];
            equalFuncs[i] = equalFromFlatFuncs[curTypeId];
        } else {
            sortCompareFuncs[i] = compareFromFlatFuncs[curTypeId];
            equalFuncs[i] = equalFromFlatFuncs[curTypeId];
        }
    }

    for (int32_t i = 0; i < inputColNum; i++) {
        auto curTypeId = sourceTypeIds[i];
        if (inputVectors[i]->GetEncoding() == OMNI_DICTIONARY) {
            createVectorFuncs[i] = createVectorFromDictionaryFuncs[curTypeId];
            updatePartitionValueFuncs[i] = updateValueFromDictionaryFuncs[curTypeId];
        } else {
            createVectorFuncs[i] = createVectorFromFlatFuncs[curTypeId];
            updatePartitionValueFuncs[i] = updateValueFromFlatFuncs[curTypeId];
        }
    }
}

void TopNSortOperator::InsertNewPartition(StringRef &key, BaseVector **inputVectors, int32_t inputColNum,
    int32_t rowIdx)
{
    // construct vector batch which has only one row
    auto singleRow = new VectorBatch(1);
    for (int32_t i = 0; i < inputColNum; i++) {
        auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
        singleRow->Append(vector);
    }
    auto value = new PartitionValue(sortColNum, n);
    value->vecBatches[0] = singleRow;
    value->nextIndex = 1;
    partitionedMap[key] = value;
}

void TopNSortOperator::InsertNewValue(PartitionValue &value, BaseVector **inputVectors, int32_t inputColNum,
    BaseVector **sortVectors, int32_t rowIdx)
{
    auto vecBatches = value.vecBatches;
    auto vecBatchSize = value.nextIndex;
    auto lastPosition = vecBatchSize - 1;

    // find insert position
    auto insertPos = FindInsertPositionWhenNotFull(sortVectors, rowIdx, vecBatches, lastPosition);
    for (int32_t pos = vecBatchSize; pos > insertPos; pos--) {
        vecBatches[pos] = vecBatches[pos - 1];
    }

    // construct vector batch which has only one row
    auto singleRow = new VectorBatch(1);
    for (int32_t i = 0; i < inputColNum; i++) {
        auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
        singleRow->Append(vector);
    }
    vecBatches[insertPos] = singleRow;
    value.nextIndex++;
}

void TopNSortOperator::UpdatePartitionValue(PartitionValue &value, BaseVector **inputVectors, int32_t inputColNum,
    BaseVector **sortVectors, int32_t rowIdx)
{
    auto vecBatches = value.vecBatches;

    // compare the last value
    int32_t result = CompareForSortCols(sortVectors, rowIdx, vecBatches[n - 1]);
    if (result > 0) {
        // if the value to be inserted is greater or equals to the last, then skip
        return;
    }
    if (result == 0) {
        // for rank function, if the value is same, we should append
        auto singleRow = new VectorBatch(1);
        for (int32_t i = 0; i < inputColNum; i++) {
            auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
            singleRow->Append(vector);
        }
        vecBatches[value.nextIndex] = singleRow;
        value.nextIndex++;
    } else {
        auto singleRow = new VectorBatch(1);
        for (int32_t i = 0; i < inputColNum; i++) {
            auto vector = createVectorFuncs[i](inputVectors[i], rowIdx);
            singleRow->Append(vector);
        }

        auto insertPos = FindInsertPositionWhenNotFull(sortVectors, rowIdx, vecBatches, n - 2);
        for (int32_t pos = value.nextIndex; pos > insertPos; pos--) {
            vecBatches[pos] = vecBatches[pos - 1];
        }
        vecBatches[insertPos] = singleRow;
        value.nextIndex++;
        if (insertPos == (n - 1)) {
            value.nextIndex = n;
        } else {
            // check the nth and (n-1)th values are equal
            auto isDistinct = CheckDistinctForLast(vecBatches[n - 1], vecBatches[n]);
            if (isDistinct) {
                value.nextIndex = n;
            }
        }
    }
}

int32_t TopNSortOperator::AddInput(omniruntime::vec::VectorBatch *inputVecBatch)
{
    auto inputVectors = inputVecBatch->GetVectors();
    auto inputColNum = sourceTypes.GetSize();
    Prepare(inputVectors, inputColNum);

    BaseVector *partitionVectors[partitionColNum];
    for (int32_t i = 0; i < partitionColNum; ++i) {
        partitionVectors[i] = inputVectors[partitionCols[i]];
    }
    BaseVector *sortVectors[sortColNum];
    for (int32_t i = 0; i < sortColNum; i++) {
        sortVectors[i] = inputVectors[sortCols[i]];
    }

    auto &arenaAllocator = *(executionContext->GetArena());
    auto inputRowCount = inputVecBatch->GetRowCount();
    for (int32_t rowIdx = 0; rowIdx < inputRowCount; rowIdx++) {
        // first, serialize partition row
        type::StringRef key = GeneratePartitionKey(partitionVectors, partitionColNum, rowIdx, arenaAllocator);

        // second, check whether the key is inserted
        auto keyPos = partitionedMap.find(key);
        if (keyPos == partitionedMap.end()) {
            // this is a new partition
            InsertNewPartition(key, inputVectors, inputColNum, rowIdx);
        } else {
            arenaAllocator.RollBackContinualMem();

            // this is an existing partition
            auto value = keyPos->second;
            if (value->nextIndex < n) {
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
    std::unordered_map<type::StringRef, PartitionValue *, PartitionHash>::iterator mapPos;
    auto mapEnd = partitionedMap.end();
    for (mapPos = currentIter; mapPos != mapEnd; ++mapPos) {
        auto value = mapPos->second;
        outputRowCount += value->nextIndex;
        if (outputRowCount >= maxRowCount) {
            break;
        }
    }
    if (outputRowCount == 0) {
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }

    std::unordered_map<type::StringRef, PartitionValue *, PartitionHash>::iterator end;
    if (mapPos == mapEnd) {
        end = mapEnd;
        SetStatus(OMNI_STATUS_FINISHED);
    } else {
        end = mapPos++;
    }

    auto outputColNum = sourceTypes.GetSize();
    auto result = new VectorBatch(outputRowCount);
    int32_t resultRowIdx = 0;
    VectorHelper::AppendVectors(result, sourceTypes, outputRowCount);
    for (auto iter = currentIter; iter != end; ++iter) {
        auto mapValue = iter->second;
        auto vecBatches = mapValue->vecBatches;
        auto vecBatchSize = mapValue->nextIndex;
        for (int32_t i = 0; i < vecBatchSize; i++) {
            auto vecBatch = vecBatches[i];
            for (int32_t j = 0; j < outputColNum; j++) {
                setOutputValueFuncs[j](vecBatch->Get(j), 0, result->Get(j), resultRowIdx);
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
    auto end = partitionedMap.end();
    for (auto iter = partitionedMap.begin(); iter != end; iter++) {
        auto mapValue = iter->second;
        delete mapValue;
    }
    return OMNI_STATUS_NORMAL;
}
