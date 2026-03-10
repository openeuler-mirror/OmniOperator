/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry dictionary functions
 */

#include "dictionaryfunctions.h"
#include "vector/vector.h"
#include "codegen/context_helper.h"

using namespace omniruntime::vec;
using namespace std;

namespace omniruntime::codegen::function {
extern DLLEXPORT int32_t GetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<int32_t> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<int32_t>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT int8_t GetByteFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<int8_t> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<int8_t>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT int16_t GetShortFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<int16_t> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<int16_t>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT int64_t GetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<int64_t> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<int64_t>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT double GetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<double> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<double>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

DLLEXPORT float GetFloatFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<float> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
            reinterpret_cast<vec::Vector<vec::DictionaryContainer<float>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT bool GetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        return reinterpret_cast<ConstVector<bool> *>(dictionaryVectorAddr)->GetConstValue();
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<bool>> *>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetValue(index);
}

extern DLLEXPORT uint8_t *GetVarcharFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index,
    int32_t *lengthPtr)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        auto constVec = reinterpret_cast<ConstVector<std::string_view> *>(dictionaryVectorAddr);
        auto sv = constVec->GetConstValue();
        *lengthPtr = static_cast<int32_t>(sv.length());
        return reinterpret_cast<uint8_t *>(const_cast<char *>(sv.data()));
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view>> *>(dictionaryVectorAddr);
    auto stringView = dictionaryVectorPtr->GetValue(index);
    int32_t length = stringView.length();
    *lengthPtr = length;
    return (uint8_t *)stringView.data();
}

extern DLLEXPORT void GetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    auto baseVec = reinterpret_cast<BaseVector *>(dictionaryVectorAddr);
    if (baseVec->GetEncoding() == OMNI_ENCODING_CONST) {
        auto constVec = reinterpret_cast<ConstVector<type::Decimal128> *>(dictionaryVectorAddr);
        auto value = constVec->GetConstValue();
        *outLowPtr = value.LowBits();
        *outHighPtr = value.HighBits();
        return;
    }
    auto dictionaryVectorPtr =
        reinterpret_cast<vec::Vector<vec::DictionaryContainer<type::Decimal128>> *>(dictionaryVectorAddr);
    auto value = dictionaryVectorPtr->GetValue(index);
    *outLowPtr = value.LowBits();
    *outHighPtr = value.HighBits();
}
}