/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "dictionary_vector.h"
#include <memory>
#include <map>
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "boolean_vector.h"
#include "varchar_vector.h"
#include "decimal128_vector.h"

namespace omniruntime {
namespace vec {
DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, uint32_t idsCount)
    : dictionary(dictionary->Slice(0, dictionary->GetSize())), ids(nullptr), idsCount(idsCount), idsOffset(0)
{
    InitIds(ids, idsCount);
}

void DictionaryVector::InitIds(int32_t *ids, uint32_t idsCount)
{
    if (idsCount < INT32_MAX) {
        this->ids = new int32_t[idsCount];
        memcpy_s(this->ids, idsCount * sizeof(int32_t), ids, idsCount * sizeof(int32_t));
    }
}

int32_t DictionaryVector::GetInt(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_INT || dictionaryType == OMNI_VEC_TYPE_DATE32) {
        return static_cast<IntVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetInt(ids[position]);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

int64_t DictionaryVector::GetLong(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_LONG || dictionaryType == OMNI_VEC_TYPE_DECIMAL64) {
        return static_cast<LongVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetLong(ids[position]);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

double DictionaryVector::GetDouble(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_DOUBLE) {
        return static_cast<DoubleVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDouble(ids[position]);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

bool DictionaryVector::GetBoolean(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_BOOLEAN) {
        return static_cast<BooleanVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetBoolean(ids[position]);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

int32_t DictionaryVector::GetVarchar(int32_t position, uint8_t **dst) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_VARCHAR) {
        return static_cast<VarcharVector *>(dictionary)->GetValue(ids[position], dst);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetVarchar(ids[position], dst);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

Decimal128 DictionaryVector::GetDecimal128(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_DECIMAL128) {
        return static_cast<Decimal128Vector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDecimal128(ids[position]);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

DictionaryVector *DictionaryVector::Slice(int32_t positionOffset, int32_t length)
{
    auto dictionaryVector = new DictionaryVector(dictionary, ids + positionOffset, length);
    return dictionaryVector;
}

DictionaryVector *DictionaryVector::CopyPositions(const int *positions, int offset, int length)
{
    std::vector<int32_t> positionsToCopy;
    std::unordered_map<int32_t, int32_t> oldIndexToNewIndex;
    std::vector<int32_t> newIds;
    int32_t index = 0;
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i];
        int oldIndex = ids[position];
        if (oldIndexToNewIndex.find(oldIndex) == oldIndexToNewIndex.end()) { // not reuse index
            oldIndexToNewIndex[oldIndex] = index++;
            positionsToCopy.push_back(oldIndex);
        }
        newIds.push_back(oldIndexToNewIndex.find(oldIndex)->second);
    }

    Vector *newDictionary = dictionary->CopyPositions(positionsToCopy.data(), 0, index);
    auto *dictionaryVector = new DictionaryVector(newDictionary, newIds.data(), length);
    // dictionary method will slice for newDictionary,so need to free it
    delete newDictionary;
    return dictionaryVector;
}

DictionaryVector *DictionaryVector::CopyRegion(int positionOffset, int length)
{
    return new DictionaryVector(dictionary, ids + positionOffset, length);
}

void DictionaryVector::Append(Vector *other, int positionOffset, int length)
{
    DictionaryVector *otherVector = reinterpret_cast<DictionaryVector *>(other);
    if (positionOffset + length > idsCount) {
        return;
    }
    int32_t *destination = this->ids + positionOffset;
    int32_t *src = otherVector->GetPositionOffset() + otherVector->GetIds();
    errno_t ret = memcpy_s(destination, idsCount * sizeof(int32_t), src, length * sizeof(int32_t));

    if (ret != EOK) {
        std::cerr << "append failed in Dictionary vector." << std::endl;
    }
}
} // namespace vec
} // namespace omniruntime