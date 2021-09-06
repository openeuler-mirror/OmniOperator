/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "dictionary_vector.h"
#include <memory>
#include "int_vector.h"
#include "long_vector.h"

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
    if (dictionaryType == OMNI_VEC_TYPE_INT) {
        return static_cast<IntVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetInt(ids[position]);
    } else {
        return -1;
    }
}

int64_t DictionaryVector::GetLong(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetType().GetId();
    if (dictionaryType == OMNI_VEC_TYPE_LONG) {
        return static_cast<LongVector *>(dictionary)->GetValue(ids[position]);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetLong(ids[position]);
    } else {
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
    return nullptr;
}

DictionaryVector *DictionaryVector::CopyRegion(int positionOffset, int length)
{
    return nullptr;
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