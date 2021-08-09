/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "dictionary_vector.h"
#include "int_vector.h"
#include "long_vector.h"

namespace omniruntime {
namespace vec {
DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, uint32_t idsCount)
    : Vector(dictionary, dictionary->GetSize(), 0), dictionary(dictionary), ids(ids), idsCount(idsCount), idsOffset(0)
{
    InitIds(ids, idsCount, dictionary->GetSize());
}

void DictionaryVector::InitIds(int32_t *ids, uint32_t idsCount, uint32_t maxIdsCount)
{
    if (idsCount <= maxIdsCount) {
        this->ids = new int32_t[idsCount];
        memcpy_s(this->ids, idsCount * sizeof(int32_t), ids, idsCount * sizeof(int32_t));
    }
}

int32_t DictionaryVector::GetInt(int32_t position)
{
    if (dictionary->GetType().GetId() != OMNI_VEC_TYPE_INT) {
        return -1;
    }
    return ((IntVector *)dictionary)->GetValue(ids[position]);
}

int64_t DictionaryVector::GetLong(int32_t position)
{
    if (dictionary->GetType().GetId() != OMNI_VEC_TYPE_LONG) {
        return -1;
    }
    return ((LongVector *)dictionary)->GetValue(ids[position]);
}

DictionaryVector *DictionaryVector::Slice(int positionOffset, int length)
{
    return nullptr;
}

DictionaryVector *DictionaryVector::CopyPositions(const int *positions, int offset, int length)
{
    return nullptr;
}

DictionaryVector *DictionaryVector::CopyRegion(int positionOffset, int length)
{
    return nullptr;
}

void DictionaryVector::Append(Vector *other, int positionOffset, int length) {}
} // namespace vec
} // namespace omniruntime