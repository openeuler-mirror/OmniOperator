/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "dictionary_vector.h"
#include "int_vector.h"
#include "long_vector.h"

DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount)
    : Vector(dictionary, dictionary->GetSize(), 0), dictionary(dictionary), ids(ids), idsCount(idsCount), idsOffset(0)
{
    this->dictionary->GetReference()->IncRef();
}

int32_t DictionaryVector::GetInt(int32_t position)
{
    if (dictionary->GetType() != OMNI_VEC_TYPE_INT) {
        return -1;
    }
    return ((IntVector *)dictionary)->GetValue(ids[position]);
}

int64_t DictionaryVector::GetLong(int32_t position)
{
    if (dictionary->GetType() != OMNI_VEC_TYPE_LONG) {
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
