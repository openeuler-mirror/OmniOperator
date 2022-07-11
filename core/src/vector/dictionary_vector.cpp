/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "dictionary_vector.h"
#include "variable_width_vector.h"
#include "fixed_width_vector.h"

namespace omniruntime {
namespace vec {
DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount)
    : DictionaryVector(dictionary->GetAllocator(), dictionary->GetTypeId(), idsCount)
{
    this->dictionary = dictionary->Slice(0, dictionary->GetSize());
    error_t ret = memcpy_s(valuesAddress, idsCount * sizeof(int32_t), ids, idsCount * sizeof(int32_t));
    if (ret != EOK) {
        LogError("Memory copy failed. %d", ret);
        delete this->dictionary;
        return;
    }

    for (int32_t i = 0; i < idsCount; i++) {
        if (dictionary->IsValueNull(ids[i]) ) {
            this->SetValueNull(i);
        }
    }
}

DictionaryVector::DictionaryVector(Vector *dictionary, int32_t idsCount)
    : DictionaryVector(dictionary->GetAllocator(), dictionary->GetTypeId(), idsCount)
{
    this->dictionary = dictionary->Slice(0, dictionary->GetSize());
}

DictionaryVector::DictionaryVector(VectorAllocator *allocator, int32_t dataTypeId, int32_t idsCount)
    : Vector(allocator, idsCount * sizeof(int32_t), idsCount, DataTypeId(dataTypeId)), dictionary(nullptr)
{}

DictionaryVector::~DictionaryVector()
{
    if (dictionary != nullptr) {
        delete dictionary;
    }
}

int32_t DictionaryVector::GetInt(int32_t position) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetInt(GetId(position));
    } else {
        return static_cast<IntVector *>(dictionary)->GetValue(GetId(position));
    }
}

int64_t DictionaryVector::GetLong(int32_t position) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetLong(GetId(position));
    } else {
        return static_cast<LongVector *>(dictionary)->GetValue(GetId(position));
    }
}

double DictionaryVector::GetDouble(int32_t position) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDouble(GetId(position));
    } else {
        return static_cast<DoubleVector *>(dictionary)->GetValue(GetId(position));
    }
}

bool DictionaryVector::GetBoolean(int32_t position) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetBoolean(GetId(position));
    } else {
        return static_cast<BooleanVector *>(dictionary)->GetValue(GetId(position));
    }
}

int32_t DictionaryVector::GetVarchar(int32_t position, uint8_t **dst) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetVarchar(GetId(position), dst);
    } else {
        return static_cast<VarcharVector *>(dictionary)->GetValue(GetId(position), dst);
    }
}

Decimal128 DictionaryVector::GetDecimal128(int32_t position) const
{
    VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
    if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDecimal128(GetId(position));
    } else {
        return static_cast<Decimal128Vector *>(dictionary)->GetValue(GetId(position));
    }
}

DictionaryVector *DictionaryVector::Slice(int32_t positionOffset, int32_t length)
{
    return new DictionaryVector(this, length, positionOffset);
}

DictionaryVector *DictionaryVector::CopyPositions(const int *positions, int offset, int length)
{
    auto *id = new int32_t[length];
    for (int i = 0; i < length; ++i) {
        id[i] = GetId(positions[offset + i]);
    }
    auto *vector = new DictionaryVector(this->dictionary, id, length);
    delete[] id;
    return vector;
}

DictionaryVector *DictionaryVector::CopyRegion(int positionOffset, int length)
{
    return new DictionaryVector(dictionary, GetIds() + positionOffset + this->positionOffset, length);
}

void DictionaryVector::Append(Vector *other, int positionOffset, int length)
{
    auto *otherVector = reinterpret_cast<DictionaryVector *>(other);
    if (positionOffset + length > size) {
        return;
    }
    int32_t *destination = GetIds() + positionOffset;
    int32_t *src = otherVector->GetIds() + otherVector->GetPositionOffset();
    errno_t ret = memcpy_s(destination, size * sizeof(int32_t), src, length * sizeof(int32_t));
    if (ret != EOK) {
        LogError("Append failed in Dictionary vector, ret:%d.", ret);
    }
}

Vector *DictionaryVector::ExtractDictionary()
{
    Vector *dictionary = this;
    int32_t positions[size];
    int32_t *preIds = nullptr;
    do {
        auto dictionaryVector = static_cast<DictionaryVector *>(dictionary);
        int32_t *currentIds = dictionaryVector->GetIds();
        dictionary = dictionaryVector->GetDictionary();
        for (int32_t i = 0; i < size; i++) {
            positions[i] = (preIds == nullptr) ? currentIds[i + positionOffset] : currentIds[preIds[i]];
        }
        preIds = positions;
    } while (dictionary->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY);
    return dictionary->CopyPositions(positions, 0, size);
}

Vector *DictionaryVector::ExtractDictionary(const int32_t *positions, int32_t length)
{
    ASSERT((positions != nullptr) && (length <= size));
    Vector *dictionary = this;
    int32_t newPositions[length];
    int32_t *preIds = nullptr;
    do {
        auto dictionaryVector = static_cast<DictionaryVector *>(dictionary);
        int32_t *currentIds = dictionaryVector->GetIds();
        dictionary = dictionaryVector->GetDictionary();
        for (int32_t i = 0; i < length; i++) {
            newPositions[i] = (preIds == nullptr) ? currentIds[positions[i] + positionOffset] : currentIds[preIds[i]];
        }
        preIds = newPositions;
    } while (dictionary->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY);
    return dictionary->CopyPositions(preIds, 0, length);
}

Vector *DictionaryVector::ExtractDictionaryAndIds(int32_t positionOffset, int32_t length, int32_t *originalIds)
{
    ASSERT((originalIds != nullptr) && (positionOffset + length <= size));
    Vector *dictionary = this;
    int32_t *preIds = nullptr;
    do {
        auto dictionaryVector = static_cast<DictionaryVector *>(dictionary);
        int32_t *currentIds = dictionaryVector->GetIds();
        dictionary = dictionaryVector->GetDictionary();
        for (int32_t i = 0; i < length; i++) {
            originalIds[i] =
                (preIds == nullptr) ? currentIds[i + positionOffset + this->positionOffset] : currentIds[preIds[i]];
        }
        preIds = originalIds;
    } while (dictionary->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY);
    return dictionary;
}
} // namespace vec
} // namespace omniruntime