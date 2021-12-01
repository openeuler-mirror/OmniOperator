/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "dictionary_vector.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "boolean_vector.h"
#include "varchar_vector.h"
#include "decimal128_vector.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"
namespace omniruntime {
namespace vec {
DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount)
    : DictionaryVector(dictionary->GetAllocator(), idsCount)
{
    this->dictionary = dictionary->Slice(0, dictionary->GetSize());
    memcpy_s(valuesAddress, idsCount * sizeof(int32_t), ids, idsCount * sizeof(int32_t));
    bool *nulls = new bool[idsCount];
    for (int32_t i = 0; i < idsCount; i++) {
        nulls[i] = dictionary->IsValueNull(ids[i]);
    }
    memcpy_s(valueNullsAddress, idsCount * sizeof(bool), nulls, idsCount * sizeof(bool));
    delete[] nulls;
}

DictionaryVector::DictionaryVector(Vector *dictionary, int32_t idsCount)
    : DictionaryVector(dictionary->GetAllocator(), idsCount)
{
    this->dictionary = dictionary->Slice(0, dictionary->GetSize());
}

DictionaryVector::DictionaryVector(VectorAllocator *allocator, int32_t idsCount)
    : FixedWidthVector<int32_t>(allocator, idsCount * sizeof(int32_t), idsCount, DictionaryVecType::Instance()),
      dictionary(nullptr)
{}

DictionaryVector::~DictionaryVector()
{
    if (dictionary != nullptr) {
        delete dictionary;
    }
}

int32_t DictionaryVector::GetInt(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_INT || dictionaryType == OMNI_VEC_TYPE_DATE32) {
        return static_cast<IntVector *>(dictionary)->GetValue(GetId(position));
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetInt(GetId(position));
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

int64_t DictionaryVector::GetLong(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_LONG || dictionaryType == OMNI_VEC_TYPE_DECIMAL64) {
        return static_cast<LongVector *>(dictionary)->GetValue(GetId(position));
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetLong(GetId(position));
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

double DictionaryVector::GetDouble(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_DOUBLE) {
        return static_cast<DoubleVector *>(dictionary)->GetValue(GetId(position));
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDouble(GetId(position));
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

bool DictionaryVector::GetBoolean(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_BOOLEAN) {
        return static_cast<BooleanVector *>(dictionary)->GetValue(GetId(position));
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetBoolean(GetId(position));
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

int32_t DictionaryVector::GetVarchar(int32_t position, uint8_t **dst) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_VARCHAR || dictionaryType == OMNI_VEC_TYPE_CHAR) {
        return static_cast<VarcharVector *>(dictionary)->GetValue(GetId(position), dst);
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetVarchar(GetId(position), dst);
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
    }
}

Decimal128 DictionaryVector::GetDecimal128(int32_t position) const
{
    VecTypeId dictionaryType = dictionary->GetTypeId();
    if (dictionaryType == OMNI_VEC_TYPE_DECIMAL128) {
        return static_cast<Decimal128Vector *>(dictionary)->GetValue(GetId(position));
    } else if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
        return static_cast<DictionaryVector *>(dictionary)->GetDecimal128(GetId(position));
    } else {
        std::cerr << "unsupported type:" << dictionaryType << std::endl;
        return -1;
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
    return new DictionaryVector(dictionary, GetIds() + positionOffset, length);
}

void DictionaryVector::Append(Vector *other, int positionOffset, int length)
{
    DictionaryVector *otherVector = reinterpret_cast<DictionaryVector *>(other);
    if (positionOffset + length > size) {
        return;
    }
    int32_t *destination = GetIds() + positionOffset;
    int32_t *src = otherVector->GetIds() + otherVector->GetPositionOffset();
    errno_t ret = memcpy_s(destination, size * sizeof(int32_t), src, length * sizeof(int32_t));

    if (ret != EOK) {
        std::cerr << "append failed in Dictionary vector." << std::endl;
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
    } while (dictionary->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY);
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
    } while (dictionary->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY);
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
    } while (dictionary->GetTypeId() == OMNI_VEC_TYPE_DICTIONARY);
    return dictionary;
}

void DictionaryVector::SetValues(int startIndex, const int32_t *values, int length) {}
} // namespace vec
} // namespace omniruntime