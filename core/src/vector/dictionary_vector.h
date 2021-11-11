/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __DICTIONARY_VECTOR_H__
#define __DICTIONARY_VECTOR_H__

#include "vector.h"
#include "fixed_width_vector.h"
#include "vector_allocator.h"
#include "decimal128.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
class DictionaryVector : public FixedWidthVector<int32_t> {
public:
    DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount);

    DictionaryVector(Vector *dictionary, int32_t idsCount);

    DictionaryVector(VectorAllocator *allocator, int32_t idsCount);

    ~DictionaryVector();

    Vector *GetDictionary() const
    {
        return dictionary;
    }

    VecTypeId ExtractDictionaryTypeId()
    {
        VecTypeId dictionaryType = dictionary->GetTypeId();
        if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
            return static_cast<DictionaryVector *>(dictionary)->ExtractDictionaryTypeId();
        }
        return dictionary->GetTypeId();
    }

    Vector *ExtractDictionaryAndId(int32_t position, int32_t &originalId)
    {
        ASSERT(position < size);
        VecTypeId dictionaryType = dictionary->GetTypeId();
        if (dictionaryType == OMNI_VEC_TYPE_DICTIONARY) {
            return static_cast<DictionaryVector *>(dictionary)->ExtractDictionaryAndId(GetId(position), originalId);
        }
        originalId = GetId(position);
        return dictionary;
    }

    Vector *ExtractDictionaryAndIds(int32_t positionOffset, int32_t length, int32_t *originalIds);

    Vector *ExtractDictionary();

    Vector *ExtractDictionary(const int32_t *positions, int32_t length);

    // inline for high performance.
    int32_t ALWAYS_INLINE GetId(int index) const
    {
        return ((int32_t *)valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void ALWAYS_INLINE SetId(int index, int32_t id)
    {
        ((int32_t *)valuesAddress)[index] = id;
    }

    int32_t GetInt(int32_t position) const;

    int64_t GetLong(int32_t position) const;

    double GetDouble(int32_t position) const;

    bool GetBoolean(int32_t position) const;

    int32_t GetVarchar(int32_t position, uint8_t **dst) const;

    Decimal128 GetDecimal128(int32_t position) const;

    DictionaryVector *Slice(int positionOffset, int length) override;

    DictionaryVector *CopyPositions(const int *positions, int offset, int length) override;

    DictionaryVector *CopyRegion(int positionOffset, int length) override;

    void SetValues(int startIndex, const int32_t *values, int length) override;

    // / Append Ids. Vectors must use the same dictionary
    void Append(Vector *other, int positionOffset, int length) override;

    void SetDictionary(Vector *dictionary)
    {
        this->dictionary = dictionary;
        // set nulls
        bool *nulls = new bool[size];
        for (int32_t i = 0; i < size; i++) {
            nulls[i] = dictionary->IsValueNull(GetId(i));
        }
        memcpy_s(valueNullsAddress, size * sizeof(bool), nulls, size * sizeof(bool));
        delete[] nulls;
    }

private:
    DictionaryVector(DictionaryVector *vector, int size, int positionOffset)
        : FixedWidthVector(vector, size, positionOffset),
          dictionary(vector->dictionary->Slice(0, vector->dictionary->GetSize())) {};
    int32_t *GetIds() const
    {
        return (int32_t *)valuesAddress;
    }
    Vector *dictionary;
};
} // namespace vec
} // namespace omniruntime
#endif // __DICTIONARY_VECTOR_H__
