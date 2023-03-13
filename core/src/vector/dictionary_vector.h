/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __DICTIONARY_VECTOR_H__
#define __DICTIONARY_VECTOR_H__

#include <huawei_secure_c/include/securec.h>

#include "vector.h"
#include "vector_allocator.h"
#include "type/decimal128.h"
#include "type/data_type.h"

namespace omniruntime {
namespace vec {
class DictionaryVector : public Vector {
public:
    DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount);

    DictionaryVector(VectorAllocator *allocator, int32_t dataTypeId, int32_t idsCount);

    ~DictionaryVector() override;

    Vector *GetDictionary() const
    {
        return dictionary;
    }

    type::DataTypeId ExtractDictionaryTypeId()
    {
        VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
        if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
            return static_cast<DictionaryVector *>(dictionary)->ExtractDictionaryTypeId();
        }
        return dictionary->GetTypeId();
    }

    Vector *ExtractDictionaryAndId(int32_t position, int32_t &originalId)
    {
        ASSERT(position < size);
        VectorEncoding dictionaryEncoding = dictionary->GetEncoding();
        if (dictionaryEncoding == OMNI_VEC_ENCODING_DICTIONARY) {
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

    int16_t GetShort(int32_t position) const;

    int32_t GetInt(int32_t position) const;

    int64_t GetLong(int32_t position) const;

    double GetDouble(int32_t position) const;

    bool GetBoolean(int32_t position) const;

    int32_t GetVarchar(int32_t position, uint8_t **dst) const;

    Decimal128 GetDecimal128(int32_t position) const;

    DictionaryVector *Slice(int32_t positionOffset, int32_t length) override;

    DictionaryVector *CopyPositions(const int *positions, int offset, int length) override;

    DictionaryVector *CopyRegion(int positionOffset, int length) override;

    // / Append Ids. Vectors must use the same dictionary
    void Append(Vector *other, int positionOffset, int length) override;

    void SetDictionary(Vector *dictionaryVector)
    {
        this->dictionary = dictionaryVector;
        if (size <= 0) {
            return;
        }
        // when set dictionary, means we slice a dictionary vector, we need set nulls here.
        for (int32_t i = 0; i < size; i++) {
            if (dictionary->IsValueNull(GetId(i))) {
                this->SetValueNull(i);
            }
        }
    }

    VectorEncoding GetEncoding() override
    {
        return OMNI_VEC_ENCODING_DICTIONARY;
    }

    StringRef SerializeValue(size_t rowId, mem::SimpleArenaAllocator &arenaAllocator,
        const uint8_t *&begin) override
    {
        int32_t originId = 0;
        auto *originVector = ExtractDictionaryAndId(static_cast<int32_t>(rowId), originId);
        return originVector->SerializeValue(originId, arenaAllocator, begin);
    }

private:
    DictionaryVector(DictionaryVector *vector, int size, int positionOffset)
        : Vector(vector, size, positionOffset),
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
