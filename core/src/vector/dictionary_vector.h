/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __DICTIONARY_VECTOR_H__
#define __DICTIONARY_VECTOR_H__

#include "vector.h"
#include "decimal128.h"

namespace omniruntime {
namespace vec {
class DictionaryVector : public Vector {
public:
    DictionaryVector(Vector *dictionary, int32_t *ids, uint32_t idsCount);

    ~DictionaryVector()
    {
        delete dictionary;
        delete[] ids;
    }

    Vector *GetDictionary() const
    {
        return dictionary;
    }

    int32_t *GetIds() const
    {
        return ids;
    }

    int32_t GetIdsCount()
    {
        return idsCount;
    }

    int32_t GetSize() override
    {
        return idsCount;
    }

    const VecType &GetType() override
    {
        return DictionaryVecType::Instance();
    }

    VectorAllocator *GetAllocator() const override
    {
        return dictionary->GetAllocator();
    }

    int GetCapacityInBytes() const override
    {
        return dictionary->GetCapacityInBytes();
    }

    int GetValueNullsSizeInBytes() const override
    {
        return dictionary->GetValueNullsSizeInBytes();
    }

    int32_t GetInt(int32_t position) const;

    int64_t GetLong(int32_t position) const;

    double GetDouble(int32_t position) const;

    bool GetBoolean(int32_t position) const;

    int32_t GetVarchar(int32_t position, uint8_t **dst) const;

    Decimal128 GetDecimal128(int32_t position) const;

    bool IsValueNull(int32_t position) override
    {
        return dictionary->IsValueNull(ids[position]);
    }

    DictionaryVector *Slice(int positionOffset, int length) override;

    DictionaryVector *CopyPositions(const int *positions, int offset, int length) override;

    DictionaryVector *CopyRegion(int positionOffset, int length) override;

    /// Append Ids. Vectors must use the same dictionary
    void Append(Vector *other, int positionOffset, int length) override;

private:
    void InitIds(int32_t *ids, uint32_t idsCount);
    Vector *dictionary;
    int32_t *ids;
    uint32_t idsCount;
    int32_t idsOffset;
};
} // namespace vec
} // namespace omniruntime
#endif // __DICTIONARY_VECTOR_H__
