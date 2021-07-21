/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __DICTIONARY_VECTOR_H__
#define __DICTIONARY_VECTOR_H__

#include "vector.h"

// template<class T>
class DictionaryVector : public Vector {
public:
    DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount);
    ~DictionaryVector()
    {
        delete dictionary;
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

    VecType GetType() override
    {
        return OMNI_VEC_TYPE_DICTIONARY;
    }

    int32_t GetInt(int32_t position);

    int64_t GetLong(int32_t position);

    DictionaryVector *Slice(int positionOffset, int length) override;

    DictionaryVector *CopyPositions(const int *positions, int offset, int length) override;

    DictionaryVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

private:
    Vector *dictionary;
    int32_t *ids;
    int32_t idsCount;
    int32_t idsOffset;
};


#endif // __DICTIONARY_VECTOR_H__
