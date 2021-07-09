#ifndef __DICTIONARY_VECTOR_H__
#define __DICTIONARY_VECTOR_H__

#include "vector.h"

//template<class T>
class DictionaryVector : public Vector{
public:
    DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount);
    ~DictionaryVector()
    {
        delete dictionary;
    }
    Vector *getDictionary()
    {
        return dictionary;
    }

    int32_t *getIds()
    {
        return ids;
    }

    int32_t getIdsCount()
    {
        return idsCount;
    }

    int32_t getSize()
    {
        return idsCount;
    }

    VecType getType()
    {
        return OMNI_VEC_TYPE_DICTIONARY;
    }

    int32_t getInt(int32_t position);

    int64_t getLong(int32_t position);

    DictionaryVector *slice(int positionOffset, int length);

    DictionaryVector *copyPositions(int *positions, int offset, int length);

    DictionaryVector *copyRegion(int positionOffset, int length);

    void append(Vector *other, int positionOffset, int length);

private:
    Vector *dictionary;
    int32_t *ids;
    int32_t idsCount;
    int32_t idsOffset;
};


#endif //__DICTIONARY_VECTOR_H__
