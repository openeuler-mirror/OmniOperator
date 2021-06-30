#include "dictionary_vector.h"
#include "int_vector.h"
#include "long_vector.h"

DictionaryVector::DictionaryVector(Vector *dictionary, int32_t *ids, int32_t idsCount) : Vector(dictionary, dictionary->getSize(), 0)
{
    dictionary->getReference()->incRef();
    this->dictionary = dictionary;
    this->ids = ids;
    this->idsCount = idsCount;
    this->idsOffset = 0;
}

int32_t DictionaryVector::getInt(int32_t position)
{
    ASSERT(dictionary->getType() == OMNI_VEC_TYPE_INT);
    return ((IntVector *)dictionary)->getValue(ids[position]);
}

int64_t DictionaryVector::getLong(int32_t position)
{
    ASSERT(dictionary->getType() == OMNI_VEC_TYPE_LONG);
    return ((LongVector *)dictionary)->getValue(ids[position]);
}

DictionaryVector *DictionaryVector::slice(int positionOffset, int length)
{
    return nullptr;
}

DictionaryVector *DictionaryVector::copyPositions(int *positions, int offset, int length)
{
    return nullptr;
}

DictionaryVector *DictionaryVector::copyRegion(int positionOffset, int length)
{
    return nullptr;
}

void DictionaryVector::append(Vector *other, int positionOffset, int length)
{
}
