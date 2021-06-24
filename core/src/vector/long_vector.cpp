
#include "debug.h"
#include <cstring>
#include "long_vector.h"

LongVector::LongVector(VectorAllocator *allocator, int size) :
        FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_LONG) {
}

//inline int64_t LongVector::getValue(int index) {
//    return valueAddress[index];
//}

//void LongVector::setValue(int index, int64_t value) {
//    ASSERT(getReference()->isWritable());
//    ASSERT((uint)index < getSize());
//    ((int64_t *) valuesAddress)[index] = value;
//}

void LongVector::setValues(int startIndex, int64_t *values, int length) {
    ASSERT(getReference()->isWritable());
    ASSERT(startIndex + length <= getSize());
    void *startAddress = &(((int64_t *) valuesAddress)[startIndex]);
    std::memcpy(startAddress, values, length * BYTES);
}

LongVector *LongVector::slice(int positionOffset, int length) {
    return new LongVector(this, length, positionOffset);
}

LongVector *LongVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(offset + length < getSize());
    LongVector *vector = new LongVector(getAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->setValue(i, getValue(position));
    }
    return vector;
}

LongVector *LongVector::copyRegion(int positionOffset, int length) {
    ASSERT(positionOffset + length < getSize());
    LongVector *vector = new LongVector(getAllocator(), length);
    vector->setValues(0, (int64_t *) valuesAddress + positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset, length);
    return vector;
}
