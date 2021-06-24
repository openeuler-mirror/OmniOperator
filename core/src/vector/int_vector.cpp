
#include "debug.h"
#include <cstring>
#include "int_vector.h"

IntVector::IntVector(VectorAllocator *allocator, int size) :
        FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_INT) {}

void IntVector::setValues(int startIndex, int32_t *values, int length) {
    ASSERT(getReference()->isWritable());
    ASSERT(startIndex + length <= getSize());
    void *startAddress = &(((int32_t *) valuesAddress)[startIndex]);
    std::memcpy(startAddress, values, length * BYTES);
}

IntVector *IntVector::slice(int positionOffset, int length) {
    return new IntVector(this, length, positionOffset);
}

IntVector *IntVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(offset + length < getSize());
    IntVector *vector = new IntVector(getAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->setValue(i, getValue(position));
    }
    return vector;
}

IntVector *IntVector::copyRegion(int positionOffset, int length) {
    ASSERT(positionOffset + length < getSize());
    IntVector *vector = new IntVector(getAllocator(), length);
    vector->setValues(0, (int32_t *) valuesAddress + positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset, length);
    return vector;
}
