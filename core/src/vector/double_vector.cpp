
#include "debug.h"
#include <cstring>
#include "double_vector.h"

DoubleVector::DoubleVector(VectorAllocator *allocator, int size) :
        FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_DOUBLE) {}

void DoubleVector::setValues(int startIndex, double *values, int length) {
    ASSERT(getReference()->isWritable());
    ASSERT(startIndex + length <= getSize());
    void *startAddress = &(((double *) valuesAddress)[startIndex]);
    std::memcpy(startAddress, values, length * BYTES);
}

DoubleVector *DoubleVector::slice(int positionOffset, int length) {
    return new DoubleVector(this, length, positionOffset);
}

DoubleVector *DoubleVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(offset + length < getSize());
    DoubleVector *vector = new DoubleVector(getAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->setValue(i, getValue(position));
    }
    return vector;
}

DoubleVector *DoubleVector::copyRegion(int positionOffset, int length) {
    ASSERT(positionOffset + length < getSize());
    DoubleVector *vector = new DoubleVector(getAllocator(), length);
    vector->setValues(0, (double *) valuesAddress + positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset, length);
    return vector;
}
