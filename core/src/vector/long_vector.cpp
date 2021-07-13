
#include "debug.h"
#include <cstring>
#include "long_vector.h"

LongVector::LongVector(VectorAllocator *allocator, int size) :
        FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_LONG) {
}

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
    ASSERT(length <= getSize());
    LongVector *vector = new LongVector(getAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->setValue(i, getValue(position));
        vector->setValueNulls(i, ((bool *) valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

LongVector *LongVector::copyRegion(int positionOffset, int length) {
    ASSERT(positionOffset + length <= getSize());
    LongVector *vector = new LongVector(getAllocator(), length);
    vector->setValues(0, (int64_t *) valuesAddress + positionOffset + this->positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void LongVector::append(Vector *other, int positionOffset, int length) {
    ASSERT(positionOffset + length <= getSize());
    uint8_t *destination = (uint8_t*) this->getValues() + positionOffset * BYTES;
    uint8_t *src = (other->getPositionOffset() * BYTES) + ((uint8_t*) other->getValues());
    std::memcpy(destination, src, length * BYTES);
}
