
#include "debug.h"
#include <cstring>
#include "boolean_vector.h"

BooleanVector::BooleanVector(VectorAllocator *allocator, int size) :
        FixedWidthVector(allocator, size, size, OMNI_VEC_TYPE_BOOLEAN) {}

void BooleanVector::setValues(int startIndex, bool *values, int length) {
    ASSERT(getReference()->isWritable());
    ASSERT(length <= getSize());
    bool *startAddr = reinterpret_cast<bool *>(valuesAddress);
    std::memcpy(startAddr + startIndex, values, length);
}

BooleanVector *BooleanVector::slice(int positionOffset, int length) {
    return new BooleanVector(this, length, positionOffset);
}

BooleanVector *BooleanVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(length <= getSize());
    BooleanVector *vector = new BooleanVector(getAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->setValue(i, getValue(position));
        vector->setValueNulls(i, ((bool *) valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

BooleanVector *BooleanVector::copyRegion(int positionOffset, int length) {
    ASSERT(length <= getSize());
    BooleanVector *vector = new BooleanVector(getAllocator(), length);
    vector->setValues(0, (bool *) valuesAddress + positionOffset + this->positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void BooleanVector::append(Vector *other, int positionOffset, int length) {
    ASSERT(positionOffset + length <= getSize());
    uint8_t *destination = (uint8_t*) this->getValues() + positionOffset * 1/8;
    uint8_t *src = (other->getPositionOffset() * 1/8) + ((uint8_t*) other->getValues());
    std::memcpy(destination, src, length * 1/8);
}
