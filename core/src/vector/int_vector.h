#ifndef __INT_VECTOR__H__
#define __INT_VECTOR__H__

#include "fixed_width_vector.h"

class IntVector : public FixedWidthVector<int32_t> {
public:
    IntVector(VectorAllocator *allocator, int size);

    // inline for high performance.
    int32_t getValue(int index){
        ASSERT(index < getSize());
        return ((int32_t *) valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void setValue(int index, int32_t value){
        ASSERT(getReference()->isWritable());
        ASSERT((uint)index < getSize());
        ((int32_t *) valuesAddress)[index] = value;
    }

    void setValues(int startIndex, int32_t *values, int length);

    IntVector *slice(int positionOffset, int length);

    IntVector *copyPositions(int *positions, int offset, int length);

    IntVector *copyRegion(int positionOffset, int length);

    void append(Vector *other, int positionOffset, int length);

private:
    IntVector(IntVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(int32_t);
};

#endif // __INT_VECTOR__H__