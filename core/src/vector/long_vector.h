#ifndef __LONG_VECTOR__H__
#define __LONG_VECTOR__H__

#include "fixed_width_vector.h"
#include <limits>

class LongVector : public FixedWidthVector<int64_t> {
public:
    LongVector(VectorAllocator *allocator, int size);

    // inline for high performance.
    int64_t getValue(int index){
        ASSERT(index < getSize());
        return ((int64_t *)valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void setValue(int index, int64_t value){
        ASSERT(getReference()->isWritable());
        ASSERT((uint)index < getSize());
        ((int64_t *)valuesAddress)[index] = value;
    }

    void setValues(int startIndex, int64_t *values, int length);

    LongVector *slice(int positionOffset, int length);

    LongVector *copyPositions(int *positions, int offset, int length);

    LongVector *copyRegion(int positionOffset, int length);

    void append(Vector *other, int positionOffset, int length);

private:
    LongVector(LongVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(int64_t);
};

#endif
