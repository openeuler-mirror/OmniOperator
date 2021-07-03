#ifndef __BOOLEAN_VECTOR__H__
#define __BOOLEAN_VECTOR__H__

#include "fixed_width_vector.h"

class BooleanVector : public FixedWidthVector<bool> {
public:
    BooleanVector(VectorAllocator *allocator, int size);
    bool getValue(int index) {
        ASSERT(index < getSize());
        return  reinterpret_cast<bool *>(valuesAddress)[index + positionOffset];
    }
    
    void setValue(int index, bool value) {
        ASSERT(getReference()->isWritable());
        ASSERT((uint)index < getSize());
        if (value) {
            (reinterpret_cast<bool *>(valuesAddress))[index] = true;
        } else {
            (reinterpret_cast<bool *>(valuesAddress))[index] = false;
        }
    }

    void setValues(int startIndex, bool *values, int length);

    BooleanVector *slice(int positionOffset, int length);

    BooleanVector *copyPositions(int *positions, int offset, int length);

    BooleanVector *copyRegion(int positionOffset, int length);

private:
    BooleanVector(BooleanVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = 0;
};

#endif // __BOOLEAN_VECTOR__H__