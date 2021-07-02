#ifndef __DOUBLE_VECTOR__H__
#define __DOUBLE_VECTOR__H__

#include "fixed_width_vector.h"

class DoubleVector : public FixedWidthVector<double> {
public:
    DoubleVector(VectorAllocator *allocator, int size);

    // inline for high performance.
    double getValue(int index){
        ASSERT(index < getSize());
        return ((double *) valuesAddress)[index + positionOffset];
    }

    // inline for high performance.
    void setValue(int index, double value){
        ASSERT(getReference()->isWritable());
        ASSERT((uint)index < getSize());
        ((double *) valuesAddress)[index] = value;
    }

    void setValues(int startIndex, double *values, int length);

    DoubleVector *slice(int positionOffset, int length);

    DoubleVector *copyPositions(int *positions, int offset, int length);

    DoubleVector *copyRegion(int positionOffset, int length);

private:
    DoubleVector(DoubleVector *vector, int size, int positionOffset) : FixedWidthVector(vector, size, positionOffset) {};

    static const int BYTES = sizeof(double);
};

#endif