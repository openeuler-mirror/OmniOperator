#ifndef __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
#define __VARIABLE_WIDTH_VECTOR_OPERATOR_H__

#include "vector.h"

template<class T>
class VariableWidthVector : public Vector {
public:
    VariableWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    VariableWidthVector(VectorAllocator *pAllocator, int capacityInBytes, int size, VecType type) : Vector(pAllocator,
                                                                                                           capacityInBytes,
                                                                                                           size,
                                                                                                           type) {}
    virtual int getValue(int index, T *dst) { return -1; }

    virtual void setValue(int index, T data, int length) {}                                                                                                       
};
#endif // __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
