#ifndef __VARCHAR_VECTOR__H__
#define __VARCHAR_VECTOR__H__

#include "variable_width_vector.h"

class VarcharVector : public VariableWidthVector<char *>
{
public:
    VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size);
    
    int getValue(int index, char **dst) {
        ASSERT(index >= 0);
        ASSERT(index < getSize());
        int actualIndex = index + positionOffset;
        
        int startOffset = getValueOffset(actualIndex);
        int dataLen =  getValueOffset(actualIndex + 1) - startOffset;
        *dst = new char[dataLen];
        getData(startOffset, *dst, 0, dataLen);
        return dataLen;
    }
    
    void setValue(int index, const char *value, int length) {
        ASSERT(getReference()->isWritable());
        ASSERT(index >= 0);
        ASSERT(index < getSize());
        
        fillSlots(index);
        setData(index, value, 0, length);
        lastOffsetPosition = index;
    }

    VarcharVector *slice(int index, int length);

    VarcharVector *copyPositions(int *positions, int offset, int length);

    VarcharVector *copyRegion(int positionOffset, int length);

private:
    VarcharVector(VarcharVector *vector, int size, int positionOffset) : VariableWidthVector(vector, size, positionOffset) {};

    void getData(int index, char* dst, int start, int length);

    void setData(int index, const char* data, int start, int length);
    
    void fillSlots(int index);

    int lastOffsetPosition = -1;    
};
#endif // __VARCHAR_VECTOR__H__