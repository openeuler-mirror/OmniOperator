#include "debug.h"
#include "varchar_vector.h"
#include <cstring>

VarcharVector::VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size) :
        VariableWidthVector(allocator, capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR) {}

int VarcharVector::getValue(int index, char **dst) {
    ASSERT(index >= 0);
    int actualIndex = index + getPositionOffset();
    ASSERT(actualIndex < getSize());
    // check is null
    // if (isValueNull(actualIndex)) {
    //     dst = nullptr;
    //     return -1;
    // }

    int startOffset = getValueOffset(actualIndex);
    int dataLen =  getValueOffset(actualIndex + 1) - startOffset;
    *dst = new char[dataLen];
    getData(startOffset, *dst, 0, dataLen);
    return dataLen;
}

void VarcharVector::setValue(int index, char *value, int length) {
    ASSERT(index >= 0);
    int actualIndex = index + getPositionOffset();
    ASSERT(actualIndex < getSize());
    
    fillSlots(actualIndex);
    setValueNullBitMap(actualIndex);
    setData(actualIndex, value, 0, length);
    lastOffsetPosition = actualIndex;
}

VarcharVector *VarcharVector::slice(int positionOffset, int length) {
    return new VarcharVector(this, length, positionOffset);
}

VarcharVector *VarcharVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(length < getSize());
    int totalDataLen = 0;
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + getPositionOffset();
        totalDataLen += getValueOffset(position + 1) - getValueOffset(position);
    }
    VarcharVector *vector = new VarcharVector(getAllocator(), totalDataLen, length);
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + getPositionOffset();
        int startOffset = getValueOffset(position);
        int dataLen =  getValueOffset(position + 1) - startOffset;
        char *data = reinterpret_cast<char *>(valuesAddress);
        vector->setValue(i, data + startOffset, dataLen);
    }
    return vector;
}

VarcharVector *VarcharVector::copyRegion(int positionOffset, int length) {
    ASSERT(length < getSize());
    int totalDataLen = 0;
    for (int i = 0; i < length; i++) {
        int position = positionOffset + i + getPositionOffset();
        totalDataLen += getValueOffset(position + 1) - getValueOffset(position);
    }

    VarcharVector *vector = new VarcharVector(getAllocator(), totalDataLen, length);
    for (int i = 0; i < length; i++) {
        int position = positionOffset + i + getPositionOffset();
        int startOffset = getValueOffset(position);
        int dataLen =  getValueOffset(position + 1) - startOffset;
        char *data = reinterpret_cast<char *>(valuesAddress);
        vector->setValue(i, data + startOffset, dataLen);
    }
    return vector;
}

void VarcharVector::getData(int startOffset, char* dst, int start, int length) {
    ASSERT(startOffset + length <= getReference()->getCapacityInBytes());
    if (dst == nullptr) {
        return;
    }
    char *data = reinterpret_cast<char *>(valuesAddress);
    std::memcpy(dst + start, data + startOffset, length);
}

void VarcharVector::setData(int index, char* value, int start, int length) {
    int startOffset = getValueOffset(index);
    ASSERT(startOffset + length <= getReference()->getCapacityInBytes());
    setValueOffset(index + 1, startOffset + length);
    char *data = reinterpret_cast<char *>(valuesAddress);
    std::memcpy(data + startOffset, value + start, length);
}

void VarcharVector::fillSlots(int index) {
    for (int i = lastOffsetPosition + 1; i < index; i++) {
        setData(i, nullptr, 0, 0);
    }
    lastOffsetPosition = index - 1;
}