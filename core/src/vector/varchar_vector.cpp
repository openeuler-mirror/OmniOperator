#include "debug.h"
#include "varchar_vector.h"
#include <cstring>

VarcharVector::VarcharVector(VectorAllocator *allocator, int capacityInBytes, int size) :
        VariableWidthVector(allocator, capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR) {}

VarcharVector *VarcharVector::slice(int positionOffset, int length) {
    return new VarcharVector(this, length, positionOffset);
}

VarcharVector *VarcharVector::copyPositions(int *positions, int offset, int length) {
    ASSERT(length <= getSize());
    int totalDataLen = 0;
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + positionOffset;
        totalDataLen += getValueOffset(position + 1) - getValueOffset(position);
    }
    VarcharVector *vector = new VarcharVector(getAllocator(), totalDataLen, length);
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i] + positionOffset;
        int startOffset = getValueOffset(position);
        int dataLen =  getValueOffset(position + 1) - startOffset;
        char *data = reinterpret_cast<char *>(valuesAddress);
        vector->setValue(i, data + startOffset, dataLen);
        vector->setValueNulls(i, ((bool *) valueNullsAddress) + position, 1);
    }
    return vector;
}

VarcharVector *VarcharVector::copyRegion(int positionOffset, int length) {
    ASSERT(length <= getSize());
    
    int newPosition = positionOffset + this->positionOffset;
    int startOffset = getValueOffset(newPosition);
    int totalDataLen = getValueOffset(newPosition + length) - getValueOffset(newPosition);

    VarcharVector *vector = new VarcharVector(getAllocator(), totalDataLen, length);
    std::memcpy(reinterpret_cast<char *>(vector->getValues()), (reinterpret_cast<char *>(valuesAddress)) + startOffset, totalDataLen);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset + this->positionOffset, length);
    
    // copy offset
    int32_t *offsets = reinterpret_cast<int32_t *>(vector->getValueOffsets());
    for (int32_t i = 1; i <= length; i++) {
        offsets[i] =  getValueOffset(newPosition + i) - getValueOffset(newPosition);
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

void VarcharVector::setData(int index, const char* value, int start, int length) {
    if (value == nullptr) {
        return;
    }
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