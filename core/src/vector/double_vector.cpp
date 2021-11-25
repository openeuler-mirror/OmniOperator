/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include <cstring>
#include "double_vector.h"
#include "dictionary_vector.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"
namespace omniruntime {
namespace vec {
DoubleVector::DoubleVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size * BYTES, size, DoubleVecType::Instance())
{}

void DoubleVector::SetValues(int startIndex, const double *values, int length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((double *)valuesAddress)[startIndex]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "set values failed in double vector." << std::endl;
    }
}

DoubleVector *DoubleVector::Slice(int positionOffset, int length)
{
    return new DoubleVector(this, length, positionOffset);
}

DoubleVector *DoubleVector::CopyPositions(const int *positions, int offset, int length)
{
    DoubleVector *vector = new DoubleVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNull(i, IsValueNull(position));
    }
    return vector;
}

DoubleVector *DoubleVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    DoubleVector *vector = new DoubleVector(GetAllocator(), length);
    vector->SetValues(0, (double *)valuesAddress + positionOffset + this->positionOffset, length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void DoubleVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }
    if (other->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
        int32_t otherPositionOffset = other->GetPositionOffset();
        double *otherValues = static_cast<double *>(other->GetValues()) + otherPositionOffset;
        bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
        SetValues(positionOffset, otherValues, length);
        SetValueNulls(positionOffset, otherValueNulls, length);
    } else {
        DictionaryVector *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        DoubleVector *dictionary = static_cast<DoubleVector *>(src->ExtractDictionaryAndIds(0, length, originalIds));
        for (int32_t i = 0; i < length; i++) {
            if (dictionary->IsValueNull(originalIds[i])) {
                SetValueNull(positionOffset + i);
            } else {
                SetValue(positionOffset + i, dictionary->GetValue(originalIds[i]));
            }
        }
    }
}
}
}