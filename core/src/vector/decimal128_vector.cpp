/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "decimal128_vector.h"
#include "vector_type.h"

namespace omniruntime {
namespace vec {
Decimal128Vector::Decimal128Vector(VectorAllocator *allocator, int32_t size, int32_t precision, int32_t scale)
    : Vector(allocator, size * BYTES, size, Decimal128VecType(precision, scale))
{}

void Decimal128Vector::SetValues(int32_t startIndex, const int64_t *values, int32_t length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((int64_t *)valuesAddress)[startIndex * DECIMAL128_TYPE_WIDTH]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "set values failed in decimal vector." << std::endl;
    }
}

Decimal128Vector *Decimal128Vector::Slice(int32_t positionOffset, int32_t length)
{
    return new Decimal128Vector(this, length, positionOffset);
}

Decimal128Vector *Decimal128Vector::CopyPositions(const int32_t *positions, int32_t offset, int32_t length)
{
    if (length > size) {
        return nullptr;
    }
    auto vector = new Decimal128Vector(GetAllocator(), length, GetPrecision(), GetScale());
    for (int32_t i = 0; i < length; ++i) {
        int32_t position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

Decimal128Vector *Decimal128Vector::CopyRegion(int32_t positionOffset, int32_t length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    auto vector = new Decimal128Vector(GetAllocator(), length, GetPrecision(), GetScale());
    vector->SetValues(0, (int64_t *)valuesAddress + (positionOffset + this->positionOffset) * DECIMAL128_TYPE_WIDTH,
        length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void Decimal128Vector::Append(Vector *other, int32_t positionOffset, int32_t length)
{
    if (positionOffset + length > size) {
        return;
    }
    uint8_t *destination = (uint8_t *)this->GetValues() + positionOffset * BYTES;
    uint8_t *src = (other->GetPositionOffset() * BYTES) + (static_cast<uint8_t *>(other->GetValues()));
    errno_t ret = memcpy_s(destination, capacityInBytes, src, length * BYTES);
    if (ret != EOK) {
        std::cerr << "append failed in double vector." << std::endl;
    }
}

int32_t Decimal128Vector::GetPrecision()
{
    VecType type = GetType();
    return static_cast<Decimal128VecType *>(&type)->GetPrecision();
}

int32_t Decimal128Vector::GetScale()
{
    VecType type = GetType();
    return static_cast<Decimal128VecType *>(&type)->GetScale();
}
}
}