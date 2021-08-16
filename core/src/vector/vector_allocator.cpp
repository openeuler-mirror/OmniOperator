/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_allocator.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
VectorAllocator::VectorAllocator(std::string scope) : scope(scope)
{
    vectorList.clear();
}

// TODO: support different type of vector.
VectorReference *VectorAllocator::NewVector(int capacityInBytes, int size, VecType type)
{
    Chunk *values = new Chunk(capacityInBytes);
    Chunk *valueNulls = new Chunk(size);
    if (memset_s(valueNulls->GetAddress(), size, 0, size) != EOK) {
        std::cerr << "init value nulls failed." << std::endl;
        delete values;
        delete valueNulls;
        return nullptr;
    }
    Chunk *valueOffsets = nullptr;
    if (IsVariableWidthType(type.GetId())) {
        // 4-byte length storage variable length type offset
        int offsetSizeInBytes = (size + 1) * sizeof(int32_t);
        valueOffsets = new Chunk(offsetSizeInBytes);
        if (memset_s(valueOffsets->GetAddress(), offsetSizeInBytes, 0, offsetSizeInBytes) != EOK) {
            std::cerr << "init value offsets failed." << std::endl;
            delete values;
            delete valueNulls;
            delete valueOffsets;
            return nullptr;
        }
    }
    return new VectorReference(values, valueNulls, valueOffsets, type);
}

void VectorAllocator::FreeAllVectors() {}

std::string VectorAllocator::GetScope() const
{
    return scope;
}

int64_t VectorAllocator::GetAllocatedBytes()
{
    return allocatedBytes;
}

bool VectorAllocator::IsVariableWidthType(int type)
{
    switch (type) {
        case OMNI_VEC_TYPE_VARCHAR:
            return true;
        default:
            return false;
    }
}
}
}