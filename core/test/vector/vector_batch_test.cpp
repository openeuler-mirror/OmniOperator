/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"

using namespace omniruntime::vec;

namespace VectorBatchTest {
void VectorBatchTestInitDataTypes(std::vector<DataType> &types)
{
    types.push_back(IntDataType::Instance());
    types.push_back(DoubleDataType::Instance());
    types.push_back(LongDataType::Instance());
    types.push_back(DoubleDataType::Instance());
    types.push_back(ContainerDataType::Instance());
}

TEST(VectorBatch, constructVectorBatchWithVectorCount)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetGlobalAllocator();
    VectorBatch *vectorBatch = new VectorBatch(4);
    LongVector *vector0 = new LongVector(allocator, 1024);
    LongVector *vector1 = new LongVector(allocator, 1024);
    LongVector *vector2 = new LongVector(allocator, 1024);
    LongVector *vector3 = new LongVector(allocator, 1024);

    vectorBatch->SetVector(0, vector0);
    vectorBatch->SetVector(1, vector1);
    vectorBatch->SetVector(2, vector2);
    vectorBatch->SetVector(3, vector3);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(vectorBatch->GetVector(0)->GetSize(), 1024);
    }
    delete vectorBatch;
}

TEST(VectorBatch, constructVectorBatchWithTypes)
{
    std::vector<DataType> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(4, 1024);
    vectorBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), types);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(vectorBatch->GetVector(0)->GetSize(), 1024);
    }
    delete vectorBatch;
}

TEST(VectorBatch, getVectorCount)
{
    std::vector<DataType> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(4, 1024);
    vectorBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), types);

    EXPECT_EQ(4, vectorBatch->GetVectorCount());
    EXPECT_EQ(1024, vectorBatch->GetRowCount());
    delete vectorBatch;
}

TEST(VectorBatch, getVectorTypes)
{
    std::vector<DataType> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(5, 1024);
    vectorBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), types);

    const int32_t *vectorTypeIds = vectorBatch->GetVectorTypeIds();
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(types[i].GetId(), vectorTypeIds[i]);
    }
    delete vectorBatch;
}
}
