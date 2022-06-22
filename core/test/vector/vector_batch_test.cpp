/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"

using namespace omniruntime::vec;

namespace VectorBatchTest {
void VectorBatchTestInitDataTypes(std::vector<DataTypeRawPtr> &types)
{
    types.push_back(new IntDataType);
    types.push_back(new DoubleDataType);
    types.push_back(new LongDataType);
    types.push_back(new DoubleDataType);
    types.push_back(new ContainerDataType());
}

TEST(VectorBatch, constructVectorBatchWithVectorCount)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("vectorBatch_constructVectorBatchWithVectorCount");
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
    VectorHelper::FreeVecBatch(vectorBatch);
    delete allocator;
}

TEST(VectorBatch, constructVectorBatchWithTypes)
{
    std::vector<DataTypeRawPtr> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(4, 1024);
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("vectorBatch_constructVectorBatchWithTypes");
    vectorBatch->NewVectors(allocator, types);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(vectorBatch->GetVector(0)->GetSize(), 1024);
    }
    VectorHelper::FreeVecBatch(vectorBatch);
    delete allocator;
}

TEST(VectorBatch, getVectorCount)
{
    std::vector<DataTypeRawPtr> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(4, 1024);
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("vectorBatch_getVectorCount");
    vectorBatch->NewVectors(allocator, types);

    EXPECT_EQ(4, vectorBatch->GetVectorCount());
    EXPECT_EQ(1024, vectorBatch->GetRowCount());

    VectorHelper::FreeVecBatch(vectorBatch);
    delete allocator;
}

TEST(VectorBatch, getVectorTypes)
{
    std::vector<DataTypeRawPtr> types;
    VectorBatchTestInitDataTypes(types);
    VectorBatch *vectorBatch = new VectorBatch(5, 1024);
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("vectorBatch_getVectorTypes");
    vectorBatch->NewVectors(allocator, types);

    const int32_t *vectorTypeIds = vectorBatch->GetVectorTypeIds();
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(types[i]->GetId(), vectorTypeIds[i]);
    }
    VectorHelper::FreeVecBatch(vectorBatch);
    delete allocator;
}
}
