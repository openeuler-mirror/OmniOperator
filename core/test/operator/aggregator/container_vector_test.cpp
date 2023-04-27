/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector/vector_common.h"
#include "test/util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::TestUtil;

namespace ContainerVectorTest {
const int32_t POSITION_COUNT = 100;
const int32_t VECTOR_COUNT = 2;
TEST(ContainerVector, setAndGetValue)
{
    auto doubleVector = std::make_unique<Vector<double>>(POSITION_COUNT);
    auto longVector = std::make_unique<Vector<int64_t>>(POSITION_COUNT);
    std::vector<int64_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<int64_t>(doubleVector.get());
    vectorAddresses[1] = reinterpret_cast<int64_t>(longVector.get());
    std::vector<DataTypePtr> VECTOR_TYPES = { DoubleType(), LongType() };
    auto *vector = new ContainerVector(POSITION_COUNT, vectorAddresses, VECTOR_TYPES);
    BaseVector* values[] = {new Vector<double>(POSITION_COUNT), new Vector<int64_t>(POSITION_COUNT)};
    for (int i = 0; i < VECTOR_COUNT; ++i) {
        vector->SetValue(i, reinterpret_cast<int64_t>(values[i]));
    }

    for (int i = 0; i < VECTOR_COUNT; ++i) {
        EXPECT_EQ(vector->GetValue(i), reinterpret_cast<int64_t>(values[i]));
    }

    delete vector;
}

TEST(ContainerVector, testNullFlagWithSet)
{
    int rows = 10;
    auto intVector = new Vector<int32_t>(rows);
    auto longVector = new Vector<int64_t>(rows);

    std::vector<int64_t> subAddrs = { reinterpret_cast<int64_t>(intVector), reinterpret_cast<int64_t>(longVector) };
    std::vector<DataTypePtr> subTypes = { IntType(), LongType() };
    auto *hasNulls = new ContainerVector(rows, subAddrs, subTypes);

    std::vector<bool> nulls = { true, false, true, false, true, false, true, false, true, false };
    for (int32_t i = 0; i < rows; ++i) {
        if (nulls[i]) {
            hasNulls->SetNull(i);
        }
    }
    EXPECT_TRUE(hasNulls->HasNull());

    delete hasNulls;
}

TEST(ContainerVector, jniFreeVector)
{
    auto doubleVector = new Vector<double>(POSITION_COUNT);
    auto longVector = new Vector<int64_t>(POSITION_COUNT);

    std::vector<int64_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<int64_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<int64_t>(longVector);
    std::vector<DataTypePtr> VECTOR_TYPES = { DoubleType(), LongType() };
    auto *vector = new ContainerVector(POSITION_COUNT, vectorAddresses, VECTOR_TYPES);
    auto *vec = (BaseVector *)vector;

    delete vec;
}

TEST(ContainerVector, appendVector)
{
    // TODO: add UT after container vector supports this function.
}

TEST(ContainerVector, copyPositions)
{
    // TODO: add UT after container vector supports this function.
}

TEST(ContainerVector, sliceVector)
{
    // TODO: add UT after container vector supports this function.
}
}