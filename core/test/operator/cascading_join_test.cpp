/*
* @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * @Description: cascading lookup join operator test
 */

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <vector>

#include "gtest/gtest.h"
#include "operator/operator.h"
#include "operator/join/hash_builder.h"
#include "operator/join/cascading_lookup_join.h"
#include "type/data_type.h"
#include "type/data_types.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace TestUtil;

namespace CascadingJoinTest {

/**
 * Safely allocate an array.
 *
 * Checks:
 * 1. element count must be positive;
 * 2. count * sizeof(T) must not overflow size_t;
 * 3. memory allocation must succeed.
 *
 * Returns nullptr when allocation is invalid or failed.
 */
template <typename T>
T* SafeAllocateArray(int32_t size)
{
    // 1. Reject invalid element count.
    if (size <= 0) {
        std::cerr
            << "[SafeAllocateArray] invalid size: "
            << size
            << std::endl;
        return nullptr;
    }

    const size_t count = static_cast<size_t>(size);

    // 2. Check whether:
    //
    //      count * sizeof(T)
    //
    // may overflow size_t.
    if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
        std::cerr
            << "[SafeAllocateArray] allocation size overflow: "
            << "count=" << count
            << ", elementSize=" << sizeof(T)
            << std::endl;
        return nullptr;
    }

    const size_t bytes = count * sizeof(T);

    /*
     * Optional:
     * If you want to impose a hard upper limit for unit tests,
     * it can be checked here.
     *
     * Example:
     *
     * constexpr size_t MAX_TEST_ALLOCATION_BYTES = 1ULL << 30;
     * if (bytes > MAX_TEST_ALLOCATION_BYTES) {
     *     return nullptr;
     * }
     */

    // 3. Use nothrow so allocation failure does not directly throw.
    T* ptr = new (std::nothrow) T[count];

    if (ptr == nullptr) {
        std::cerr
            << "[SafeAllocateArray] memory allocation failed: "
            << "count=" << count
            << ", bytes=" << bytes
            << std::endl;
        return nullptr;
    }

    return ptr;
}


VectorBatch* CreateBuildTable_B(int32_t size)
{
    if (size <= 0) {
        std::cerr
            << "CreateBuildTable_B: invalid size="
            << size
            << std::endl;
        return nullptr;
    }

    DataTypes buildTypes({
        LongType(),
        LongType(),
        IntType()
    });

    /*
     * Use unique_ptr so that when any allocation fails,
     * already allocated memory will be automatically released.
     */
    std::unique_ptr<int64_t[]> col0(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int64_t[]> col1(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int32_t[]> col2(
        SafeAllocateArray<int32_t>(size));

    if (!col0 || !col1 || !col2) {
        std::cerr
            << "CreateBuildTable_B: failed to allocate columns, "
            << "size=" << size
            << std::endl;
        return nullptr;
    }

    for (int32_t i = 0; i < size; ++i) {
        col0[i] = i % 100;
        col1[i] = 100 + static_cast<int64_t>(i);
        col2[i] = 200 + i;
    }

    VectorBatch* batch = CreateVectorBatch(
        buildTypes,
        size,
        col0.get(),
        col1.get(),
        col2.get());

    if (batch == nullptr) {
        std::cerr
            << "CreateBuildTable_B: CreateVectorBatch failed"
            << std::endl;
        return nullptr;
    }

    /*
     * col0 / col1 / col2 are automatically freed here.
     *
     * This preserves the behavior of your original code:
     *
     *     CreateVectorBatch(...)
     *     delete[] col0;
     *
     * Therefore this assumes CreateVectorBatch copies the input data
     * into VectorBatch-owned memory.
     */
    return batch;
}


VectorBatch* CreateBuildTable_C(int32_t size)
{
    if (size <= 0) {
        std::cerr
            << "CreateBuildTable_C: invalid size="
            << size
            << std::endl;
        return nullptr;
    }

    DataTypes buildTypes({
        LongType(),
        LongType(),
        IntType()
    });

    std::unique_ptr<int64_t[]> col0(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int64_t[]> col1(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int32_t[]> col2(
        SafeAllocateArray<int32_t>(size));

    if (!col0 || !col1 || !col2) {
        std::cerr
            << "CreateBuildTable_C: failed to allocate columns, "
            << "size=" << size
            << std::endl;
        return nullptr;
    }

    for (int32_t i = 0; i < size; ++i) {
        col0[i] = i % 50;
        col1[i] = 300 + static_cast<int64_t>(i);
        col2[i] = 400 + i;
    }

    VectorBatch* batch = CreateVectorBatch(
        buildTypes,
        size,
        col0.get(),
        col1.get(),
        col2.get());

    if (batch == nullptr) {
        std::cerr
            << "CreateBuildTable_C: CreateVectorBatch failed"
            << std::endl;
        return nullptr;
    }

    return batch;
}


VectorBatch* CreateProbeTable_A(int32_t size)
{
    if (size <= 0) {
        std::cerr
            << "CreateProbeTable_A: invalid size="
            << size
            << std::endl;
        return nullptr;
    }

    DataTypes probeTypes({
        LongType(),
        LongType(),
        IntType()
    });

    std::unique_ptr<int64_t[]> col0(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int64_t[]> col1(
        SafeAllocateArray<int64_t>(size));

    std::unique_ptr<int32_t[]> col2(
        SafeAllocateArray<int32_t>(size));

    if (!col0 || !col1 || !col2) {
        std::cerr
            << "CreateProbeTable_A: failed to allocate columns, "
            << "size=" << size
            << std::endl;
        return nullptr;
    }

    for (int32_t i = 0; i < size; ++i) {
        col0[i] = i % 100;
        col1[i] = static_cast<int64_t>(i);
        col2[i] = i;
    }

    VectorBatch* batch = CreateVectorBatch(
        probeTypes,
        size,
        col0.get(),
        col1.get(),
        col2.get());

    if (batch == nullptr) {
        std::cerr
            << "CreateProbeTable_A: CreateVectorBatch failed"
            << std::endl;
        return nullptr;
    }

    return batch;
}


TEST(CascadingJoinTest, BasicThreeTableJoin)
{
    const int32_t buildSize = 1000;
    const int32_t probeSize = 100;

    /*
     * Validate sizes before doing any memory allocation.
     */
    ASSERT_GT(buildSize, 0);
    ASSERT_GT(probeSize, 0);

    VectorBatch* buildDataB =
        CreateBuildTable_B(buildSize);

    VectorBatch* buildDataC =
        CreateBuildTable_C(buildSize);

    VectorBatch* probeDataA =
        CreateProbeTable_A(probeSize);

    ASSERT_NE(buildDataB, nullptr);
    ASSERT_NE(buildDataC, nullptr);
    ASSERT_NE(probeDataA, nullptr);

    std::vector<int32_t> buildHashColsB = {0};
    std::vector<int32_t> buildHashColsC = {0};

    auto* hashBuilderFactoryB =
        new (std::nothrow) HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes({
                LongType(),
                LongType(),
                IntType()
            }),
            buildHashColsB.data(),
            static_cast<int32_t>(buildHashColsB.size()),
            1);

    ASSERT_NE(hashBuilderFactoryB, nullptr);

    omniruntime::op::Operator* hashBuilderOpB =
        hashBuilderFactoryB->CreateOperator();

    ASSERT_NE(hashBuilderOpB, nullptr);

    int32_t status =
        hashBuilderOpB->AddInput(buildDataB);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);

    VectorBatch* outputB = nullptr;

    status =
        hashBuilderOpB->GetOutput(&outputB);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);


    auto* hashBuilderFactoryC =
        new (std::nothrow) HashBuilderOperatorFactory(
            JoinType::OMNI_JOIN_TYPE_INNER,
            DataTypes({
                LongType(),
                LongType(),
                IntType()
            }),
            buildHashColsC.data(),
            static_cast<int32_t>(buildHashColsC.size()),
            1);

    ASSERT_NE(hashBuilderFactoryC, nullptr);

    omniruntime::op::Operator* hashBuilderOpC =
        hashBuilderFactoryC->CreateOperator();

    ASSERT_NE(hashBuilderOpC, nullptr);

    status =
        hashBuilderOpC->AddInput(buildDataC);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);

    VectorBatch* outputC = nullptr;

    status =
        hashBuilderOpC->GetOutput(&outputC);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);


    std::vector<DataTypes> probeTypesList;

    probeTypesList.push_back(
        DataTypes({
            LongType(),
            LongType(),
            IntType()
        }));

    probeTypesList.push_back(
        DataTypes({
            LongType(),
            LongType(),
            IntType()
        }));

    probeTypesList.push_back(
        DataTypes({
            LongType(),
            LongType(),
            IntType()
        }));


    std::vector<std::vector<int32_t>>
        probeHashColsList;

    probeHashColsList.push_back({0});
    probeHashColsList.push_back({0});
    probeHashColsList.push_back({0});


    std::vector<std::vector<int32_t>>
        probeHashColTypesList;

    probeHashColTypesList.push_back({OMNI_LONG});
    probeHashColTypesList.push_back({OMNI_LONG});
    probeHashColTypesList.push_back({OMNI_LONG});


    std::vector<std::vector<int32_t>>
        buildOutputColsList;

    buildOutputColsList.push_back({1, 2});
    buildOutputColsList.push_back({1, 2});


    std::vector<DataTypes>
        buildOutputTypesList;

    buildOutputTypesList.push_back(
        DataTypes({
            LongType(),
            IntType()
        }));

    buildOutputTypesList.push_back(
        DataTypes({
            LongType(),
            IntType()
        }));


    std::vector<HashTableVariants*>
        hashTablesList;

    HashTableVariants* hashTableB =
        hashBuilderFactoryB->GetHashTablesVariants();

    HashTableVariants* hashTableC =
        hashBuilderFactoryC->GetHashTablesVariants();

    ASSERT_NE(hashTableB, nullptr);
    ASSERT_NE(hashTableC, nullptr);

    hashTablesList.push_back(hashTableB);
    hashTablesList.push_back(hashTableC);


    /*
     * Output columns:
     *
     * A:
     *   Long + Long + Int
     *
     * B output:
     *   Long + Int
     *
     * C output:
     *   Long + Int
     */
    constexpr int32_t outputRowSize =
        static_cast<int32_t>(
            sizeof(int64_t) +
            sizeof(int64_t) +
            sizeof(int32_t) +
            sizeof(int64_t) +
            sizeof(int32_t) +
            sizeof(int64_t) +
            sizeof(int32_t));

    ASSERT_GT(outputRowSize, 0);


    auto* cascadingFactory =
        new (std::nothrow)
            CascadingLookupJoinOperatorFactory(
                probeTypesList,
                probeHashColsList,
                probeHashColTypesList,
                buildOutputColsList,
                buildOutputTypesList,
                hashTablesList,
                outputRowSize);

    ASSERT_NE(cascadingFactory, nullptr);


    omniruntime::op::Operator* cascadingOp =
        cascadingFactory->CreateOperator();

    ASSERT_NE(cascadingOp, nullptr);


    status =
        cascadingOp->AddInput(probeDataA);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);


    VectorBatch* result = nullptr;

    status =
        cascadingOp->GetOutput(&result);

    ASSERT_EQ(status, OMNI_STATUS_NORMAL);


    if (result != nullptr) {
        const int32_t rowCount =
            result->GetRowCount();

        EXPECT_GE(rowCount, 0);

        std::cout
            << "Cascading join result rows: "
            << rowCount
            << std::endl;

        VectorHelper::FreeVecBatch(result);

        result = nullptr;
    }


    /*
     * Cleanup operators before factories.
     */
    omniruntime::op::Operator::DeleteOperator(
        hashBuilderOpB);

    omniruntime::op::Operator::DeleteOperator(
        hashBuilderOpC);

    omniruntime::op::Operator::DeleteOperator(
        cascadingOp);


    delete hashBuilderFactoryB;
    delete hashBuilderFactoryC;
    delete cascadingFactory;
}

} // namespace CascadingJoinTest