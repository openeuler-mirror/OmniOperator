/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "compute/driver.h"
#include "compute/task.h"
#include "compute/local_planner.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "gtest/gtest.h"
#include "test/util/test_util.h"
#include "util/config/QueryConfig.h"

using namespace omniruntime;
using namespace TestUtil;

namespace orderbyTest {
class TestBatchIterator : public ColumnarBatchIterator {
public:
    TestBatchIterator(const std::vector<VectorBatch*> &date): date(date) {}

    ~TestBatchIterator() override = default;

    VectorBatch *Next() override
    {
        if (index < date.size()) {
            return date[index++];
        } else {
            return nullptr;
        }
    }

private:
    std::vector<VectorBatch*> date;
    size_t index = 0;
};

TEST(DriverTest, TestOrderBy)
{
    std::vector<DataTypePtr> types = {LongType()};
    DataTypes sourceTypes(types);
    const int64_t dataSize1 = 6;
    int64_t data1[dataSize1] = {6, 4, 7, 3, 2, 1};
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize1, data1);
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch1);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(std::vector<DataTypePtr>({LongType()}));
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    std::vector<int32_t> c = {0};
    auto orderbyNode = std::make_shared<const OrderByNode>("order_by", c, c, c, valueStreamNode);

    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{orderbyNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            // Not need to wait. Break.
            vectorBatch = out;
            break;
        }
        // Omni suggested to wait. This might be because another thread (e.g., background io thread)
        // is spilling the task.
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }
    int64_t expect[dataSize1] = {7, 6, 4, 3, 2, 1};
    VectorBatch *vecBatchExpect = CreateVectorBatch(sourceTypes, dataSize1, expect);
    ASSERT_TRUE(VecBatchMatch(vecBatchExpect, vectorBatch));
    VectorHelper::FreeVecBatch(vecBatchExpect);
    VectorHelper::FreeVecBatch(vectorBatch);
}

TEST(DriverTest, TestCreateFactory)
{
    std::vector<DataTypePtr> types = {LongType()};
    DataTypes sourceTypes(types);
    const int64_t dataSize1 = 6;
    int64_t data1[dataSize1] = {6, 4, 7, 3, 2, 1};
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize1, data1);
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch1);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(std::vector<DataTypePtr>({LongType()}));
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    std::vector<int32_t> c = {0};
    auto orderbyNode = std::make_shared<const OrderByNode>("order_by", c, c, c, valueStreamNode);

    const auto queryConfig = omniruntime::config::QueryConfig();
    auto sortFactory =
        omniruntime::compute::createOperatorFactory(orderbyNode, queryConfig);
    auto valueStreamFactory =
        omniruntime::compute::createOperatorFactory(valueStreamNode, queryConfig);

    auto sortOperator = omniruntime::compute::createOperator(sortFactory);
    auto valueStreamOperator = omniruntime::compute::createOperator(valueStreamFactory);

    delete sortFactory;
    delete valueStreamFactory;

    VectorHelper::FreeVecBatch(vecBatch1);
}
}
