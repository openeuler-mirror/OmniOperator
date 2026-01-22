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
using namespace type;

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

TEST(PipelineTest, TestWindowGroupLimitRank2)
{

    const int32_t dataSize = 10;
    std::string group[dataSize] = {"A", "C", "B", "A", "D", "B", "A", "C", "C", "B"};
    int64_t val1[dataSize]       = {10, 50, 40, 20, 99, 30, 15, 70, 60, 25};
    int64_t val2[dataSize]       = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};

    std::vector<DataTypePtr> types = {VarcharType(10), LongType(), LongType()};
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, group, val1, val2);

    std::vector<VectorBatch*> inputVector{vecBatch};
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    std::vector<omniruntime::expressions::Expr*> partitionKeys = {
            new FieldExpr(0, VarcharType(10))
    };
    std::vector<omniruntime::expressions::Expr*> sortKeys = {
            new FieldExpr(1, LongType())
    };
    std::vector<int32_t> sortAscendings = {0};
    std::vector<int32_t> sortNullFirsts = {0};

    int32_t n = 2;
    std::string funcName = "rank";

    auto windowGroupLimitNode = std::make_shared<const WindowGroupLimitNode>(
            "window_group_limit",
            valueStreamNode,
            n,
            funcName,
            partitionKeys,
            sortKeys,
            sortAscendings,
            sortNullFirsts,
            outTypes
    );

    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{windowGroupLimitNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());

    VectorBatch *outputVecBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        VectorHelper::PrintVecBatch(out);
        if (!future.valid()) {
            outputVecBatch = out;
            break;
        }
        future.wait();
    }

    const int32_t expectedSize = 7;
    std::string expGroup[expectedSize] = {"A","A","B","B","C","C","D"};
    int64_t expVal1[expectedSize]      = {20,15,40,30,70,60,99};
    int64_t expVal2[expectedSize]      = {400,700,300,600,800,900,500};

    DataTypes expectTypes(std::vector<DataTypePtr>({VarcharType(10), LongType(), LongType()}));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, expectedSize, expGroup, expVal1, expVal2);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
}