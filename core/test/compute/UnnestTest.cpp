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
 
namespace UnionTest {
class TestBatchIterator : public ColumnarBatchIterator {
public:
    explicit TestBatchIterator(const std::vector<VectorBatch*> &data): data(data) {}

    ~TestBatchIterator() override = default;

    VectorBatch *Next() override
    {
        if (index < data.size()) {
            return data[index++];
        } else {
            return nullptr;
        }
    }

private:
    std::vector<VectorBatch*> data;
    size_t index = 0;
};

VectorBatch *CreateTestUnnestVecBatch()
{
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[dataSize] = {6, 5, 4, 3, 2, 1};

    std::vector<DataTypePtr> types = { IntType(), DoubleType(), ShortType() };
    DataTypes sourceTypes(types);

    return CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);
}

VectorBatch *CreateTestUnnestOutputVecBatch()
{
    const int32_t dataSize = 6;
    int32_t expData1[dataSize] = {0, 1, 2, 0, 1, 2};
    double expData2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t expData3[dataSize] = {6, 5, 4, 3, 2, 1};

    std::vector<DataTypePtr> types = { IntType(), DoubleType(), ShortType() };
    DataTypes sourceTypes(types);

    return CreateVectorBatch(sourceTypes, dataSize, expData1, expData2, expData3);
}

TEST(PipelineTest, TestUnest)
{
    std::vector<DataTypePtr> types = { IntType(), DoubleType(), ShortType() };
    VectorBatch *vecBatch = CreateTestUnnestVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, DoubleType());
    auto expr3 = new FieldExpr(2, ShortType());
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2),
        static_cast<ExprPtr>(expr3)};
    auto unnestVariables = std::vector<ExprPtr>{};
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            vectorBatch = out;
            break;
        }
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }

    VectorBatch *expVecBatch = CreateTestUnnestOutputVecBatch();

    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestUnnestVecBatchWithArray()
{
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    // Create array vector
    // [[1], [2, 2], [3, 3], [4], [5], [6, 6, 6]]
    int32_t elementSize = 10;
    auto elementVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    elementVector->SetValue(0, 1);
    elementVector->SetValue(1, 2);
    elementVector->SetValue(2, 2);
    elementVector->SetValue(3, 3);
    elementVector->SetValue(4, 3);
    elementVector->SetValue(5, 4);
    elementVector->SetValue(6, 5);
    elementVector->SetValue(7, 6);
    elementVector->SetValue(8, 6);
    elementVector->SetValue(9, 6);
    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementVector);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 1);
    arrayVector->SetOffset(2, 3);
    arrayVector->SetOffset(3, 5);
    arrayVector->SetOffset(4, 6);
    arrayVector->SetOffset(5, 7);
    arrayVector->SetOffset(6, 10);
    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestUnnestOutputVecBatchWithArray()
{
    const int32_t dataSize = 10;
    int32_t data1[dataSize] = {0, 1, 1, 2, 2, 0, 1, 2, 2, 2};
    double data2[dataSize] = {6.6, 5.5, 5.5, 4.4, 4.4, 3.3, 2.2, 1.1, 1.1, 1.1};
    int32_t data3[dataSize] = {1, 2, 2, 3, 3, 4, 5, 6, 6, 6};

    std::vector<DataTypePtr> types = { IntType(), DoubleType(), IntType() };
    DataTypes sourceTypes(types);
    return CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);
}

TEST(PipelineTest, TestUnestArray)
{
    std::vector<DataTypePtr> types = { IntType(), DoubleType(), std::make_shared<ArrayType>(IntType()) };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, DoubleType());
    auto expr3 = new FieldExpr(2, std::make_shared<ArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2)};

    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr3) };
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            vectorBatch = out;
            break;
        }
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }

    VectorBatch *expVecBatch = CreateTestUnnestOutputVecBatchWithArray();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestUnnestVecBatchWithTwoArray()
{
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1);

    // Create array vector
    // [[1], [2, 2], [3, 3], [4], [5], [6, 6, 6]]
    int32_t elementSize = 10;
    auto elementVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    elementVector->SetValue(0, 1);
    elementVector->SetValue(1, 2);
    elementVector->SetValue(2, 2);
    elementVector->SetValue(3, 3);
    elementVector->SetValue(4, 3);
    elementVector->SetValue(5, 4);
    elementVector->SetValue(6, 5);
    elementVector->SetValue(7, 6);
    elementVector->SetValue(8, 6);
    elementVector->SetValue(9, 6);
    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementVector);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 1);
    arrayVector->SetOffset(2, 3);
    arrayVector->SetOffset(3, 5);
    arrayVector->SetOffset(4, 6);
    arrayVector->SetOffset(5, 7);
    arrayVector->SetOffset(6, 10);
    batch->Append(arrayVector);

    // Create array vector
    // [[1, 1], [2], [3, 3, 3, 3], [4], [5], [6]]
    auto elementVector1 = std::make_shared<vec::Vector<int32_t>>(elementSize);
    elementVector1->SetValue(0, 1);
    elementVector1->SetValue(1, 1);
    elementVector1->SetValue(2, 2);
    elementVector1->SetValue(3, 3);
    elementVector1->SetValue(4, 3);
    elementVector1->SetValue(5, 3);
    elementVector1->SetValue(6, 3);
    elementVector1->SetValue(7, 4);
    elementVector1->SetValue(8, 5);
    elementVector1->SetValue(9, 6);
    vec::ArrayVector* arrayVector1 = new vec::ArrayVector(dataSize, elementVector1);
    arrayVector1->SetOffset(0, 0);
    arrayVector1->SetOffset(1, 2);
    arrayVector1->SetOffset(2, 3);
    arrayVector1->SetOffset(3, 7);
    arrayVector1->SetOffset(4, 8);
    arrayVector1->SetOffset(5, 9);
    arrayVector1->SetOffset(6, 10);

    batch->Append(arrayVector1);
    return batch;
}

VectorBatch *CreateTestUnnestOutputVecBatchWithTwoArray()
{
    const int32_t dataSize = 13;
    int32_t data1[dataSize] = {0, 0, 1, 1, 2, 2, 2, 2, 0, 1, 2, 2, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1);

    int32_t elementSize = 13;
    auto vector1 = new vec::Vector<int32_t>(elementSize);
    vector1->SetValue(0, 1);
    vector1->SetNull(1);
    vector1->SetValue(2, 2);
    vector1->SetValue(3, 2);
    vector1->SetValue(4, 3);
    vector1->SetValue(5, 3);
    vector1->SetNull(6);
    vector1->SetNull(7);
    vector1->SetValue(8, 4);
    vector1->SetValue(9, 5);
    vector1->SetValue(10, 6);
    vector1->SetValue(11, 6);
    vector1->SetValue(12, 6);

    auto vector2 = new vec::Vector<int32_t>(elementSize);
    vector2->SetValue(0, 1);
    vector2->SetValue(1, 1);
    vector2->SetValue(2, 2);
    vector2->SetNull(3);
    vector2->SetValue(4, 3);
    vector2->SetValue(5, 3);
    vector2->SetValue(6, 3);
    vector2->SetValue(7, 3);
    vector2->SetValue(8, 4);
    vector2->SetValue(9, 5);
    vector2->SetValue(10, 6);
    vector2->SetNull(11);
    vector2->SetNull(12);
    
    batch->Append(vector1);
    batch->Append(vector2);
    return batch;
}

TEST(PipelineTest, TestUnestMultiArray)
{
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<ArrayType>(IntType()),
        std::make_shared<ArrayType>(IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithTwoArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<ArrayType>(IntType()));
    auto expr3 = new FieldExpr(2, std::make_shared<ArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1)
    };

    auto unnestVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr2),
        static_cast<ExprPtr>(expr3)
    };
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            vectorBatch = out;
            break;
        }
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }

    VectorBatch *expVecBatch = CreateTestUnnestOutputVecBatchWithTwoArray();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestUnnestOneArrayOutputVecBatch()
{
    const int32_t dataSize = 10;
    int32_t data1[dataSize] = {0, 0, 1, 2, 2, 2, 2, 0, 1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1);

    // Create array vector
    // [[1], [1], [2, 2], [3, 3], [3, 3], [3, 3], [3, 3], [4], [5], [6, 6, 6]]
    int32_t elementSize = 17;
    auto elementVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    elementVector->SetValue(0, 1);
    elementVector->SetValue(1, 1);
    elementVector->SetValue(2, 2);
    elementVector->SetValue(3, 2);
    elementVector->SetValue(4, 3);
    elementVector->SetValue(5, 3);
    elementVector->SetValue(6, 3);
    elementVector->SetValue(7, 3);
    elementVector->SetValue(8, 3);
    elementVector->SetValue(9, 3);
    elementVector->SetValue(10, 3);
    elementVector->SetValue(11, 3);
    elementVector->SetValue(12, 4);
    elementVector->SetValue(13, 5);
    elementVector->SetValue(14, 6);
    elementVector->SetValue(15, 6);
    elementVector->SetValue(16, 6);
    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementVector);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 1);
    arrayVector->SetOffset(2, 2);
    arrayVector->SetOffset(3, 4);
    arrayVector->SetOffset(4, 6);
    arrayVector->SetOffset(5, 8);
    arrayVector->SetOffset(6, 10);
    arrayVector->SetOffset(7, 12);
    arrayVector->SetOffset(8, 13);
    arrayVector->SetOffset(9, 14);
    arrayVector->SetOffset(10, 17);
    batch->Append(arrayVector);

    auto vector = new vec::Vector<int32_t>(dataSize);
    vector->SetValue(0, 1);
    vector->SetValue(1, 1);
    vector->SetValue(2, 2);
    vector->SetValue(3, 3);
    vector->SetValue(4, 3);
    vector->SetValue(5, 3);
    vector->SetValue(6, 3);
    vector->SetValue(7, 4);
    vector->SetValue(8, 5);
    vector->SetValue(9, 6);
    batch->Append(vector);
    return batch;
}

TEST(PipelineTest, TestUnestOneArray)
{
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<ArrayType>(IntType()),
        std::make_shared<ArrayType>(IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithTwoArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<ArrayType>(IntType()));
    auto expr3 = new FieldExpr(2, std::make_shared<ArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2)
    };

    auto unnestVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr3)
    };
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            vectorBatch = out;
            break;
        }
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }

    VectorBatch *expVecBatch = CreateTestUnnestOneArrayOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestUnnestVecBatchMultiType()
{
    const int32_t dataSize = 1;
    int32_t data1[dataSize] = {0};
    double data2[dataSize] = {0.1};
    std::string data3[dataSize] = {"abc"};
    int64_t data4[dataSize] = {10L};
    Decimal128 data5[dataSize] = {111111};
    int32_t data6[dataSize] = {0};
    int64_t data7[dataSize] = {10L};
    bool data8[dataSize] = {true};
    std::string data9[dataSize] = {"123"};
    int16_t data10[dataSize] = {0};

    std::vector<DataTypePtr> types = {
        IntType(),
        DoubleType(),
        VarcharType(10),
        LongType(),
        Decimal128Type(10, 2),
        Date32Type(),
        Decimal64DataType::Instance(),
        BooleanType(),
        CharDataType::Instance(),
        ShortType()
    };
    DataTypes sourceTypes(types);

    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10);

    // Create array vector
    VectorBatch* elementBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10);
    for (int32_t i = 0; i < 10; i++) {
        std::shared_ptr<BaseVector> element(elementBatch->Get(i));
        vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, element);
        arrayVector->SetOffset(0, 0);
        arrayVector->SetOffset(1, 1);
        batch->Append(arrayVector);
    }
    elementBatch->ClearVectors();
    VectorHelper::FreeVecBatch(elementBatch);
    return batch;
}

VectorBatch *CreateTestUnnestOutputVecBatchMultiType()
{
    const int32_t dataSize = 1;
    int32_t data1[dataSize] = {0};
    double data2[dataSize] = {0.1};
    std::string data3[dataSize] = {"abc"};
    int64_t data4[dataSize] = {10L};
    Decimal128 data5[dataSize] = {111111};
    int32_t data6[dataSize] = {0};
    int64_t data7[dataSize] = {10L};
    bool data8[dataSize] = {true};
    std::string data9[dataSize] = {"123"};
    int16_t data10[dataSize] = {0};

    int32_t data11[dataSize] = {0};
    double data12[dataSize] = {0.1};
    std::string data13[dataSize] = {"abc"};
    int64_t data14[dataSize] = {10L};
    Decimal128 data15[dataSize] = {111111};
    int32_t data16[dataSize] = {0};
    int64_t data17[dataSize] = {10L};
    bool data18[dataSize] = {true};
    std::string data19[dataSize] = {"123"};
    int16_t data20[dataSize] = {0};

    std::vector<DataTypePtr> types = {
        IntType(),
        DoubleType(),
        VarcharType(10),
        LongType(),
        Decimal128Type(10, 2),
        Date32Type(),
        Decimal64DataType::Instance(),
        BooleanType(),
        CharDataType::Instance(),
        ShortType(),
        IntType(),
        DoubleType(),
        VarcharType(10),
        LongType(),
        Decimal128Type(10, 2),
        Date32Type(),
        Decimal64DataType::Instance(),
        BooleanType(),
        CharDataType::Instance(),
        ShortType()
    };
    DataTypes sourceTypes(types);

    return CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
        data11, data12, data13, data14, data15, data16, data17, data18, data19, data20);
}

TEST(PipelineTest, TestUnestMultiType)
{
    std::vector<DataTypePtr> types = {
        IntType(),
        DoubleType(),
        VarcharType(10),
        LongType(),
        Decimal128Type(10, 2),
        Date32Type(),
        Decimal64DataType::Instance(),
        BooleanType(),
        CharDataType::Instance(),
        ShortType(),
        std::make_shared<ArrayType>(IntType()),
        std::make_shared<ArrayType>(DoubleType()),
        std::make_shared<ArrayType>(VarcharType(10)),
        std::make_shared<ArrayType>(LongType()),
        std::make_shared<ArrayType>(Decimal128Type(10, 2)),
        std::make_shared<ArrayType>(Date32Type()),
        std::make_shared<ArrayType>(Decimal64DataType::Instance()),
        std::make_shared<ArrayType>(BooleanType()),
        std::make_shared<ArrayType>(CharDataType::Instance()),
        std::make_shared<ArrayType>(ShortType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchMultiType();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, DoubleType());
    auto expr3 = new FieldExpr(2, VarcharType(10));
    auto expr4 = new FieldExpr(3, LongType());
    auto expr5 = new FieldExpr(4, Decimal128Type(10, 2));
    auto expr6 = new FieldExpr(5, Date32Type());
    auto expr7 = new FieldExpr(6, Decimal64DataType::Instance());
    auto expr8 = new FieldExpr(7, BooleanType());
    auto expr9 = new FieldExpr(8, CharDataType::Instance());
    auto expr10 = new FieldExpr(9, ShortType());
    auto expr11 = new FieldExpr(10, std::make_shared<ArrayType>(IntType()));
    auto expr12 = new FieldExpr(11, std::make_shared<ArrayType>(DoubleType()));
    auto expr13 = new FieldExpr(12, std::make_shared<ArrayType>(VarcharType(10)));
    auto expr14 = new FieldExpr(13, std::make_shared<ArrayType>(LongType()));
    auto expr15 = new FieldExpr(14, std::make_shared<ArrayType>(Decimal128Type(10, 2)));
    auto expr16 = new FieldExpr(15, std::make_shared<ArrayType>(Date32Type()));
    auto expr17 = new FieldExpr(16, std::make_shared<ArrayType>(Decimal64DataType::Instance()));
    auto expr18 = new FieldExpr(17, std::make_shared<ArrayType>(BooleanType()));
    auto expr19 = new FieldExpr(18, std::make_shared<ArrayType>(CharDataType::Instance()));
    auto expr20 = new FieldExpr(19, std::make_shared<ArrayType>(ShortType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2),
        static_cast<ExprPtr>(expr3),
        static_cast<ExprPtr>(expr4),
        static_cast<ExprPtr>(expr5),
        static_cast<ExprPtr>(expr6),
        static_cast<ExprPtr>(expr7),
        static_cast<ExprPtr>(expr8),
        static_cast<ExprPtr>(expr9),
        static_cast<ExprPtr>(expr10)
    };

    auto unnestVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr11),
        static_cast<ExprPtr>(expr12),
        static_cast<ExprPtr>(expr13),
        static_cast<ExprPtr>(expr14),
        static_cast<ExprPtr>(expr15),
        static_cast<ExprPtr>(expr16),
        static_cast<ExprPtr>(expr17),
        static_cast<ExprPtr>(expr18),
        static_cast<ExprPtr>(expr19),
        static_cast<ExprPtr>(expr20)
    };
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    auto task = std::make_shared<OmniTask>(planFragment, config::QueryConfig());
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task->Next(&future);
        if (!future.valid()) {
            vectorBatch = out;
            break;
        }
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }

    VectorBatch *expVecBatch = CreateTestUnnestOutputVecBatchMultiType();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9, expr10,
        expr11, expr12, expr13, expr14, expr15, expr16, expr17, expr18, expr19, expr20});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

}