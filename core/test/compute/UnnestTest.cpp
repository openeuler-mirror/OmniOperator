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
#include "vector/map_vector.h"
#include "vector/row_vector.h"
#include "type/data_type.h"

using namespace omniruntime;
using namespace TestUtil;
using OmniArrayType = omniruntime::type::ArrayType;
 
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

using TestVarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;

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
    int64_t data4[dataSize] = {0, 0, 1, 0, 1, 0, 0, 0, 1, 2};  // Ordinality starts from 0 (Spark SQL convention)

    std::vector<DataTypePtr> types = { IntType(), DoubleType(), IntType(), LongType() };
    DataTypes sourceTypes(types);
    return CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);
}

TEST(PipelineTest, TestUnestArray)
{
    std::vector<DataTypePtr> types = { IntType(), DoubleType(), std::make_shared<OmniArrayType>(IntType()) };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, DoubleType());
    auto expr3 = new FieldExpr(2, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2)};

    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr3) };
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, true);
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
        std::make_shared<OmniArrayType>(IntType()),
        std::make_shared<OmniArrayType>(IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithTwoArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto expr3 = new FieldExpr(2, std::make_shared<OmniArrayType>(IntType()));
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
        std::make_shared<OmniArrayType>(IntType()),
        std::make_shared<OmniArrayType>(IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithTwoArray();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto expr3 = new FieldExpr(2, std::make_shared<OmniArrayType>(IntType()));
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
        std::make_shared<OmniArrayType>(IntType()),
        std::make_shared<OmniArrayType>(DoubleType()),
        std::make_shared<OmniArrayType>(VarcharType(10)),
        std::make_shared<OmniArrayType>(LongType()),
        std::make_shared<OmniArrayType>(Decimal128Type(10, 2)),
        std::make_shared<OmniArrayType>(Date32Type()),
        std::make_shared<OmniArrayType>(Decimal64DataType::Instance()),
        std::make_shared<OmniArrayType>(BooleanType()),
        std::make_shared<OmniArrayType>(CharDataType::Instance()),
        std::make_shared<OmniArrayType>(ShortType())
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
    auto expr11 = new FieldExpr(10, std::make_shared<OmniArrayType>(IntType()));
    auto expr12 = new FieldExpr(11, std::make_shared<OmniArrayType>(DoubleType()));
    auto expr13 = new FieldExpr(12, std::make_shared<OmniArrayType>(VarcharType(10)));
    auto expr14 = new FieldExpr(13, std::make_shared<OmniArrayType>(LongType()));
    auto expr15 = new FieldExpr(14, std::make_shared<OmniArrayType>(Decimal128Type(10, 2)));
    auto expr16 = new FieldExpr(15, std::make_shared<OmniArrayType>(Date32Type()));
    auto expr17 = new FieldExpr(16, std::make_shared<OmniArrayType>(Decimal64DataType::Instance()));
    auto expr18 = new FieldExpr(17, std::make_shared<OmniArrayType>(BooleanType()));
    auto expr19 = new FieldExpr(18, std::make_shared<OmniArrayType>(CharDataType::Instance()));
    auto expr20 = new FieldExpr(19, std::make_shared<OmniArrayType>(ShortType()));
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


VectorBatch *CreateTestUnnestVecBatchWithMap()
{
    const int32_t dataSize = 3;
    int32_t data1[dataSize] = {0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    // Create map vector {null, {2: 2, 4: 4}, {3: 3}}
    int32_t elementSize = 4;
    auto keyVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    auto valueVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    keyVector->SetNull(0);
    keyVector->SetValue(1, 2);
    keyVector->SetValue(2, 4);
    keyVector->SetValue(3, 3);
    valueVector->SetNull(0);
    valueVector->SetValue(1, 2);
    valueVector->SetValue(2, 4);
    valueVector->SetValue(3, 3);
    vec::MapVector* mapVector = new vec::MapVector(dataSize, keyVector, valueVector);
    mapVector->SetOffset(0, 0);
    mapVector->SetOffset(1, 1);
    mapVector->SetOffset(2, 3);
    mapVector->SetOffset(3, 4);
    batch->Append(mapVector);

    // Create map vector {{1: 1}, {2: 2}, {3: 3}}
    int32_t elementSize1 = 3;
    auto keyVector1 = std::make_shared<vec::Vector<int32_t>>(elementSize1);
    auto valueVector1 = std::make_shared<vec::Vector<int32_t>>(elementSize1);
    keyVector1->SetValue(0, 1);
    keyVector1->SetValue(1, 2);
    keyVector1->SetValue(2, 3);
    valueVector1->SetValue(0, 1);
    valueVector1->SetValue(1, 2);
    valueVector1->SetValue(2, 3);
    vec::MapVector* mapVector1 = new vec::MapVector(dataSize, keyVector1, valueVector1);
    mapVector1->SetOffset(0, 0);
    mapVector1->SetOffset(1, 1);
    mapVector1->SetOffset(2, 2);
    mapVector1->SetOffset(3, 3);
    batch->Append(mapVector1);
    return batch;
}

VectorBatch *CreateTestUnnestOutputVecBatchWithMap()
{
    const int32_t dataSize = 3;
    int32_t data1[dataSize] = {0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4};
    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    // Create map vector {null, {2: 2, 4: 4}, {3: 3}}
    int32_t elementSize = 4;
    auto keyVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    auto valueVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    keyVector->SetNull(0);
    keyVector->SetValue(1, 2);
    keyVector->SetValue(2, 4);
    keyVector->SetValue(3, 3);
    valueVector->SetNull(0);
    valueVector->SetValue(1, 2);
    valueVector->SetValue(2, 4);
    valueVector->SetValue(3, 3);
    vec::MapVector* mapVector = new vec::MapVector(dataSize, keyVector, valueVector);
    mapVector->SetOffset(0, 0);
    mapVector->SetOffset(1, 1);
    mapVector->SetOffset(2, 3);
    mapVector->SetOffset(3, 4);
    batch->Append(mapVector);

    int32_t elementSize1 = 3;
    auto vector1 = new vec::Vector<int32_t>(elementSize1);
    vector1->SetValue(0, 1);
    vector1->SetValue(1, 2);
    vector1->SetValue(2, 3);
    auto vector2 = new vec::Vector<int32_t>(elementSize1);
    vector2->SetValue(0, 1);
    vector2->SetValue(1, 2);
    vector2->SetValue(2, 3);
    batch->Append(vector1);
    batch->Append(vector2);
    return batch;
}

TEST(PipelineTest, TestUnestMap)
{
    std::vector<DataTypePtr> types = {
        IntType(),
        DoubleType(),
        std::make_shared<MapType>(IntType(), IntType()),
        std::make_shared<MapType>(IntType(), IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestVecBatchWithMap();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, DoubleType());
    auto expr3 = new FieldExpr(2, std::make_shared<MapType>(IntType(), IntType()));
    auto expr4 = new FieldExpr(3, std::make_shared<MapType>(IntType(), IntType()));
    auto replicateVariables = std::vector<ExprPtr>{
        static_cast<ExprPtr>(expr1),
        static_cast<ExprPtr>(expr2),
        static_cast<ExprPtr>(expr3)
    };

    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr4) };
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

    VectorBatch *expVecBatch = CreateTestUnnestOutputVecBatchWithMap();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    Expr::DeleteExprs({expr1, expr2, expr3, expr4});
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// ==================== Outer Support Tests ====================

// Test explode (outer=false): null and empty arrays should be filtered
VectorBatch *CreateTestExplodeVecBatchWithNullEmpty()
{
    const int32_t dataSize = 4;
    int32_t data1[dataSize] = {1, 2, 3, 4};
    
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1);
    
    // Create array vector: [null, [], [1, 2], [3]]
    int32_t elementSize = 3;
    auto elementVector = std::make_shared<vec::Vector<int32_t>>(elementSize);
    elementVector->SetValue(0, 1);
    elementVector->SetValue(1, 2);
    elementVector->SetValue(2, 3);
    
    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementVector);
    // Row 0: null array
    arrayVector->SetNull(0);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 0);  // null array: offset stays at 0
    // Row 1: empty array []
    arrayVector->SetNotNull(1);
    arrayVector->SetOffset(1, 0);
    arrayVector->SetOffset(2, 0);  // empty array: offset stays at 0
    // Row 2: [1, 2]
    arrayVector->SetNotNull(2);
    arrayVector->SetOffset(2, 0);
    arrayVector->SetOffset(3, 2);  // [1, 2]: offset goes from 0 to 2
    // Row 3: [3]
    arrayVector->SetNotNull(3);
    arrayVector->SetOffset(3, 2);
    arrayVector->SetOffset(4, 3);  // [3]: offset goes from 2 to 3
    
    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestExplodeOutputVecBatchWithNullEmpty()
{
    // Expected output: only rows with non-empty arrays
    // Row 2: [1, 2] -> 2 output rows (id=3)
    // Row 3: [3] -> 1 output row (id=4)
    const int32_t dataSize = 3;
    int32_t data1[dataSize] = {3, 3, 4};  // Replicated id column (from input row 2 and 3)
    int32_t data2[dataSize] = {1, 2, 3};  // Unnested array elements
    
    std::vector<DataTypePtr> types = { IntType(), IntType() };
    DataTypes sourceTypes(types);
    return CreateVectorBatch(sourceTypes, dataSize, data1, data2);
}

TEST(PipelineTest, TestExplodeFilterNullEmpty)
{
    std::vector<DataTypePtr> types = { 
        IntType(), 
        std::make_shared<OmniArrayType>(IntType()) 
    };
    VectorBatch *vecBatch = CreateTestExplodeVecBatchWithNullEmpty();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);
    
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);
    
    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    
    // outer=false (default)
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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
    
    VectorBatch *expVecBatch = CreateTestExplodeOutputVecBatchWithNullEmpty();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test explode_outer (outer=true): null and empty arrays should be preserved
VectorBatch *CreateTestExplodeOuterOutputVecBatchWithNullEmpty()
{
    // Expected output: all rows, with null values for null/empty arrays
    // Row 0: null -> 1 output row with null
    // Row 1: [] -> 1 output row with null
    // Row 2: [1, 2] -> 2 output rows
    // Row 3: [3] -> 1 output row
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {1, 2, 3, 3, 4};  // Replicated id column
    int32_t data2[dataSize] = {0, 0, 1, 2, 3};  // Unnested array elements (0 means null)
    bool nullMask[dataSize] = {true, true, false, false, false};  // First two are null
    
    std::vector<DataTypePtr> types = { IntType(), IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    
    // Set null mask for the unnested column
    auto* unnestCol = dynamic_cast<vec::Vector<int32_t>*>(batch->Get(1));
    for (int32_t i = 0; i < dataSize; ++i) {
        if (nullMask[i]) {
            unnestCol->SetNull(i);
        }
    }
    
    return batch;
}

TEST(PipelineTest, TestExplodeOuterPreserveNullEmpty)
{
    std::vector<DataTypePtr> types = { 
        IntType(), 
        std::make_shared<OmniArrayType>(IntType()) 
    };
    VectorBatch *vecBatch = CreateTestExplodeVecBatchWithNullEmpty();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);
    
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);
    
    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    
    // outer=true
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, false, true);
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
    
    VectorBatch *expVecBatch = CreateTestExplodeOuterOutputVecBatchWithNullEmpty();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test posexplode with ordinality
VectorBatch *CreateTestPosexplodeOutputVecBatch()
{
    const int32_t dataSize = 3;
    int32_t data1[dataSize] = {3, 3, 4};  // Replicated id
    int32_t data2[dataSize] = {1, 2, 3};  // Unnested elements
    int64_t data3[dataSize] = {0, 1, 0};  // Ordinality (0-indexed, Spark SQL convention)
    
    std::vector<DataTypePtr> types = { IntType(), IntType(), LongType() };
    DataTypes sourceTypes(types);
    return CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);
}

TEST(PipelineTest, TestPosexplodeWithOrdinality)
{
    std::vector<DataTypePtr> types = { 
        IntType(), 
        std::make_shared<OmniArrayType>(IntType()) 
    };
    VectorBatch *vecBatch = CreateTestExplodeVecBatchWithNullEmpty();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);
    
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);
    
    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    
    // withOrdinality=true, outer=false
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, true, false);
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
    
    VectorBatch *expVecBatch = CreateTestPosexplodeOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test posexplode_outer (withOrdinality=true, outer=true): null array should have NULL pos and NULL val
VectorBatch *CreateTestPosexplodeOuterOutputVecBatchWithNullArray()
{
    // Expected output: all rows, with NULL pos and NULL val for null/empty arrays
    // Row 0: null -> 1 output row with NULL pos and NULL val
    // Row 1: [] -> 1 output row with NULL pos and NULL val
    // Row 2: [1, 2] -> 2 output rows with pos=0,1 and val=1,2
    // Row 3: [3] -> 1 output row with pos=0 and val=3
    // According to Spark SQL, both null arrays and empty arrays produce NULL pos
    // for posexplode_outer.
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {1, 2, 3, 3, 4};  // Replicated id column
    int32_t data2[dataSize] = {0, 0, 1, 2, 3};  // Unnested array elements
    int64_t data3[dataSize] = {0, 0, 0, 1, 0};  // Ordinality (0-indexed, masked to NULL for null/empty arrays)
    bool nullMaskVal[dataSize] = {true, true, false, false, false};  // First two are null (null array and empty array both have null val)
    bool nullMaskPos[dataSize] = {true, true, false, false, false};  // Null and empty array rows both have NULL pos
    
    std::vector<DataTypePtr> types = { IntType(), IntType(), LongType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);
    
    // Set null mask for the unnested column (val)
    auto* unnestCol = dynamic_cast<vec::Vector<int32_t>*>(batch->Get(1));
    for (int32_t i = 0; i < dataSize; ++i) {
        if (nullMaskVal[i]) {
            unnestCol->SetNull(i);
        }
    }
    
    // Set null mask for the ordinality column (pos) - null and empty arrays have NULL pos
    auto* ordCol = dynamic_cast<vec::Vector<int64_t>*>(batch->Get(2));
    for (int32_t i = 0; i < dataSize; ++i) {
        if (nullMaskPos[i]) {
            ordCol->SetNull(i);
        }
    }
    
    return batch;
}

TEST(PipelineTest, TestPosexplodeOuterWithNullArray)
{
    std::vector<DataTypePtr> types = { 
        IntType(), 
        std::make_shared<OmniArrayType>(IntType()) 
    };
    VectorBatch *vecBatch = CreateTestExplodeVecBatchWithNullEmpty();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);
    
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);
    
    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    
    // withOrdinality=true, outer=true
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, true, true);
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
    
    VectorBatch *expVecBatch = CreateTestPosexplodeOuterOutputVecBatchWithNullArray();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test stack with NULL values: NULL values should be output as NULL, not empty strings
VectorBatch *CreateTestStackVecBatchWithNull()
{
    const int32_t dataSize = 1;
    int32_t data1[dataSize] = {1};
    
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, data1);
    
    // Create array vector: [['A', 'B', NULL, 'D']]
    // This simulates stack(4, 'A', 'B', NULL, 'D')
    int32_t elementSize = 4;
    auto elementVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(elementSize);
    std::string_view svA("A");
    std::string_view svB("B");
    std::string_view svD("D");
    elementVector->SetValue(0, svA);
    elementVector->SetValue(1, svB);
    elementVector->SetNull(2);  // NULL value
    elementVector->SetValue(3, svD);
    
    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementVector);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 4);
    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestStackOutputVecBatchWithNull()
{
    const int32_t dataSize = 4;
    
    // Create batch manually
    VectorBatch* batch = new VectorBatch(dataSize);
    
    // Create int column
    int32_t data1[dataSize] = {1, 1, 1, 1};  // Replicated id
    std::vector<DataTypePtr> intTypes = { IntType() };
    DataTypes intSourceTypes(intTypes);
    VectorBatch* intBatch = CreateVectorBatch(intSourceTypes, dataSize, data1);
    batch->Append(intBatch->Get(0));
    intBatch->ClearVectors();
    VectorHelper::FreeVecBatch(intBatch);
    
    // Create string column manually
    // VectorBatch will take ownership and delete it in destructor
    BaseVector* stringVector = VectorHelper::CreateStringVector(dataSize);
    auto* stackCol = dynamic_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(stringVector);
    
    std::string_view svA("A");
    std::string_view svB("B");
    std::string_view svD("D");
    stackCol->SetValue(0, svA);
    stackCol->SetValue(1, svB);
    stackCol->SetNull(2);  // Third value is NULL
    stackCol->SetValue(3, svD);
    
    batch->Append(stringVector);  // Ownership transferred to batch
    
    return batch;
}

TEST(PipelineTest, TestStackWithNullValues)
{
    std::vector<DataTypePtr> types = { 
        IntType(), 
        std::make_shared<OmniArrayType>(VarcharType(10)) 
    };
    VectorBatch *vecBatch = CreateTestStackVecBatchWithNull();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);
    
    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);
    
    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(VarcharType(10)));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    
    // outer=false (default for stack)
    auto unnestNode = std::make_shared<const UnnestNode>("unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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
    
    VectorBatch *expVecBatch = CreateTestStackOutputVecBatchWithNull();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Simulates stack(2, c_map): one input row, array of [map, null] -> two output rows
VectorBatch *CreateTestStackArrayOfMapInputVecBatch()
{
    const int32_t dataSize = 1;
    int32_t idData[dataSize] = { 42 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    auto keys = std::make_shared<TestVarcharVector>(1);
    keys->SetValue(0, std::string_view("k"));
    auto vals = std::make_shared<vec::Vector<int32_t>>(1);
    vals->SetValue(0, 100);
    auto *middleMap = new vec::MapVector(2, keys, vals);
    middleMap->SetOffset(0, 0);
    middleMap->SetOffset(1, 1);
    middleMap->SetOffset(2, 1);
    middleMap->SetNull(1);

    auto *outerArray = new vec::ArrayVector(dataSize, std::shared_ptr<vec::BaseVector>(middleMap));
    outerArray->SetOffset(0, 0);
    outerArray->SetOffset(1, 2);
    batch->Append(outerArray);
    return batch;
}

VectorBatch *CreateTestStackArrayOfMapOutputVecBatch()
{
    const int32_t outRows = 2;
    int32_t idData[outRows] = { 42, 42 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, outRows, idData);

    auto expKeys = std::make_shared<TestVarcharVector>(1);
    expKeys->SetValue(0, std::string_view("k"));
    auto expVals = std::make_shared<vec::Vector<int32_t>>(1);
    expVals->SetValue(0, 100);
    auto *outMap = new vec::MapVector(outRows, expKeys, expVals);
    outMap->SetOffset(0, 0);
    outMap->SetOffset(1, 1);
    outMap->SetOffset(2, 1);
    outMap->SetNull(1);
    batch->Append(outMap);
    return batch;
}

TEST(PipelineTest, TestStackUnnestArrayOfMap)
{
    auto mapElemType = std::make_shared<type::MapType>(VarcharType(10), IntType());
    std::vector<DataTypePtr> types = { IntType(), std::make_shared<OmniArrayType>(mapElemType) };
    VectorBatch *vecBatch = CreateTestStackArrayOfMapInputVecBatch();
    std::vector<VectorBatch *> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(mapElemType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{ unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet };
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

    VectorBatch *expVecBatch = CreateTestStackArrayOfMapOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr *> exprsToDelete = { expr1, expr2 };
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestStackArrayOfNestedArrayInputVecBatch()
{
    const int32_t dataSize = 1;
    int32_t idData[dataSize] = { 7 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    auto innerElem = std::make_shared<vec::Vector<int32_t>>(2);
    innerElem->SetValue(0, 1);
    innerElem->SetValue(1, 2);
    auto *middle = new vec::ArrayVector(2, innerElem);
    middle->SetOffset(0, 0);
    middle->SetOffset(1, 2);
    middle->SetNull(1);
    middle->SetOffset(2, 2);

    auto *outer = new vec::ArrayVector(dataSize, std::shared_ptr<vec::BaseVector>(middle));
    outer->SetOffset(0, 0);
    outer->SetOffset(1, 2);
    batch->Append(outer);
    return batch;
}

VectorBatch *CreateTestStackArrayOfNestedArrayOutputVecBatch()
{
    const int32_t outRows = 2;
    int32_t idData[outRows] = { 7, 7 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, outRows, idData);

    auto outInner = std::make_shared<vec::Vector<int32_t>>(2);
    outInner->SetValue(0, 1);
    outInner->SetValue(1, 2);
    auto *outArr = new vec::ArrayVector(outRows, outInner);
    outArr->SetOffset(0, 0);
    outArr->SetOffset(1, 2);
    outArr->SetOffset(2, 2);
    outArr->SetNull(1);
    batch->Append(outArr);
    return batch;
}

TEST(PipelineTest, TestStackUnnestArrayOfArray)
{
    auto innerArrayType = std::make_shared<OmniArrayType>(IntType());
    std::vector<DataTypePtr> types = { IntType(), std::make_shared<OmniArrayType>(innerArrayType) };
    VectorBatch *vecBatch = CreateTestStackArrayOfNestedArrayInputVecBatch();
    std::vector<VectorBatch *> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(innerArrayType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{ unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet };
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

    VectorBatch *expVecBatch = CreateTestStackArrayOfNestedArrayOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr *> exprsToDelete = { expr1, expr2 };
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestStackArrayOfStructInputVecBatch()
{
    const int32_t dataSize = 1;
    int32_t idData[dataSize] = { 9 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    auto nameCol = std::make_shared<TestVarcharVector>(2);
    auto ageCol = std::make_shared<vec::Vector<int32_t>>(2);
    nameCol->SetValue(0, std::string_view("a"));
    ageCol->SetValue(0, 42);
    nameCol->SetNull(1);
    ageCol->SetNull(1);
    std::vector<std::shared_ptr<vec::BaseVector>> structKids = { nameCol, ageCol };
    auto *elemRow = new vec::RowVector(2, structKids);
    elemRow->SetNotNull(0);
    elemRow->SetNull(1);

    auto *outer = new vec::ArrayVector(dataSize, std::shared_ptr<vec::BaseVector>(elemRow));
    outer->SetOffset(0, 0);
    outer->SetOffset(1, 2);
    batch->Append(outer);
    return batch;
}

VectorBatch *CreateTestStackArrayOfStructOutputVecBatch()
{
    const int32_t outRows = 2;
    int32_t idData[outRows] = { 9, 9 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, outRows, idData);

    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outRows);
    auto ageVector = new vec::Vector<int32_t>(outRows);
    nameVector->SetValue(0, std::string_view("a"));
    ageVector->SetValue(0, 42);
    nameVector->SetNull(1);
    ageVector->SetNull(1);
    std::vector<std::shared_ptr<vec::BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<vec::BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<vec::BaseVector>(ageVector));
    auto *outputRowVector = new vec::RowVector(outRows, structChildren);
    outputRowVector->SetNotNull(0);
    outputRowVector->SetNull(1);
    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestStackUnnestArrayOfStruct)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = { IntType(), std::make_shared<OmniArrayType>(structType) };
    VectorBatch *vecBatch = CreateTestStackArrayOfStructInputVecBatch();
    std::vector<VectorBatch *> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{ unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet };
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

    VectorBatch *expVecBatch = CreateTestStackArrayOfStructOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr *> exprsToDelete = { expr1, expr2 };
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

VectorBatch *CreateTestUnnestReplicateStructInputVecBatch()
{
    const int32_t dataSize = 1;
    int32_t idData[dataSize] = { 1 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    auto repName = std::make_shared<TestVarcharVector>(1);
    repName->SetValue(0, std::string_view("hi"));
    auto repAge = std::make_shared<vec::Vector<int32_t>>(1);
    repAge->SetValue(0, 5);
    std::vector<std::shared_ptr<vec::BaseVector>> repKids = { repName, repAge };
    batch->Append(new vec::RowVector(1, repKids));

    auto arrElem = std::make_shared<vec::Vector<int32_t>>(2);
    arrElem->SetValue(0, 100);
    arrElem->SetValue(1, 200);
    auto *arrVec = new vec::ArrayVector(1, arrElem);
    arrVec->SetOffset(0, 0);
    arrVec->SetOffset(1, 2);
    batch->Append(arrVec);
    return batch;
}

VectorBatch *CreateTestUnnestReplicateStructOutputVecBatch()
{
    const int32_t outRows = 2;
    int32_t idData[outRows] = { 1, 1 };
    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch *batch = CreateVectorBatch(sourceTypes, outRows, idData);

    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outRows);
    auto ageVector = new vec::Vector<int32_t>(outRows);
    nameVector->SetValue(0, std::string_view("hi"));
    ageVector->SetValue(0, 5);
    nameVector->SetValue(1, std::string_view("hi"));
    ageVector->SetValue(1, 5);
    std::vector<std::shared_ptr<vec::BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<vec::BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<vec::BaseVector>(ageVector));
    batch->Append(new vec::RowVector(outRows, structChildren));

    int32_t unnestVals[outRows] = { 100, 200 };
    std::vector<DataTypePtr> intOnly = { IntType() };
    DataTypes intTypes(intOnly);
    VectorBatch *intPart = CreateVectorBatch(intTypes, outRows, unnestVals);
    batch->Append(intPart->Get(0));
    intPart->ClearVectors();
    VectorHelper::FreeVecBatch(intPart);
    return batch;
}

TEST(PipelineTest, TestUnnestReplicateStructColumn)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        structType,
        std::make_shared<OmniArrayType>(IntType())
    };
    VectorBatch *vecBatch = CreateTestUnnestReplicateStructInputVecBatch();
    std::vector<VectorBatch *> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, structType);
    auto expr3 = new FieldExpr(2, std::make_shared<OmniArrayType>(IntType()));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1), static_cast<ExprPtr>(expr2) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr3) };
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{ unnestNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet };
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

    VectorBatch *expVecBatch = CreateTestUnnestReplicateStructOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr *> exprsToDelete = { expr1, expr2, expr3 };
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// ==================== Inline Support Tests ====================

// Test inline (outer=false): ARRAY<STRUCT<name:STRING, age:INT>> -> name, age columns
VectorBatch *CreateTestInlineVecBatch()
{
    const int32_t dataSize = 4;
    int32_t idData[dataSize] = {1, 2, 3, 4};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    // Build element RowVector for ARRAY<STRUCT<name:VARCHAR, age:INT>>
    // Total struct elements: row0=[{Alice,25},{Bob,30}], row1=null, row2=[], row3=[{Dave,40}]
    // -> 3 struct elements total (from row0 and row3)
    int32_t totalElements = 3;
    auto nameVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(totalElements);
    auto ageVector = std::make_shared<vec::Vector<int32_t>>(totalElements);

    nameVector->SetValue(0, std::string_view("Alice"));
    ageVector->SetValue(0, 25);
    nameVector->SetValue(1, std::string_view("Bob"));
    ageVector->SetValue(1, 30);
    nameVector->SetValue(2, std::string_view("Dave"));
    ageVector->SetValue(2, 40);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(nameVector);
    structChildren.push_back(ageVector);
    auto elementRowVector = std::make_shared<vec::RowVector>(totalElements, structChildren);

    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementRowVector);
    // Row 0: [{Alice,25},{Bob,30}] -> offset 0..2
    arrayVector->SetNotNull(0);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    // Row 1: null
    arrayVector->SetNull(1);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 2);
    // Row 2: [] (empty array)
    arrayVector->SetNotNull(2);
    arrayVector->SetOffset(2, 2);
    arrayVector->SetOffset(3, 2);
    // Row 3: [{Dave,40}] -> offset 2..3
    arrayVector->SetNotNull(3);
    arrayVector->SetOffset(3, 2);
    arrayVector->SetOffset(4, 3);

    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestInlineOutputVecBatch()
{
    // inline (outer=false): null and empty arrays filtered
    // Row 0: [{Alice,25},{Bob,30}] -> 2 output rows, id=1
    // Row 3: [{Dave,40}] -> 1 output row, id=4
    const int32_t outSize = 3;
    int32_t idData[outSize] = {1, 1, 4};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, outSize, idData);

    // Output struct column as RowVector
    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outSize);
    auto ageVector = new vec::Vector<int32_t>(outSize);

    nameVector->SetValue(0, std::string_view("Alice"));
    ageVector->SetValue(0, 25);
    nameVector->SetValue(1, std::string_view("Bob"));
    ageVector->SetValue(1, 30);
    nameVector->SetValue(2, std::string_view("Dave"));
    ageVector->SetValue(2, 40);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<BaseVector>(ageVector));
    auto outputRowVector = new vec::RowVector(outSize, structChildren);

    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestInline)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<OmniArrayType>(structType)
    };
    VectorBatch *vecBatch = CreateTestInlineVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };

    // outer=false
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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

    VectorBatch *expVecBatch = CreateTestInlineOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test inline_outer (outer=true): null and empty arrays produce NULL struct rows
VectorBatch *CreateTestInlineOuterOutputVecBatch()
{
    // inline_outer (outer=true): all input rows appear in output
    // Row 0: [{Alice,25},{Bob,30}] -> 2 output rows, id=1
    // Row 1: null array -> 1 output row with NULL struct, id=2
    // Row 2: [] empty array -> 1 output row with NULL struct, id=3
    // Row 3: [{Dave,40}] -> 1 output row, id=4
    const int32_t outSize = 5;
    int32_t idData[outSize] = {1, 1, 2, 3, 4};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, outSize, idData);

    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outSize);
    auto ageVector = new vec::Vector<int32_t>(outSize);

    nameVector->SetValue(0, std::string_view("Alice"));
    ageVector->SetValue(0, 25);
    nameVector->SetValue(1, std::string_view("Bob"));
    ageVector->SetValue(1, 30);
    // Row 1 (null array) and Row 2 (empty array) -> null struct fields
    nameVector->SetNull(2);
    ageVector->SetNull(2);
    nameVector->SetNull(3);
    ageVector->SetNull(3);
    nameVector->SetValue(4, std::string_view("Dave"));
    ageVector->SetValue(4, 40);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<BaseVector>(ageVector));
    auto outputRowVector = new vec::RowVector(outSize, structChildren);
    outputRowVector->SetNull(2);
    outputRowVector->SetNull(3);

    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestInlineOuter)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<OmniArrayType>(structType)
    };
    VectorBatch *vecBatch = CreateTestInlineVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };

    // outer=true
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, true);
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

    VectorBatch *expVecBatch = CreateTestInlineOuterOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test inline with null struct elements inside the array
VectorBatch *CreateTestInlineNullElementsVecBatch()
{
    const int32_t dataSize = 2;
    int32_t idData[dataSize] = {1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    // Row 0: [{Alice,25}, null], Row 1: [{Charlie,35}]
    int32_t totalElements = 3;
    auto nameVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(totalElements);
    auto ageVector = std::make_shared<vec::Vector<int32_t>>(totalElements);

    nameVector->SetValue(0, std::string_view("Alice"));
    ageVector->SetValue(0, 25);
    // Element 1 is a null struct
    nameVector->SetNull(1);
    ageVector->SetNull(1);
    nameVector->SetValue(2, std::string_view("Charlie"));
    ageVector->SetValue(2, 35);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(nameVector);
    structChildren.push_back(ageVector);
    auto elementRowVector = std::make_shared<vec::RowVector>(totalElements, structChildren);
    elementRowVector->SetNull(1);

    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementRowVector);
    arrayVector->SetNotNull(0);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetNotNull(1);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 3);

    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestInlineNullElementsOutputVecBatch()
{
    // Row 0: [{Alice,25}, null] -> 2 output rows (id=1,1), second struct is null
    // Row 1: [{Charlie,35}] -> 1 output row (id=2)
    const int32_t outSize = 3;
    int32_t idData[outSize] = {1, 1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, outSize, idData);

    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outSize);
    auto ageVector = new vec::Vector<int32_t>(outSize);

    nameVector->SetValue(0, std::string_view("Alice"));
    ageVector->SetValue(0, 25);
    nameVector->SetNull(1);
    ageVector->SetNull(1);
    nameVector->SetValue(2, std::string_view("Charlie"));
    ageVector->SetValue(2, 35);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<BaseVector>(ageVector));
    auto outputRowVector = new vec::RowVector(outSize, structChildren);
    outputRowVector->SetNull(1);

    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestInlineWithNullStructElements)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<OmniArrayType>(structType)
    };
    VectorBatch *vecBatch = CreateTestInlineNullElementsVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };

    // outer=false
    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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

    VectorBatch *expVecBatch = CreateTestInlineNullElementsOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test inline with numeric struct fields (INT, DOUBLE)
VectorBatch *CreateTestInlineNumericVecBatch()
{
    const int32_t dataSize = 2;
    int32_t idData[dataSize] = {10, 20};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    // Row 0: [{1, 1.1}, {2, 2.2}], Row 1: [{3, 3.3}]
    int32_t totalElements = 3;
    auto col1Vector = std::make_shared<vec::Vector<int32_t>>(totalElements);
    auto col2Vector = std::make_shared<vec::Vector<double>>(totalElements);

    col1Vector->SetValue(0, 1);
    col2Vector->SetValue(0, 1.1);
    col1Vector->SetValue(1, 2);
    col2Vector->SetValue(1, 2.2);
    col1Vector->SetValue(2, 3);
    col2Vector->SetValue(2, 3.3);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(col1Vector);
    structChildren.push_back(col2Vector);
    auto elementRowVector = std::make_shared<vec::RowVector>(totalElements, structChildren);

    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementRowVector);
    arrayVector->SetNotNull(0);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetNotNull(1);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 3);

    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestInlineNumericOutputVecBatch()
{
    const int32_t outSize = 3;
    int32_t idData[outSize] = {10, 10, 20};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, outSize, idData);

    auto col1Vector = new vec::Vector<int32_t>(outSize);
    auto col2Vector = new vec::Vector<double>(outSize);
    col1Vector->SetValue(0, 1);
    col2Vector->SetValue(0, 1.1);
    col1Vector->SetValue(1, 2);
    col2Vector->SetValue(1, 2.2);
    col1Vector->SetValue(2, 3);
    col2Vector->SetValue(2, 3.3);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<BaseVector>(col1Vector));
    structChildren.push_back(std::shared_ptr<BaseVector>(col2Vector));
    auto outputRowVector = new vec::RowVector(outSize, structChildren);

    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestInlineNumericStruct)
{
    std::vector<DataTypePtr> rowFieldTypes = { IntType(), DoubleType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<OmniArrayType>(structType)
    };
    VectorBatch *vecBatch = CreateTestInlineNumericVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };

    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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

    VectorBatch *expVecBatch = CreateTestInlineNumericOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

// Test inline with null FIELDS inside non-null structs (the struct itself is NOT null)
VectorBatch *CreateTestInlineNullFieldsVecBatch()
{
    const int32_t dataSize = 2;
    int32_t idData[dataSize] = {1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, dataSize, idData);

    // Row 0: [{NULL, 40}, {Eve, NULL}], Row 1: [{Alice, 25}]
    int32_t totalElements = 3;
    auto nameVector = std::make_shared<vec::Vector<vec::LargeStringContainer<std::string_view>>>(totalElements);
    auto ageVector = std::make_shared<vec::Vector<int32_t>>(totalElements);

    // Element 0: {name=NULL, age=40} -- struct NOT null, but name field is null
    nameVector->SetNull(0);
    ageVector->SetValue(0, 40);
    // Element 1: {name=Eve, age=NULL} -- struct NOT null, but age field is null
    nameVector->SetValue(1, std::string_view("Eve"));
    ageVector->SetNull(1);
    // Element 2: {name=Alice, age=25}
    nameVector->SetValue(2, std::string_view("Alice"));
    ageVector->SetValue(2, 25);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(nameVector);
    structChildren.push_back(ageVector);
    auto elementRowVector = std::make_shared<vec::RowVector>(totalElements, structChildren);

    vec::ArrayVector* arrayVector = new vec::ArrayVector(dataSize, elementRowVector);
    arrayVector->SetNotNull(0);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetNotNull(1);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 3);

    batch->Append(arrayVector);
    return batch;
}

VectorBatch *CreateTestInlineNullFieldsOutputVecBatch()
{
    // Row 0: [{NULL,40},{Eve,NULL}] -> 2 output rows (id=1,1)
    // Row 1: [{Alice,25}] -> 1 output row (id=2)
    const int32_t outSize = 3;
    int32_t idData[outSize] = {1, 1, 2};

    std::vector<DataTypePtr> types = { IntType() };
    DataTypes sourceTypes(types);
    VectorBatch* batch = CreateVectorBatch(sourceTypes, outSize, idData);

    auto nameVector = new vec::Vector<vec::LargeStringContainer<std::string_view>>(outSize);
    auto ageVector = new vec::Vector<int32_t>(outSize);

    // {NULL, 40}: name is NULL, age is 40
    nameVector->SetNull(0);
    ageVector->SetValue(0, 40);
    // {Eve, NULL}: name is Eve, age is NULL
    nameVector->SetValue(1, std::string_view("Eve"));
    ageVector->SetNull(1);
    // {Alice, 25}
    nameVector->SetValue(2, std::string_view("Alice"));
    ageVector->SetValue(2, 25);

    std::vector<std::shared_ptr<BaseVector>> structChildren;
    structChildren.push_back(std::shared_ptr<BaseVector>(nameVector));
    structChildren.push_back(std::shared_ptr<BaseVector>(ageVector));
    auto outputRowVector = new vec::RowVector(outSize, structChildren);

    batch->Append(outputRowVector);
    return batch;
}

TEST(PipelineTest, TestInlineWithNullFields)
{
    std::vector<DataTypePtr> rowFieldTypes = { VarcharType(50), IntType() };
    auto structType = std::make_shared<type::RowType>(rowFieldTypes);
    std::vector<DataTypePtr> types = {
        IntType(),
        std::make_shared<OmniArrayType>(structType)
    };
    VectorBatch *vecBatch = CreateTestInlineNullFieldsVecBatch();
    std::vector<VectorBatch*> inputVector;
    inputVector.push_back(vecBatch);

    auto sourceBatchIterator = std::make_unique<TestBatchIterator>(inputVector);
    auto resIterator = std::make_shared<ResultIterator>(std::move(sourceBatchIterator));
    auto outTypes = std::make_shared<DataTypes>(types);
    auto valueStreamNode = std::make_shared<const ValueStreamNode>("value_stream", outTypes, resIterator);

    auto expr1 = new FieldExpr(0, IntType());
    auto expr2 = new FieldExpr(1, std::make_shared<OmniArrayType>(structType));
    auto replicateVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr1) };
    auto unnestVariables = std::vector<ExprPtr>{ static_cast<ExprPtr>(expr2) };

    auto unnestNode = std::make_shared<const UnnestNode>(
        "unnest", replicateVariables, unnestVariables, valueStreamNode, false, false);
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

    VectorBatch *expVecBatch = CreateTestInlineNullFieldsOutputVecBatch();
    EXPECT_TRUE(VecBatchMatch(vectorBatch, expVecBatch));
    std::vector<Expr*> exprsToDelete = {expr1, expr2};
    Expr::DeleteExprs(exprsToDelete);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
}

}