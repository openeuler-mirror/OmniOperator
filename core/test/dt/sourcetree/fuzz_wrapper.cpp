/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Fuzz test file
 */

#include "fuzz_wrapper.h"
#include <iostream>
#include <vector>
#include "type/decimal128.h"
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/limit/limit.h"
#include "operator/limit/distinct_limit.h"
#include "operator/partitionedoutput/partitionedoutput.h"
#include "operator/sort/sort.h"
#include "operator/union/union.h"
#include "operator/topn/topn.h"
#include "operator/filter/filter_and_project.h"
#include "operator/window/window.h"
#include "operator/join/lookup_join.h"
#include "operator/join/sortmergejoin/sort_merge_join.h"
#include "expression/expressions.h"
#include "test/util/dt_fuzz_factory_create_util.h"
#include "test/util/test_util.h"

// for fuzz mode
// type1 numbers
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace DtFuzzFactoryCreateUtil;

const int32_t SHORT_DECIMAL_SIZE = 18;
const int32_t LONG_DECIMAL_SIZE = 38;
int32_t CHAR_SIZE = 10;
std::vector<DataTypePtr> allSupportedBaseTypes = { ShortType(),
                                                   IntType(),
                                                   LongType(),
                                                   BooleanType(),
                                                   DoubleType(),
                                                   Date32Type(DAY),
                                                   Decimal64Type(SHORT_DECIMAL_SIZE, 0),
                                                   Decimal128Type(LONG_DECIMAL_SIZE, 0),
                                                   VarcharType(CHAR_SIZE),
                                                   CharType(CHAR_SIZE) };

VectorBatch *CreateInputForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize, int32_t loopCount,
    VectorAllocator *vectorAllocator, bool isDictionary, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    std::vector<DataTypePtr> sourceTypesVec = sourceTypes.Get();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;
    const int32_t modValue = 4;

    Vector *sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        int32_t capacityInBytes = (sourceTypeIds[i] == OMNI_CHAR || sourceTypeIds[i] == OMNI_VARCHAR) ?
            static_cast<int32_t>(static_cast<VarcharDataType *>(sourceTypesVec[i].get())->GetWidth()) * totalDataSize :
            0;
        sourceVectors[i] = VectorHelper::CreateVector(vectorAllocator, OMNI_VEC_ENCODING_FLAT, sourceTypeIds[i],
            capacityInBytes, totalDataSize);
        VectorHelper::SetValue(sourceVectors[i], 0, sortDatas[i]);
    }
    for (int32_t i = 1; i < totalDataSize; i++) {
        for (int32_t j = 0; j < sourceTypesSize; j++) {
            if ((i % modValue == 0) && hasNull && (sourceTypeIds[j] == OMNI_VARCHAR || sourceTypeIds[j] == OMNI_CHAR)) {
                static_cast<VarcharVector *>(sourceVectors[j])->SetValueNull(i);
            } else if ((i % modValue == 0) && hasNull) {
                sourceVectors[j]->SetValueNull(i);
            } else {
                VectorHelper::SetValue(sourceVectors[j], i, sortDatas[j]);
            }
        }
    }

    if (isDictionary) {
        int32_t ids[totalDataSize];
        for (int32_t i = 0; i < totalDataSize; i++) {
            ids[i] = i;
        }
        for (int32_t i = 0; i < sourceTypesSize; i++) {
            auto sortVector = sourceVectors[i];
            sourceVectors[i] = new DictionaryVector(sortVector, ids, totalDataSize);
            delete sortVector;
        }
    }

    auto sortVecBatch = new VectorBatch(sourceTypesSize, totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sortVecBatch->SetVector(i, sourceVectors[i]);
    }
    return sortVecBatch;
}

void DeleteOperatorFactory(omniruntime::op::OperatorFactory *operatorFactory)
{
    delete operatorFactory;
}

VectorBatch *CreateEmptyVectorBatch(std::vector<DataTypePtr> &dataTypes)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    VectorBatch *vectorBatch = new VectorBatch(dataTypes.size());
    vectorBatch->NewVectors(allocator, dataTypes);
    return vectorBatch;
}

void TestFilter(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Filter Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    FieldExpr *col1Expr = new FieldExpr(2, LongType());
    FieldExpr *col2Expr = new FieldExpr(4, DoubleType());
    std::vector<Expr *> projections = { col2Expr, col1Expr };
    FieldExpr *eqLeft = new FieldExpr(4, DoubleType());
    LiteralExpr *eqRight = new LiteralExpr(50.0, DoubleType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());
    auto overflowConfig = new OverflowConfig();

    auto factory = dynamic_cast<FilterAndProjectOperatorFactory *>(
        CreateFilterFactory(sourceTypes, eqExpr, projections, overflowConfig));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete overflowConfig;
    delete vecAllocator;
}

void TestSort(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Sort Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("sort");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    auto operatorFactory = dynamic_cast<SortOperatorFactory *>(CreateSortFactory(sourceTypes));
    auto sortOperator = dynamic_cast<SortOperator *>(operatorFactory->CreateOperator());
    sortOperator->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestAggregation(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Aggregation Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    AggregationOperatorFactory *nativeOperatorFactory =
        dynamic_cast<AggregationOperatorFactory *>(CreateAggregationFactory(sourceTypes));
    nativeOperatorFactory->Init();
    auto groupBy = nativeOperatorFactory->CreateOperator();
    groupBy->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> result;
    groupBy->GetOutput(result);

    VectorHelper::FreeVecBatches(result);
    omniruntime::op::Operator::DeleteOperator(groupBy);
    DeleteOperatorFactory(nativeOperatorFactory);
    delete vecAllocator;
}

void TestHashAggregation(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize,
    int32_t loopCount)
{
    std::cout << "HashAggregation Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("HashAggregation");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    HashAggregationOperatorFactory *nativeOperatorFactory =
        dynamic_cast<HashAggregationOperatorFactory *>(CreateHashAggregationFactory(sourceTypes));
    nativeOperatorFactory->Init();
    auto groupBy = nativeOperatorFactory->CreateOperator();
    groupBy->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> result;
    groupBy->GetOutput(result);

    VectorHelper::FreeVecBatches(result);
    omniruntime::op::Operator::DeleteOperator(groupBy);
    DeleteOperatorFactory(nativeOperatorFactory);
    delete vecAllocator;
}

void TestHashJoin(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "HashJoin Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("HashJoin");
    auto vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, (loopCount / 10) + 1, vecAllocator, true, true);

    auto overflowConfig = new OverflowConfig();
    auto operatorFactories = CreateHashJoinFactory(sourceTypes, overflowConfig);
    auto hashBuilderFactory = dynamic_cast<HashBuilderOperatorFactory *>(operatorFactories[0]);
    auto hashBuilderOperator = hashBuilderFactory->CreateOperator();
    hashBuilderOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VectorBatch *probeVecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    auto lookupJoinFactory = dynamic_cast<LookupJoinOperatorFactory *>(operatorFactories[1]);
    auto lookupJoinOperator = lookupJoinFactory->CreateOperator();
    lookupJoinOperator->AddInput(probeVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    lookupJoinOperator->GetOutput(&outputVecBatch);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(hashBuilderOperator);
    omniruntime::op::Operator::DeleteOperator(lookupJoinOperator);
    for (auto operatorFactory : operatorFactories) {
        DeleteOperatorFactory(operatorFactory);
    }
    delete overflowConfig;
    delete vecAllocator;
}

void TestLimit(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Limit Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Limit");
    VectorBatch *vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    auto operatorFactory = dynamic_cast<LimitOperatorFactory *>(CreateLimitFactory(sourceTypes, loopCount));
    auto limitOperator = operatorFactory->CreateOperator();
    limitOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->GetOutput(outputVecBatches);

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestDistinctLimit(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "DistinctLimit Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DistinctLimit");
    VectorBatch *vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    auto operatorFactory =
        dynamic_cast<DistinctLimitOperatorFactory *>(CreateDistinctLimitFactory(sourceTypes, loopCount));
    auto distinctLimitOperator = operatorFactory->CreateOperator();
    distinctLimitOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestPartitionedOutput(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize,
    int32_t loopCount)
{
    std::cout << "PartitionedOutput Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("PartitionedOutput");
    VectorBatch *vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    auto operatorFactory =
        dynamic_cast<PartitionedOutputOperatorFactory *>(CreatePartitionedOutputFactory(sourceTypes));
    auto partitionedOperator = operatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    VectorHelper::FreeVecBatches(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestProject(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Project Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Project");
    VectorBatch *vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    FieldExpr *addLeft = new FieldExpr(1, IntType());
    LiteralExpr *addRight = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    std::vector<Expr *> exprs = { addExpr };
    auto overflowConfig = new OverflowConfig();

    auto operatorFactory =
        dynamic_cast<ProjectionOperatorFactory *>(CreateProjectFactory(sourceTypes, exprs, overflowConfig));
    auto projectOperator = operatorFactory->CreateOperator();
    projectOperator->AddInput(vecBatch);
    VectorBatch *resultVecBatch = nullptr;
    projectOperator->GetOutput(&resultVecBatch);

    VectorHelper::FreeVecBatch(resultVecBatch);
    omniruntime::op::Operator::DeleteOperator(projectOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
    delete overflowConfig;
}

void TestUnion(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Union Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Union");
    VectorBatch *vecBatch1 =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    VectorBatch *vecBatch2 =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    UnionOperatorFactory *operatorFactory = dynamic_cast<UnionOperatorFactory *>(CreateUnionFactory(sourceTypes));
    auto unionOperator = operatorFactory->CreateOperator();
    unionOperator->AddInput(vecBatch1);
    unionOperator->AddInput(vecBatch2);
    std::vector<VectorBatch *> outputVecBatches;
    while (unionOperator->GetStatus() == OMNI_STATUS_NORMAL) {
        unionOperator->GetOutput(outputVecBatches);
    }

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(unionOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestSortMergeJoin(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "SortMergeJoin Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("SortMergeJoin");
    auto overflowConfig = new OverflowConfig();
    auto smjOp = dynamic_cast<SortMergeJoinOperator *>(CreateSortMergeJoinOperator(sourceTypes, overflowConfig));

    // construct data
    VectorBatch *streamedTblVecBatch1 =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    VectorBatch *bufferedTblVecBatch1 =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    // need add buffered table data
    smjOp->AddStreamedTableInput(streamedTblVecBatch1);

    // need add buffered table data
    smjOp->AddBufferedTableInput(bufferedTblVecBatch1);

    std::vector<DataTypePtr> bufferTypesVector = { ShortType(),
        IntType(),
        LongType(),
        BooleanType(),
        DoubleType(),
        Date32Type(DAY),
        Decimal64Type(SHORT_DECIMAL_SIZE, 0),
        Decimal128Type(LONG_DECIMAL_SIZE, LONG_DECIMAL_SIZE),
        VarcharType(CHAR_SIZE),
        CharType(CHAR_SIZE) };
    // add eof flag to buffered table , need add streamed table data
    VectorBatch *bufferedTblVecBatchEof = CreateEmptyVectorBatch(bufferTypesVector);
    smjOp->AddBufferedTableInput(bufferedTblVecBatchEof);

    // add eof flag to streamed table
    std::vector<DataTypePtr> streamTypesVector = { ShortType(),
        IntType(),
        LongType(),
        BooleanType(),
        DoubleType(),
        Date32Type(DAY),
        Decimal64Type(SHORT_DECIMAL_SIZE, 0),
        Decimal128Type(LONG_DECIMAL_SIZE, LONG_DECIMAL_SIZE),
        VarcharType(CHAR_SIZE),
        CharType(CHAR_SIZE) };
    VectorBatch *streamedTblVecBatchEof = CreateEmptyVectorBatch(streamTypesVector);
    smjOp->AddStreamedTableInput(streamedTblVecBatchEof);

    std::vector<omniruntime::vec::VectorBatch *> result;
    smjOp->GetOutput(result);

    auto streamedTblVecBatchEof1 = CreateEmptyVectorBatch(streamTypesVector);
    smjOp->AddStreamedTableInput(streamedTblVecBatchEof1);

    // check the join result
    for (uint32_t i = 0; i < result.size(); i++) {
        VectorHelper::FreeVecBatch(result[i]);
    }

    omniruntime::op::Operator::DeleteOperator(smjOp);
    delete overflowConfig;
    delete vecAllocator;
}

void TestTopN(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "TopN Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("TopN");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    TopNOperatorFactory *topNOperatorFactory = dynamic_cast<TopNOperatorFactory *>(CreateTopNFactory(sourceTypes));
    auto topNOperator = topNOperatorFactory->CreateOperator();
    topNOperator->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);

    VectorHelper::FreeVecBatches(outputVecorBatchs);
    omniruntime::op::Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    delete vecAllocator;
}

void TestWindow(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Window Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Window");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    DataTypes allTypes(std::vector<DataTypePtr>({ ShortType(), IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(SHORT_DECIMAL_SIZE, 0), Decimal128Type(LONG_DECIMAL_SIZE, 0),
        VarcharType(CHAR_SIZE), CharType(CHAR_SIZE), DoubleType() }));

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = dynamic_cast<WindowOperatorFactory *>(CreateWindowFactory(sourceTypes));
    auto windowOperator = operatorFactory->CreateOperator();
    windowOperator->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    VectorHelper::FreeVecBatches(outputVecBatches);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

int GlobalFuzz(int16_t shortValue, int32_t intValue, int64_t longValue, bool boolValue, double doubleValue,
    int64_t highBits, uint64_t lowBits, std::string stringValue, int32_t loopCount, int32_t charSize)
{
    Decimal128 decimal128(highBits, lowBits);
    const int32_t dataSize = 10;
    CHAR_SIZE = charSize;

    // all types: short, int, long, boolean, double, date32, decimal, decimal128, varchar, char
    void *inputDatas[dataSize] = {&shortValue, &intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue,
                                  &decimal128, &stringValue, &stringValue};
    DataTypes sourceTypes(allSupportedBaseTypes);

    TestAggregation(inputDatas, sourceTypes, dataSize, loopCount);
    TestFilter(inputDatas, sourceTypes, dataSize, loopCount);
    TestHashAggregation(inputDatas, sourceTypes, dataSize, loopCount);
    TestHashJoin(inputDatas, sourceTypes, dataSize, loopCount);
    TestLimit(inputDatas, sourceTypes, dataSize, loopCount);
    TestDistinctLimit(inputDatas, sourceTypes, dataSize, loopCount);
    TestPartitionedOutput(inputDatas, sourceTypes, dataSize, loopCount);
    TestProject(inputDatas, sourceTypes, dataSize, loopCount);
    TestSort(inputDatas, sourceTypes, dataSize, loopCount);
    TestSortMergeJoin(inputDatas, sourceTypes, dataSize, loopCount);
    TestTopN(inputDatas, sourceTypes, dataSize, loopCount);
    TestUnion(inputDatas, sourceTypes, dataSize, loopCount);
    TestWindow(inputDatas, sourceTypes, dataSize, loopCount);

    return 0;
}
