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
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/util/function_type.h"
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
#include "../../util/test_util.h"
// for fuzz mode
// type1 numbers
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;

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
    const int32_t projectCount = 2;

    FieldExpr *col1Expr = new FieldExpr(2, LongType());
    FieldExpr *col2Expr = new FieldExpr(4, DoubleType());
    std::vector<Expr *> projections = { col2Expr, col1Expr };
    FieldExpr *eqLeft = new FieldExpr(4, DoubleType());
    LiteralExpr *eqRight = new LiteralExpr(50.0, DoubleType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());
    auto overflowConfig = new OverflowConfig();

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(eqExpr, sourceTypes, dataSize, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(sourceVecBatch);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    Expr::DeleteExprs({ eqExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete overflowConfig;
    delete vecAllocator;
}

void TestSort(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Sort Fuzz" << std::endl;
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[sourceTypesSize];
    int32_t sortCols[sourceTypesSize];
    int32_t ascendings[sourceTypesSize];
    int32_t nullFirsts[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        outputCols[i] = i;
        sortCols[i] = i;
        ascendings[i] = 1;
        nullFirsts[i] = 0;
    }
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("sort");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
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
    //    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::cout << "Aggregation Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation");
    auto sourceVecBatch = CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    DataTypes groupTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    DataTypes aggInputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    uint32_t aggCols[2] = {2, 4};
    std::vector<uint32_t> aggFuncTypeContext =
        std::vector<uint32_t>((uint32_t *)aggFunType, (uint32_t *)aggFunType + 2);
    uint32_t maskCols[2] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};
    std::vector<uint32_t> maskColsContext = std::vector<uint32_t>((uint32_t *)maskCols, (uint32_t *)maskCols + 2);
    std::vector<uint32_t> aggInputColsContext = std::vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + 2);

    auto aggInputColsContextWrap = AggregatorUtil::WrapWithVector(aggInputColsContext);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeContext.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeContext.size());

    AggregationOperatorFactory *nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypeContext,
        aggInputColsContextWrap, maskColsContext, aggOutputTypesWrap, inputRawsWrap, outputPartialsWrap);
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
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    DataTypes groupTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    DataTypes aggInputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    uint32_t groupCols[2] = {1, 2};
    uint32_t aggCols[2] = {2, 4};
    std::vector<uint32_t> groupByColContext = std::vector<uint32_t>((uint32_t *)groupCols, (uint32_t *)groupCols + 2);
    std::vector<uint32_t> aggColContext = std::vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + 2);
    std::vector<uint32_t> aggFuncTypeContext =
        std::vector<uint32_t>((uint32_t *)aggFunType, (uint32_t *)aggFunType + 2);
    int32_t maskCols[] = {-1, -1};
    std::vector<uint32_t> maskColsContext = std::vector<uint32_t>((uint32_t *)maskCols, (uint32_t *)maskCols + 2);

    auto aggColContextWrap = AggregatorUtil::WrapWithVector(aggColContext);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeContext.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeContext.size());

    HashAggregationOperatorFactory *nativeOperatorFactory =
        new HashAggregationOperatorFactory(groupByColContext, groupTypes, aggColContextWrap, aggInputTypesWrap,
        aggOutputTypesWrap, aggFuncTypeContext, maskColsContext, inputRawsWrap, outputPartialsWrap, false);
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

    DataTypes buildTypes(allSupportedBaseTypes);
    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    std::string filterExpression = "";

    auto hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildTypes, buildJoinCols,
        joinColsCount, filterExpression, 1);
    auto *hashBuilderOperator = hashBuilderFactory->CreateOperator();
    hashBuilderOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VectorBatch *probeVecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    DataTypes probeTypes(allSupportedBaseTypes);
    int32_t probeOutputCols[1] = {2};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    DataTypes buildOutputTypes(std::vector<DataTypePtr>({ LongType() }));
    int32_t buildOutputCols[1] = {2};

    auto hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderFactory);
    auto overflowConfig = new OverflowConfig();

    auto lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(probeTypes, probeOutputCols,
        probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols, buildOutputTypes,
        JoinType::OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr, overflowConfig);
    auto *lookupJoinOperator = lookupJoinFactory->CreateOperator();
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    VectorHelper::FreeVecBatches(output);
    omniruntime::op::Operator::DeleteOperator(hashBuilderOperator);
    omniruntime::op::Operator::DeleteOperator(lookupJoinOperator);
    DeleteOperatorFactory(hashBuilderFactory);
    DeleteOperatorFactory(lookupJoinFactory);
    delete overflowConfig;
    delete vecAllocator;
}

void TestLimit(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "Limit Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Limit");
    VectorBatch *vecBatch =
        CreateInputForAllTypes(sourceTypes, testData, dataSize, loopCount, vecAllocator, true, true);
    const int64_t limitCount = loopCount - 1;

    auto operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount);
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
    const int64_t limitCount = loopCount - 1;
    int32_t distinctCols[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    auto operatorFactory =
        DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes, distinctCols, 10, -1, limitCount);
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

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[10] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    int32_t partitionCount = 10;
    int32_t bucketToPartition[1] = { 1 };
    bool isHashPrecomputed = false;
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { IntType(), VarcharType() });
    int32_t hashChannels[2] = { 1, 8 };
    int32_t hashChannelsCount = 2;

    auto operatorFactory = PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 10,
        replicatesAnyRow, nullChannel, partitionChannels, 10, partitionCount, bucketToPartition, 1, isHashPrecomputed,
        hashChannelTypes, hashChannels, hashChannelsCount);
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

    auto operatorFactory = new ProjectionOperatorFactory(exprs, dataSize, sourceTypes, dataSize, overflowConfig);
    auto projectOperator = operatorFactory->CreateOperator();
    projectOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> ret;
    projectOperator->GetOutput(ret);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatches(ret);
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

    UnionOperatorFactory *operatorFactory =
        UnionOperatorFactory::CreateUnionOperatorFactory(sourceTypes, sourceTypes.GetSize(), false);
    auto unionOperator = operatorFactory->CreateOperator();
    unionOperator->AddInput(vecBatch1);
    unionOperator->AddInput(vecBatch2);
    std::vector<VectorBatch *> outputVecBatches;
    unionOperator->GetOutput(outputVecBatches);

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(unionOperator);
    DeleteOperatorFactory(operatorFactory);
    delete vecAllocator;
}

void TestSortMergeJoin(void **testData, omniruntime::type::DataTypes &sourceTypes, int32_t dataSize, int32_t loopCount)
{
    std::cout << "SortMergeJoin Fuzz" << std::endl;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("SortMergeJoin");
    std::string blank = "";
    SortMergeJoinOperator *smjOp = new SortMergeJoinOperator(JoinType::OMNI_JOIN_TYPE_INNER, blank);

    DataTypes streamedTblTypes(sourceTypes);
    std::vector<int32_t> streamedKeysCols;
    streamedKeysCols.push_back(1);
    std::vector<int32_t> streamedOutputCols;
    streamedOutputCols.push_back(2);
    smjOp->ConfigStreamedTblInfo(streamedTblTypes, streamedKeysCols, streamedOutputCols);

    DataTypes bufferedTblTypes(sourceTypes);
    std::vector<int32_t> bufferedKeysCols;
    bufferedKeysCols.push_back(2);
    std::vector<int32_t> bufferedOutputCols;
    bufferedOutputCols.push_back(1);
    auto overflowConfig = new OverflowConfig();
    smjOp->ConfigBufferedTblInfo(bufferedTblTypes, bufferedKeysCols, bufferedOutputCols);
    smjOp->InitScannerAndResultBuilder(overflowConfig);

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
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, 5, sortCols, ascendings, nullFirsts, 1);
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
    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {2};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_AVG};
    int32_t partitionCols[1] = {1};
    int32_t preGroupedCols[0] = {};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    int32_t argumentChannels[1] = {2};
    DataTypes allTypes(std::vector<DataTypePtr>({ ShortType(), IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(SHORT_DECIMAL_SIZE, 0), Decimal128Type(LONG_DECIMAL_SIZE, 0),
        VarcharType(CHAR_SIZE), CharType(CHAR_SIZE), DoubleType() }));

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        9, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
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
