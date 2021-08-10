#include <gtest/gtest.h>
#include "../../src/operator/aggregation/group_aggregation.h"
#include "../../src/operator/aggregation/non_group_aggregation.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include "../../src/jit/jit.h"
#include "../../src/jit/specialization.h"
#include "../../src/operator/optimization.h"
#include "../../src/vector/vector_helper.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include <thread>
#include <cstdlib>
#include <time.h>
#include <mutex>

const int32_t VEC_BATCH_NUM = 10;
const int32_t ROW_PER_VEC_BATCH = 2000000;
const int32_t CARDINALITY = 4;
const int32_t COLUMN_NUM = 4;
const bool INPUT_MODE = true;
const bool OUTPUT_MODE = false;

long lrand()
{
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * 8) | rand();
    }
    return rand();
}

using namespace omniruntime::vec;

VectorBatch** buildInput(int32_t vecBatchNum, int32_t colNum, int32_t rowPerVecBatch, int32_t cardinality)
{
    VectorBatch** input = new VectorBatch*[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch* vecBatch = new VectorBatch(colNum);
        for (int32_t c = 0; c < colNum; ++ c) {
            LongVector* col = new LongVector(nullptr, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            vecBatch->SetVector(c, col);
        }
        input[i] = vecBatch;
    }
    return input;
}

void destroyInput(VectorBatch** input, int32_t vecBatchNum, int32_t colNum)
{
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        delete input[i];
    }
}

// create a factory and make it optimized
uintptr_t CreateHashFactoryWithJit(bool inputRaw, bool outputPartial)
{
    using namespace omniruntime::jit;
    using namespace omniruntime::op;
    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = OMNI_VEC_TYPE_LONG;
    groupTypes[1] = OMNI_VEC_TYPE_LONG;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = OMNI_VEC_TYPE_LONG;
    aggTypes[1] = OMNI_VEC_TYPE_LONG;
    // uint32_t aggFunType[] = {0, 0};
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = OMNI_AGGREGATION_TYPE_SUM;
    aggFunType[1] = OMNI_AGGREGATION_TYPE_AVG;
    uint32_t retTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t* colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    Specialization *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_col_type);
    inloopSp->AddSpecializedParam(4, &p_col_count);
    inloopSp->AddSpecializedParam(5, &p_groupByColIdx);
    inloopSp->AddSpecializedParam(6, &p_group_num);
    inloopSp->AddSpecializedParam(7, &p_aggColIdx);
    inloopSp->AddSpecializedParam(8, &p_agg_num);
    inloopSp->AddSpecializedParam(9, &p_agg_types);

    Specialization *hashColumnSp = new Specialization();
    hashColumnSp->AddSpecializedParam(2, &p_col_type);
    hashColumnSp->AddSpecializedParam(3, &p_group_num);

    Specialization *aggColumnSp = new Specialization();
    aggColumnSp->AddSpecializedParam(2, &p_col_type);
    aggColumnSp->AddSpecializedParam(3, &p_agg_num);

    std::map<std::string, Specialization> hashGroupbySps = {
        {OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp},
    };

    omniruntime::jit::Context *groupAggregationContext = new omniruntime::jit::Context("group_aggregation", hashGroupbySps, std::vector<std::string>(), std::vector<std::string>(), true);
    omniruntime::jit::Context *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    omniruntime::jit::Context *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->Specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    std::cout << "after jit" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

uintptr_t CreateAggFactoryWithJit()
{
    using namespace omniruntime::jit;
    uint32_t* aggTypes = new uint32_t[4];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    aggTypes[2] = 2;
    aggTypes[3] = 2;
    uint32_t* aggFunType = new uint32_t[4];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    aggFunType[2] = 0;
    aggFunType[3] = 0;
    omniruntime::op::PrepareContext aggTypeContext = {aggTypes, 4};
    omniruntime::op::PrepareContext aggFuncTypeContext = {aggFunType, 4};

    int32_t aggColNum = aggTypeContext.len;
    int32_t colNum = aggTypeContext.len;

    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_agg_num);
    inloopSp->AddSpecializedParam(4, &p_agg_data_type);
    inloopSp->AddSpecializedParam(5, &p_agg_types);

    std::map<std::string, Specialization> nonGroupSps = {
            {OMNIJIT_NON_GROUP_INLOOP, *inloopSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("non_group_aggregation", nonGroupSps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->Specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    std::cout << "after jit" << std::endl;
    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

uintptr_t CreateHashFactoryWithoutJit(bool inputRaw, bool outputPartial)
{
    using namespace omniruntime::jit;
    using namespace omniruntime::op;
    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = OMNI_VEC_TYPE_LONG;
    groupTypes[1] = OMNI_VEC_TYPE_LONG;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = OMNI_VEC_TYPE_LONG;
    aggTypes[1] = OMNI_VEC_TYPE_LONG;
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = OMNI_AGGREGATION_TYPE_SUM;
    aggFunType[1] = OMNI_AGGREGATION_TYPE_AVG;
    uint32_t retTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t* colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

double total_cpu_time;
double total_wall_time;

void perfTestOriginal(int64_t moduleAddr, VectorBatch** input)
{
    using namespace omniruntime::op;

    // create operator
    HashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<HashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = nativeOperatorFactory->CreateOperator();

    // execution
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }
    std::vector<VectorBatch*> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
}

void perfTest(int64_t moduleAddr, VectorBatch** input, int32_t vecBatchNum, int32_t* rowCount)
{
    using namespace omniruntime::op;

    // create operatory
    HashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<HashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = reinterpret_cast<HashAggModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto errNo = groupBy->AddInput(input[pageIndex]);
    }
    std::vector<VectorBatch*> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
}

TEST(HashAggregationOperatorTest, VerifyCorrectness)
{
    using namespace omniruntime::op;
    // create 10 pages
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 4;
    const int COLUMN_COUNT = 7; // groupby*2 + sum + avg + count + min + max
    std::string aggNames[] = {"group", "group", "sum", "avg", "count", "min", "max"};

    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);

    // First stage
    ColumnIndex c0 = {0, LongVecType::Instance()};
    ColumnIndex c1 = {1, LongVecType::Instance()};
    ColumnIndex c2 = {2, LongVecType::Instance()};
    ColumnIndex c3 = {3, LongVecType::Instance()};
    ColumnIndex c4 = {4, LongVecType::Instance()};
    ColumnIndex c5 = {5, LongVecType::Instance()};
    ColumnIndex c6 = {6, LongVecType::Instance()};
    std::vector<ColumnIndex> groupByColumns1 = {c0, c1};
    std::vector<ColumnIndex> aggregateColumns1 = {c2, c3, c4, c5, c6};
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<CountAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator* groupBy1 = new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy1->AddInput(input[i]);
    }

    std::vector<VectorBatch*> result1;
    int32_t vecBatchCount = groupBy1->GetOutput(result1);
    delete groupBy1;

    ColumnIndex c7 = {0, LongVecType::Instance()};
    ColumnIndex c8 = {1, LongVecType::Instance()};
    ColumnIndex c9 = {2, LongVecType::Instance()};
    ColumnIndex c10 = {3, LongVecType::Instance()};
    ColumnIndex c11 = {4, LongVecType::Instance()};
    ColumnIndex c12 = {5, LongVecType::Instance()};
    ColumnIndex c13 = {6, LongVecType::Instance()};
    groupByColumns1 = {c7, c8};
    aggregateColumns1 = {c9, c10, c11, c12, c13};
    aggs1.clear();
    aggs1.push_back(std::make_unique<SumAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<CountAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator* groupBy2 = new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy2->AddInput(input[i]);
    }

    int32_t tableCount2 = groupBy2->GetOutput(result1);
    delete groupBy2;

    for (auto& aggType : aggNames) {
        std::cout << aggType << "   ";
    }
    std::cout << std::endl;
    for (auto vecBatch : result1) {
        PrintVecBatch(vecBatch);
    }
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);

    // Second stage
    ColumnIndex c14 = {0, LongVecType::Instance()};
    ColumnIndex c15 = {1, LongVecType::Instance()};
    ColumnIndex c16 = {2, LongVecType::Instance()};
    ColumnIndex c17 = {3, LongVecType::Instance()};
    ColumnIndex c18 = {4, LongVecType::Instance()};
    ColumnIndex c19 = {5, LongVecType::Instance()};
    ColumnIndex c20 = {6, LongVecType::Instance()};
    std::vector<ColumnIndex> groupByColumns2 = {c14, c15};
    std::vector<ColumnIndex> aggregateColumns2 = {c16, c17, c18, c19, c20};
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(std::make_unique<SumAggregator>(2, false, false));
    aggs2.push_back(std::make_unique<AverageAggregator>(2, false, false));
    aggs2.push_back(std::make_unique<CountAggregator>(2, false, false));
    aggs2.push_back(std::make_unique<MinAggregator>(2, false, false));
    aggs2.push_back(std::make_unique<MaxAggregator>(2, false, false));
    HashAggregationOperator* groupBy3 = new HashAggregationOperator(groupByColumns2, aggregateColumns2, std::move(aggs2), true, false);

    for (int32_t i = 0; i < result1.size(); ++i) {
        groupBy3->AddInput(result1[i]);
    }

    VectorHelper::FreeVecBatches(result1);

    std::vector<VectorBatch*> result2;
    groupBy3->GetOutput(result2);
    delete groupBy3;

    EXPECT_EQ(result2[0]->GetVectorCount(), 7);
    EXPECT_EQ(result2[0]->GetRowCount(), 4);

    std::cout << std::endl;
    for (auto vecBatch : result2) {
        PrintVecBatch(vecBatch);
    }
    VectorHelper::FreeVecBatches(result2);
}

TEST(HashAggregationOperatorTest, VerfifyCorrectness_GroupByAggSameCols)
{
    using namespace omniruntime::op;
    //FIXME INT32+INT64
    // create 10 vecBatches
    const int VEC_BATCH_NUM = 10;
    VectorBatch** input = new VectorBatch*[VEC_BATCH_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        VectorBatch* vecBatch = new VectorBatch(2);
        LongVector* col1 = new LongVector(nullptr, DATA_SIZE);
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            col1->SetValue(i, i % 3);
        }

        LongVector* col2 = new LongVector(nullptr, DATA_SIZE);
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            col2->SetValue(i, i % 3);
        }
        vecBatch->SetVector(0, col1);
        vecBatch->SetVector(1, col2);
        input[i] = vecBatch;
    }
    ColumnIndex c0 = {0, LongVecType::Instance()};
    ColumnIndex c1 = {1, LongVecType::Instance()};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator>(1, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator* groupBy = new HashAggregationOperator(v1, v2, std::move(aggs), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch*> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);

    EXPECT_EQ(result[0]->GetVectorCount(), 4);

    for (int32_t i = 0; i < result[0]->GetVectorCount(); ++i) {
        Vector* col = result[0]->GetVector(i);
        // TODO: print data;
        // col->printColumn();
    }
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, Original_Multiple_Threads)
{
    using namespace omniruntime::op;
    using namespace std;
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;


    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);

    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = 2;
    groupTypes[1] = 2;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};
    HashAggregationOperatorFactory* nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory);

    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestOriginal, factoryObjAddr, input);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }

    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, PerfViaAPI_Multiple_Threads)
{
    using namespace std;
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);
    int32_t* rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    uint64_t factoryObjAddr = CreateHashFactoryWithJit(true, false);
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTest, factoryObjAddr, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, VerifyCorrectness)
{
    using namespace omniruntime::op;
    // create 10 vecBatches
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 4;
    const int COLUMN_COUNT = 5; // groupby*2 + sum + avg + count + min + max
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);

    ColumnIndex c0 = {0, LongVecType::Instance()};
    ColumnIndex c1 = {1, LongVecType::Instance()};
    ColumnIndex c2 = {2, LongVecType::Instance()};
    ColumnIndex c3 = {3, LongVecType::Instance()};
    ColumnIndex c4 = {4, LongVecType::Instance()};
    std::vector<ColumnIndex> aggregateColumns = {c0, c1, c2, c3, c4};
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<AverageAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<CountAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<MinAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<MaxAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    AggregationOperator* aggregate1 = new AggregationOperator(aggregateColumns, std::move(aggs), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate1->AddInput(input[i]);
    }

    std::vector<VectorBatch*> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    delete aggregate1;

    ColumnIndex c5 = {0, LongVecType::Instance()};
    ColumnIndex c6 = {1, LongVecType::Instance()};
    ColumnIndex c7 = {2, LongVecType::Instance()};
    ColumnIndex c8 = {3, LongVecType::Instance()};
    ColumnIndex c9 = {4, LongVecType::Instance()};
    aggregateColumns = {c5, c6, c7, c8, c9};
    aggs.clear();
    aggs.push_back(std::make_unique<SumAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<AverageAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<CountAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<MinAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<MaxAggregator>(2, INPUT_MODE, OUTPUT_MODE));
    AggregationOperator* aggregate2 = new AggregationOperator(aggregateColumns, std::move(aggs), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate2->AddInput(input[i]);
    }
    int32_t tableCount2 = aggregate2->GetOutput(result);
    delete aggregate2;

    // Second stage
    ColumnIndex c10 = {0, LongVecType::Instance()};
    ColumnIndex c11 = {1, LongVecType::Instance()};
    ColumnIndex c12 = {2, LongVecType::Instance()};
    ColumnIndex c13 = {3, LongVecType::Instance()};
    ColumnIndex c14 = {4, LongVecType::Instance()};
    aggregateColumns = {c10, c11, c12, c13, c14};
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator>(2, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator>(2, false, false));
    aggs1.push_back(std::make_unique<CountAggregator>(2, false, false));
    aggs1.push_back(std::make_unique<MinAggregator>(2, false, false));
    aggs1.push_back(std::make_unique<MaxAggregator>(2, false, false));
    AggregationOperator* aggregate3 = new AggregationOperator(aggregateColumns, std::move(aggs1), true, false);

    for (int32_t i = 0; i < result.size(); ++i) {
        aggregate3->AddInput(result[i]);
    }

    VectorHelper::FreeVecBatches(result);

    std::vector<VectorBatch*> result1;
    int32_t tableCount3 = aggregate3->GetOutput(result1);
    delete aggregate3;
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    for (auto& aggType : aggNames) {
        std::cout << aggType << "\t";
    }
    std::cout << std::endl;
    for (auto vecBatch : result1) {
        PrintVecBatch(vecBatch);
    }
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
    VectorHelper::FreeVecBatches(result1);
}


TEST(AggregatorTest, avg_correctness_test)
{
    using namespace omniruntime::op;
    // create 10 pages
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 100;
    const int COLUMN_COUNT = 1;
    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);

    ColumnIndex c0 = {0, LongVecType::Instance()};
    std::vector<ColumnIndex> aggregateColumns = {c0};
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<AverageAggregator>(2));
    AggregationOperator* aggregate = new AggregationOperator(aggregateColumns, std::move(aggs), true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate->GetOutput(result);

    EXPECT_EQ(result[0]->GetVectorCount(), 1);
    EXPECT_EQ(result[0]->GetRowCount(), 1);

    std::string aggNames[] = {"avg"};

    for (int32_t i = 0; i < result[0]->GetVectorCount(); ++i) {
        Vector* col = result[0]->GetVector(i);
        std::cout << aggNames[i] << " ";
        // col->printColumn();
    }

    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
    VectorHelper::FreeVecBatches(result);
}

void perfTestNonGroup(int64_t moduleAddr, bool codegenMode, VectorBatch** input, int32_t vecBatchNum, int32_t* rowCount)
{
    using namespace omniruntime::op;

    // create operatory
    AggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<AggregationOperatorFactory*>(moduleAddr);
    Operator* aggregation;
    if (codegenMode) {
        aggregation = reinterpret_cast<opt_module>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);
    }else {
        aggregation = nativeOperatorFactory->CreateOperator();
    }

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto errNo = aggregation->AddInput(input[pageIndex]);
    }
    std::vector<VectorBatch*> result;
    int32_t vecBatchCount = aggregation->GetOutput(result);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 1);
}

TEST(AggregationOperatorTest, Perf_Original)
{
    using namespace std;

    uint32_t* aggTypes = new uint32_t[4];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    aggTypes[2] = 2;
    aggTypes[3] = 2;
    uint32_t* aggFunType = new uint32_t[4];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    aggFunType[2] = 0;
    aggFunType[3] = 0;
    omniruntime::op::PrepareContext aggTypeContext = {aggTypes, 4};
    omniruntime::op::PrepareContext aggFuncTypeContext = {aggFunType, 4};

    int32_t aggColNum = aggTypeContext.len;

    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    int64_t factoryAddr = reinterpret_cast<int64_t>(nativeOperatorFactory);
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);
    int32_t* rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryAddr, false, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, Perf_Codegen)
{
    using namespace std;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);
    int32_t* rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    uint64_t factoryObjAddr = CreateAggFactoryWithJit();
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryObjAddr, true, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, compare_perf)
{
    using namespace omniruntime::jit;
    using namespace omniruntime::op;
    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = 2;
    groupTypes[1] = 2;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t* colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }
    // ------------------------------------------JIT Optimization --------------------------------------------
    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_col_type);
    inloopSp->AddSpecializedParam(4, &p_col_count);
    inloopSp->AddSpecializedParam(5, &p_groupByColIdx);
    inloopSp->AddSpecializedParam(6, &p_group_num);
    inloopSp->AddSpecializedParam(7, &p_aggColIdx);
    inloopSp->AddSpecializedParam(8, &p_agg_num);
    inloopSp->AddSpecializedParam(9, &p_agg_types);

    std::map<std::string, Specialization> hashGroupbySps = {
        {OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp},
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("group_aggregation", hashGroupbySps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->Specialize();

     // ------------------------------------------Create operator--------------------------------------------
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    std::cout << "after JIT" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext,true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
     // create operator
    auto jitGroupBy = reinterpret_cast<HashAggModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);

    // ------------------------------------------Process Input--------------------------------------------
    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);
    int32_t* rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }

    Timer timer;
    timer.setStart();
    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = jitGroupBy->AddInput(input[pageIndex]);
    }
    timer.calculateElapse();
    double wall_elapsed = timer.getWallElapse();
    double cpu_elapsed = timer.getCpuElapse();
    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;

    HashAggregationOperatorFactory* nativeOperatorFactory2 = new HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext,true, false);
    nativeOperatorFactory2->Init();
    auto groupBy = nativeOperatorFactory2->CreateOperator();

    timer.reset();
    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        groupBy->AddInput(input[pageIndex]);
    }
    timer.calculateElapse();
    wall_elapsed = timer.getWallElapse();
    cpu_elapsed = timer.getCpuElapse();

    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;

    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, MultiStage)
{
    VectorBatch** input = buildInput(VEC_BATCH_NUM, COLUMN_NUM, ROW_PER_VEC_BATCH, CARDINALITY);
    int32_t* rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    uintptr_t partialFactoryAddr1 = CreateHashFactoryWithoutJit(true, true);
    auto partialFactory1 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory*>(partialFactoryAddr1);
    auto partialOperator1 = partialFactory1->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator1->AddInput(input[i]);
    }
    std::vector<VectorBatch*> resultFromPartial;
    partialOperator1->GetOutput(resultFromPartial);
    for (auto vecBatch : resultFromPartial) {
        PrintVecBatch(vecBatch);
    }

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(true, true);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory*>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input[i]);
    }
    partialOperator2->GetOutput(resultFromPartial);
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
    for (auto vecBatch : resultFromPartial) {
        PrintVecBatch(vecBatch);
    }

    uintptr_t finalFactoryAddr = CreateHashFactoryWithoutJit(false, false);
    auto finalFactory = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory*>(finalFactoryAddr);
    auto operator2 = finalFactory->CreateOperator();
    for (int32_t i = 0; i < resultFromPartial.size(); ++i) {
        operator2->AddInput(resultFromPartial[i]);
    }
    std::vector<VectorBatch*> resultFromFinal;
    operator2->GetOutput(resultFromFinal);

    for (auto vecBatch : resultFromFinal) {
        PrintVecBatch(vecBatch);
    }
    VectorHelper::FreeVecBatches(resultFromFinal);
}