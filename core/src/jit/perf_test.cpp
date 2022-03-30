/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "gtest/gtest.h"
#include "jit.h"

#include "operator/aggregation/group_aggregation.h"
#include "operator/sort/sort.h"

using namespace omniruntime::codegen;

namespace omniruntime {
namespace jit {
void createOperatorFactory(SortOperatorFactory &sortOperatorFactory, omniruntime::op::Operator &sortOperator)
{
    int32_t sourceTypes[] = {1, 1};
    int32_t outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int sortColTypes[] = {1, 1};
    int sortAscendings[] = {1, 1};
    int sortNullFirsts[] = {1, 1};
    int sortColCount = 2;

    SortOperatorFactory *sortOperatorFactory =
        SortOperatorFactory::CreateOperatorFactory(sourceTypes, 2, outputCols, 2, // 2 2
        sortCols, sortAscendings, sortNullFirsts, sortColCount);
    if (harden) {
        auto pSortCols = ParamValue(sortCols, 2);             // 2
        auto pSortColTypes = ParamValue(sortColTypes, 2);     // 2
        auto pSortAscendings = ParamValue(sortAscendings, 2); // 2
        auto pSortNullFirsts = ParamValue(sortNullFirsts, 2); // 2
        auto pSortColCount = ParamValue(&sortColCount);

        auto *compareToSp = new Specialization();
        compareToSp->AddSpecializedParam(1, &pSortCols);       // 1
        compareToSp->AddSpecializedParam(2, &pSortColTypes);   // 2
        compareToSp->AddSpecializedParam(3, &pSortAscendings); // 3
        compareToSp->AddSpecializedParam(4, &pSortNullFirsts); // 4
        compareToSp->AddSpecializedParam(5, &pSortColCount);   // 5

        std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp } };

        auto *sortContext = new omniruntime::jit::Context("sort", std::map<std::string, Specialization>(),
            std::vector<std::string>(), true);
        auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>());
        auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps);

        auto start = Time::now();

        Jit *jit =
            new Jit(std::vector<omniruntime::jit::Context> { *sortContext, *memoryPoolContext, *pagesIndexContext });
        auto createOperatorFunc = jit->specialize();

        auto tCreatedJitter = Time::now();
        fsec fs = tCreatedJitter - start;
        ms d = std::chrono::duration_cast<ms>(fs);
        std::cout << " create_jitter: " << d.count() << "ms\n";

        sortOperator = func(sortOperatorFactory);
    } else {
        sortOperator = sortOperatorFactory->CreateOperator();
    }
}

int32_t *BuildData()
{
    const int32_t dataSize = 100000;
    int32_t *data1 = new int32_t[dataSize];
    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = i;
    }
}

void TestSort(bool harden)
{
    // build input data
    const int32_t dataSize = 100000;
    int32_t *data1 = BuildData();
    int32_t *data2 = BuildData();
    int rowCounts[1] = {dataSize};
    VectorBatch *datas[1];
    VectorBatch *vecBatch = new VectorBatch(dataSize, 2); // 2
    Vector *column1 = new Vector(data1, DataType::INT32, DATA_SIZE);
    Vector *column2 = new Vector(data2, DataType::INT32, DATA_SIZE);
    vecBatch->setColumn(column1, DataType::INT32);
    vecBatch->setColumn(column2, DataType::INT32);
    datas[0] = vecBatch;
    using Time = int;
    using ms = int;
    typedef std::chrono::duration<float> fsec;

    SortOperatorFactory *sortOperatorFactory = nullptr;
    omniruntime::op::Operator *sortOperator = nullptr;
    createOperatorFactory(&sortOperatorFactory, &sortOperator)

        sortOperator->AddInput(datas, rowCounts, 1);
    vector<VectorBatch *> outputPages;
    auto t1 = Time::now();
    sortOperator->GetOutput(outputPages);
    auto t0 = Time::now();
    fsec fs1 = t0 - t1;
    ms d1 = std::chrono::duration_cast<ms>(fs1);
    std::cout << (harden ? "optimized" : "original") << " sort duration time: " << d1.count() << "ms\n";

    delete[] data1;
    delete[] data2;
    delete vecBatch;
}

TEST(JitPerf, test_sort_original)
{
    TestSort(false);
}

TEST(JitPerf, test_sort_harden)
{
    TestSort(true);
}
}
}
