#include "jit.h"
#include "gtest/gtest.h"

#include "../operator/aggregation/group_aggregation.h"
#include "../operator/sort/sort.h"

using namespace omniruntime::codegen;

// VectorBatch **buildData(int VEC_BATCH_NUM, int DATA_SIZE, int *data_type, int column_count) {
//     VectorBatch **input = new VectorBatch *[VEC_BATCH_NUM];
//     for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
//         VectorBatch *vecBatch = new VectorBatch(DATA_SIZE, 2);

//         for (int j = 0; j < column_count; j++) {
//             if (data_type[j] == 1) //INT32
//             {
//                 int32_t *data1 = new int32_t[DATA_SIZE];
//                 for (int32_t i = 0; i < DATA_SIZE; ++i) {
//                     data1[i] = i % 3;
//                 }

//                 Vector *col1 = new Vector(data1, VecType::INT32, DATA_SIZE);
//                 vecBatch->setColumn(col1, VecType::INT32);
//             }
//             if (data_type[j] == 2) //INT64
//             {
//                 int64_t *data1 = new int64_t[DATA_SIZE];
//                 for (int64_t i = 0; i < DATA_SIZE; ++i) {
//                     data1[i] = i % 3;
//                 }

//                 Vector *col1 = new Vector(data1, VecType::INT64, DATA_SIZE);
//                 vecBatch->setColumn(col1, VecType::INT64);
//             }
//         }
//         input[i] = vecBatch;
//     }

//     int32_t *data2 = new int32_t[DATA_SIZE];
//     for (int32_t i = 0; i < DATA_SIZE; ++i) {
//         data2[i] = i;
//     }

//     int rowCounts[1] = {DATA_SIZE};
//     Table *datas[1];
//     Table *table = new Table(DATA_SIZE, 2);
//     Column *column1 = new Column(data1, ColumnType::INT32, DATA_SIZE);
//     Column *column2 = new Column(data2, ColumnType::INT32, DATA_SIZE);
//     table->setColumn(column1, ColumnType::INT32);
//     table->setColumn(column2, ColumnType::INT32);
//     datas[0] = table;

//     typedef std::chrono::high_resolution_clock Time;
//     typedef std::chrono::milliseconds ms;
//     typedef std::chrono::duration<float> fsec;

//     std::map < std::string, ParamValue * > testParam;
//     int32_t sourceTypes[] = {1, 1};
//     int32_t outputCols[] = {0, 1};
//     int sortCols[] = {0, 1};
//     int sortColTypes[] = {1, 1};
//     int sortAscendings[] = {1, 1};
//     int sortNullFirsts[] = {1, 1};
//     int sortColCount = 2;

//     SortOperatorFactory *sortOperatorFactory = SortOperatorFactory::createOperatorFactory(
//         sourceTypes, 2, outputCols, 2, sortCols, sortAscendings, sortNullFirsts, sortColCount);
//     omniruntime::op::Operator *sortOperator = nullptr;
//     if (harden) {
//         auto p_sortCols = ParamValue(sortCols, 2);
//         auto p_sortColTypes = ParamValue(sortColTypes, 2);
//         auto p_sortAscendings = ParamValue(sortAscendings, 2);
//         auto p_sortNullFirsts = ParamValue(sortNullFirsts, 2);
//         auto p_sortColCount = ParamValue(&sortColCount);

//         testParam["_Z9compareTolPiS_S_S_iii@1"] = &p_sortCols;
//         testParam["_Z9compareTolPiS_S_S_iii@2"] = &p_sortColTypes;
//         testParam["_Z9compareTolPiS_S_S_iii@3"] = &p_sortAscendings;
//         testParam["_Z9compareTolPiS_S_S_iii@4"] = &p_sortNullFirsts;
//         testParam["_Z9compareTolPiS_S_S_iii@5"] = &p_sortColCount;

//         std::list < Hammer * > deps = std::list<Hammer *>();

//         auto start = Time::now();
//         llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
//         llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

//         Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
//         Hammer hammer2("/opt/lib/ir/pages_index.ll", testParam);
//         Hammer hammer3("/opt/lib/ir/memory_pool.ll", testParam);
//         hammer1.harden();
//         hammer2.harden();
//         hammer3.harden();
//         deps.push_back(&hammer2);
//         deps.push_back(&hammer3);

//         auto opt_conf = HammerConfig::getConf(2, 119);
//         auto JITTER = hammer1.create_jitter(deps,*opt_conf);
//         auto t_created_jitter = Time::now();
//         fsec fs = t_created_jitter - start;
//         ms d = std::chrono::duration_cast<ms>(fs);
//         std::cout << " create_jitter: " << d.count() << "ms\n";

//         auto sort = JITTER->lookup("_ZN29NativeSortOperatorFactory18createOperatorEv");
//         auto t_lookup_func = Time::now();
//         fs = t_lookup_func - t_created_jitter;
//         d = std::chrono::duration_cast<ms>(fs);
//         std::cout << " lookup func 1: " << d.count() << "ms\n";

//         auto func = (opt_module)(JITTER->lookup("_ZN29NativeSortOperatorFactory18createOperatorEv")->getAddress());
//         auto t_lookup_func2 = Time::now();
//         fs = t_lookup_func2 - t_lookup_func;
//         d = std::chrono::duration_cast<ms>(fs);
//         std::cout << " lookup func 2: " << d.count() << "ms\n";

//         sortOperator = func(sortOperatorFactory);
//     }
//     else {
//         sortOperator = sortOperatorFactory->createOperator();
//     }

// }
// TEST(JitPerf, test_groupby_primitive_arg) {
//     test_groupby(true);
// }

// TEST(JitPerf, test_groupby_original) {
//     test_groupby(false);
// }

void test_sort(bool harden) {
    // build input data
    const int32_t DATA_SIZE = 100000;
    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }

    int rowCounts[1] = {DATA_SIZE};
    VectorBatch *datas[1];
    VectorBatch *vecBatch = new VectorBatch(DATA_SIZE, 2);
    Vector *column1 = new Vector(data1, VecType::INT32, DATA_SIZE);
    Vector *column2 = new Vector(data2, VecType::INT32, DATA_SIZE);
    vecBatch->setColumn(column1, VecType::INT32);
    vecBatch->setColumn(column2, VecType::INT32);
    datas[0] = vecBatch;

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    int32_t sourceTypes[] = {1, 1};
    int32_t outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int sortColTypes[] = {1, 1};
    int sortAscendings[] = {1, 1};
    int sortNullFirsts[] = {1, 1};
    int sortColCount = 2;

    SortOperatorFactory *sortOperatorFactory = SortOperatorFactory::createOperatorFactory(
            sourceTypes, 2, outputCols, 2, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    omniruntime::op::Operator *sortOperator = nullptr;
    if (harden) {
        auto p_sortCols = ParamValue(sortCols, 2);
        auto p_sortColTypes = ParamValue(sortColTypes, 2);
        auto p_sortAscendings = ParamValue(sortAscendings, 2);
        auto p_sortNullFirsts = ParamValue(sortNullFirsts, 2);
        auto p_sortColCount = ParamValue(&sortColCount);

        auto *compareToSp = new Specialization();
        compareToSp->addSpecializedParam(1, &p_sortCols);
        compareToSp->addSpecializedParam(2, &p_sortColTypes);
        compareToSp->addSpecializedParam(3, &p_sortAscendings);
        compareToSp->addSpecializedParam(4, &p_sortNullFirsts);
        compareToSp->addSpecializedParam(5, &p_sortColCount);

        std::map<std::string, Specialization> pagesIndexSps = {
                {OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp}
        };

        auto *sortContext = new omniruntime::jit::Context("sort", std::map<std::string, Specialization>(),
                                                          std::vector<std::string>(), std::vector<std::string>(), true);
        auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(),
                                                                std::vector<std::string>(), std::vector<std::string>());
        auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps,
                                                                std::vector<std::string>(), std::vector<std::string>());

        auto start = Time::now();

        Jit *jit = new Jit(
                std::vector<omniruntime::jit::Context>{*sortContext, *memoryPoolContext, *pagesIndexContext});
        auto createOperatorFunc = jit->specialize();

//        auto opt_conf = HammerConfig::getConf(2, 119);
//        auto JITTER = hammer1.create_jitter(deps,*opt_conf);
        auto t_created_jitter = Time::now();
        fsec fs = t_created_jitter - start;
        ms d = std::chrono::duration_cast<ms>(fs);
        std::cout << " create_jitter: " << d.count() << "ms\n";

        sortOperator = func(sortOperatorFactory);
    } else {
        sortOperator = sortOperatorFactory->createOperator();
    }

    sortOperator->addInput(datas, rowCounts, 1);
    vector<VectorBatch *> outputPages;
    auto t1 = Time::now();
    sortOperator->getOutput(outputPages);
    auto t0 = Time::now();
    fsec fs1 = t0 - t1;
    ms d1 = std::chrono::duration_cast<ms>(fs1);
    std::cout << (harden ? "optimized" : "original") << " sort duration time: " << d1.count() << "ms\n";

    delete[] data1;
    delete[] data2;
    delete vecBatch;
}

TEST(JitPerf, test_sort_original) {
    test_sort(false);
}

TEST(JitPerf, test_sort_harden) {
    test_sort(true);
}
