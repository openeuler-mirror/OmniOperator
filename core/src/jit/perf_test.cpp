#include "hammer.h"
#include "llvm/IRReader/IRReader.h"
#include "gtest/gtest.h"

#include "../operator/aggregator/hash_groupby.h"
#include "../vector/type.h"
#include "../vector/table.h"
#include "param_value.h"

using namespace codegen;

Table **buildData(int PAGE_NUM, int DATA_SIZE, int *data_type, int column_count) {
    Table **input = new Table *[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table *table = new Table(DATA_SIZE, 2);

        for (int j = 0; j < column_count; j++) {
            if (data_type[j] == 1) //INT32
            {
                int32_t *data1 = new int32_t[DATA_SIZE];
                for (int32_t i = 0; i < DATA_SIZE; ++i) {
                    data1[i] = i % 3;
                }

                Column *col1 = new Column(data1, ColumnType::INT32, DATA_SIZE);
                table->setColumn(col1, ColumnType::INT32);
            }
            if (data_type[j] == 2) //INT64
            {
                int64_t *data1 = new int64_t[DATA_SIZE];
                for (int64_t i = 0; i < DATA_SIZE; ++i) {
                    data1[i] = i % 3;
                }

                Column *col1 = new Column(data1, ColumnType::INT64, DATA_SIZE);
                table->setColumn(col1, ColumnType::INT64);
            }
        }
        input[i] = table;
    }
    return input;
}

void test_groupby(bool harden) {
    std::map < std::string, ParamValue * > testParam;
    std::list < Hammer * > deps = std::list<Hammer *>();

    int col_type[] = {1, 2};
    int col_count = 2;
    int groupByColNum[] = {0, 1};
    int groupbyNum = 2;
    int aggColIdx[] = {0, 1};
    int aggColNum = 2;
    ParamValue p_col_type = ParamValue(col_type, 2);
    ParamValue p_col_count = ParamValue(&col_count);
    ParamValue p_groupByColNum = ParamValue(groupByColNum, 2);
    ParamValue p_group_num = ParamValue(&groupbyNum);
    ParamValue p_aggColIdx = ParamValue(aggColIdx, 2);
    ParamValue p_agg_num = ParamValue(&aggColNum);

    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@3"] = &p_col_type;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@4"] = &p_col_count;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@5"] = &p_groupByColNum;

    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@6"] = &p_group_num;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@7"] = &p_aggColIdx;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@8"] = &p_agg_num;

    testParam["_Z13test_group_byiiPii@2"] = &p_col_type;
    testParam["_Z13test_group_byiiPii@3"] = &p_col_count;

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto start = Time::now();

    Hammer hammer1("../operator/ir/test.ll", testParam);
    Hammer hammer2("../operator/ir/hash_groupby.ll", testParam);
    Hammer hammer3("../operator/ir/aggregator.ll", testParam);
    Hammer hammer4("../operator/ir/memory_pool.ll", testParam);

    if (harden) {
        hammer2.harden();
        hammer1.harden();
        hammer3.harden();
        hammer4.harden();
    }

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    deps.push_back(&hammer4);



    auto opt_conf = HammerConfig::getConf(2, 121);
    auto JITTER = hammer1.create_jitter(deps,*opt_conf);
    auto t_created_jitter = Time::now();
    fsec fs = t_created_jitter - start;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << " create_jitter: " << d.count() << "ms\n";

    auto createGroupby_func = JITTER->lookup("_Z13createGroupByv");

    auto t_lookup_func = Time::now();
    fs = t_lookup_func - t_created_jitter;
    d = std::chrono::duration_cast<ms>(fs);
    std::cout << " lookup func: " << d.count() << "ms\n";

    createGroupby_func = JITTER->lookup("_Z13createGroupByv");
    auto t_lookup_func2 = Time::now();
    fs = t_lookup_func2 - t_lookup_func;
    d = std::chrono::duration_cast<ms>(fs);
    std::cout << " lookup func 2: " << d.count() << "ms\n";

    if (createGroupby_func) {
        auto PAGE_NUM = 25000;
        auto DATA_SIZE = 4000;
        auto createGrouby = (HashGroupBy *(*)()) createGroupby_func->getAddress();
        auto groupBy = createGrouby();
        auto input = buildData(PAGE_NUM, DATA_SIZE, col_type, col_count);

        auto t1 = Time::now();
        int result = 0;
        for (int32_t i = 0; i < PAGE_NUM; ++i) {
            groupBy->process(input[i], DATA_SIZE);
        }
        auto t0 = Time::now();
        fs = t0 - t1;
        d = std::chrono::duration_cast<ms>(fs);
        std::cout << (harden ? "optimized" : "original") << " agg duration time: " << d.count() << "ms\n";
        printf("\nresult: %d", result);
    }

}
TEST(hammer, test_groupby_primitive_arg) {
    test_groupby(true);
}

TEST(hammer, test_groupby_original) {
    test_groupby(false);
}