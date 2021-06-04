//
// Created by kkrazy on 2021-03-23.
//

#include "hammer_config.h"
#include "../operator/aggregator/hash_groupby.h"
#include "param_value.h"
#include "hammer.h"

using namespace omnirumtime::codegen;

Table **buildData2(int PAGE_NUM, int DATA_SIZE, int *data_type, int column_count) {
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

void test_groupby_primitive_arg(int func, int mod) {
    auto config = HammerConfig::getConf(func, mod);

    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();

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
    Hammer hammer5("../operator/ir/sort.ll", testParam);
    Hammer hammer6("../operator/ir/sort_api.ll", testParam);
    hammer2.harden();
    hammer1.harden();
    hammer3.harden();
    hammer4.harden();
    hammer5.harden();
    hammer6.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    deps.push_back(&hammer4);
    deps.push_back(&hammer5);
    deps.push_back(&hammer6);

    auto JITTER = hammer1.create_jitter(deps, *config);
    auto t_created_jitter = Time::now();
    fsec fs = t_created_jitter - start;
    ms d = std::chrono::duration_cast<ms>(fs);
    auto create_jitter = d.count();

    auto createGroupby_func = JITTER->lookup("_Z13createGroupByv");

    auto t_lookup_func = Time::now();
    fs = t_lookup_func - t_created_jitter;
    d = std::chrono::duration_cast<ms>(fs);
    auto lookup_func1 = d.count();

    createGroupby_func = JITTER->lookup("_Z13createGroupByv");
    auto t_lookup_func2 = Time::now();
    fs = t_lookup_func2 - t_lookup_func;
    d = std::chrono::duration_cast<ms>(fs);
    auto lookup_func2 = d.count();

    if (createGroupby_func) {
        auto PAGE_NUM = 25000;
        auto DATA_SIZE = 1000;
        auto createGrouby = (HashGroupBy *(*)()) createGroupby_func->getAddress();
        auto groupBy = createGrouby();
        auto input = buildData2(PAGE_NUM, DATA_SIZE, col_type, col_count);

        auto t1 = Time::now();
        int result = 0;
        for (int32_t i = 0; i < PAGE_NUM; ++i) {
            groupBy->process(input[i], DATA_SIZE);
        }
        auto t0 = Time::now();
        fs = t0 - t1;
        d = std::chrono::duration_cast<ms>(fs);
        auto agg_exec = d.count();

        cout << "func_config: " << func
             << " module_config: " << mod
             << " create_jitter: " << create_jitter
             << " lookup: " << lookup_func1
             << " execution: " << agg_exec
             << "\n";
    }

}

//int main(int argc, char **argv) {
//    test_groupby_primitive_arg(atol(argv[1]), atol(argv[2]));
//    return 0;
//}
