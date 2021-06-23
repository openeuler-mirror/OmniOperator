#include "omni_jit_config.h"
#include "../operator/aggregator/hash_groupby.h"
#include "param_value.h"
#include "omni_jit.h"
#include "omni_jit_context.h"
#include "../operator/optimization.h"
#include "../jit/specialization.h"


using namespace omni;

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
    auto config = omni::Config::getConf(func, mod);

    std::map<std::string, ParamValue *> testParam;
    std::list<omni::JitContext *> deps = std::list<omni::JitContext *>();

    std::vector<omni::JitContext> contexts;

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

    std::map<std::string, omni::Specialization> hashGroupbySp;

    omni::Specialization *specialization = new omni::Specialization();
    specialization->addSpecializedParam(3, p_col_type);
    specialization->addSpecializedParam(4, p_col_count);
    specialization->addSpecializedParam(5, p_groupByColNum);
    specialization->addSpecializedParam(6, p_group_num);
    specialization->addSpecializedParam(7, p_aggColIdx);
    specialization->addSpecializedParam(8, p_agg_num);
    hashGroupbySp.insert(std::make_pair(OMNIJIT_HASH_GROUPBY_INLOOP, *specialization));

    omni::JitContext *hashGroupbyContext = new omni::JitContext("hash_groupby", hashGroupbySp, std::vector<string>(),
                                                                std::vector<string>(), true);
    omni::JitContext *aggContext = new omni::JitContext("aggregator", std::map<std::string, omni::Specialization>(),
                                                        std::vector<string>(), std::vector<string>());
    omni::JitContext *mpContext = new omni::JitContext("memory_pool", std::map<std::string, omni::Specialization>(),
                                                       std::vector<string>(), std::vector<string>());

    contexts.insert(*hashGroupbyContext);
    contexts.insert(*aggContext);
    contexts.insert(*mpContext);


    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto start = Time::now();

    omni::Jit *jit = new omni::Jit(contexts, omni::llvm);
    uint64_t func = jit->specialize();
    cout << "Jit function: " + func << "\n";

    // fsec fs = t_created_jitter - start;
    // ms d = std::chrono::duration_cast<ms>(fs);
    // auto create_jitter = d.count();

    // auto createGroupby_func = JITTER->lookup("_Z13createGroupByv");

    // auto t_lookup_func = Time::now();
    // fs = t_lookup_func - t_created_jitter;
    // d = std::chrono::duration_cast<ms>(fs);
    // auto lookup_func1 = d.count();

    // createGroupby_func = JITTER->lookup("_Z13createGroupByv");
    // auto t_lookup_func2 = Time::now();
    // fs = t_lookup_func2 - t_lookup_func;
    // d = std::chrono::duration_cast<ms>(fs);
    // auto lookup_func2 = d.count();

    // if (createGroupby_func) {
    //     auto PAGE_NUM = 25000;
    //     auto DATA_SIZE = 1000;
    //     auto createGrouby = (HashGroupBy *(*)()) createGroupby_func->getAddress();
    //     auto groupBy = createGrouby();
    //     auto input = buildData2(PAGE_NUM, DATA_SIZE, col_type, col_count);

    //     auto t1 = Time::now();
    //     int result = 0;
    //     for (int32_t i = 0; i < PAGE_NUM; ++i) {
    //         groupBy->process(input[i], DATA_SIZE);
    //     }
    //     auto t0 = Time::now();
    //     fs = t0 - t1;
    //     d = std::chrono::duration_cast<ms>(fs);
    //     auto agg_exec = d.count();

    //     cout << "func_config: " << func
    //          << " module_config: " << mod
    //          << " create_jitter: " << create_jitter
    //          << " lookup: " << lookup_func1
    //          << " execution: " << agg_exec
    //          << "\n";
    // }

}

//int main(int argc, char **argv) {
//    test_groupby_primitive_arg(atol(argv[1]), atol(argv[2]));
//    return 0;
//}
