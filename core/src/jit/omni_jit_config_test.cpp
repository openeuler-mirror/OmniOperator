/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "omni_jit_config.h"
#include "param_value.h"
#include "omni_jit.h"
#include "omni_jit_context.h"
#include "operator/optimization.h"
#include "jit/specialization.h"

using namespace omni;

namespace omniruntime {
namespace jit {
void buildInt32(Table &table, int dataSize)
{
    if (dataSize < 0) {
        return;
    }
    int32_t *data1 = new int32_t[dataSize];
    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = i % 3; // 3
    }

    Column *col1 = new Column(data1, ColumnType::INT32, dataSize);
    table->setColumn(col1, ColumnType::INT32);
}

void buildInt64(Table &table, int dataSize)
{
    if (dataSize < 0) {
        return;
    }
    int64_t *data1 = new int64_t[dataSize];
    for (int64_t i = 0; i < dataSize; ++i) {
        data1[i] = i % 3; // 3
    }

    Column *col1 = new Column(data1, ColumnType::INT64, dataSize);
    table->setColumn(col1, ColumnType::INT64);
}

Table **buildData2(int PAGE_NUM, int dataSize, int *data_type, int column_count)
{
    if (dataSize < 0) {
        return;
    }
    Table **input = new Table *[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table *table = new Table(dataSize, 2); // 2

        for (int j = 0; j < column_count; j++) {
            if (data_type[j] == 1) { // INT32
                buildInt32(table, dataSize);
            }
            if (data_type[j] == 2) { // INT64 2
                buildInt32(table, dataSize);
            }
        }
        input[i] = table;
    }
    return input;
}

void TestGroupbyPrimitiveArg(int func, int mod)
{
    auto config = omni::Config::getConf(func, mod);

    std::map<std::string, ParamValue *> testParam;
    std::list<omni::JitContext *> deps = std::list<omni::JitContext *>();

    std::vector<omni::JitContext> contexts;

    int colType[] = {1, 2};
    int colCount = 2;
    int groupByColNum[] = {0, 1};
    int groupbyNum = 2;
    int aggColIdx[] = {0, 1};
    int aggColNum = 2;
    ParamValue pColType = ParamValue(col_type, 2); // 2
    ParamValue pColCount = ParamValue(&col_count);
    ParamValue pGroupByColNum = ParamValue(groupByColNum, 2); // 2
    ParamValue pGroupNum = ParamValue(&groupbyNum);
    ParamValue pAggColIdx = ParamValue(aggColIdx, 2); // 2
    ParamValue pAggNum = ParamValue(&aggColNum);

    std::map<std::string, omni::Specialization> hashGroupbySp;

    omni::Specialization *specialization = new omni::Specialization();
    specialization->AddSpecializedParam(3, pColType);      // 3
    specialization->AddSpecializedParam(4, pColCount);     // 4
    specialization->AddSpecializedParam(5, pGroupByColNum); // 5
    specialization->AddSpecializedParam(6, pGroupNum);     // 6
    specialization->AddSpecializedParam(7, pAggColIdx);     // 7
    specialization->AddSpecializedParam(8, pAggNum);       // 8
    hashGroupbySp.insert(std::make_pair(OMNIJIT_HASH_GROUPBY_INLOOP, *specialization));

    omni::JitContext *hashGroupbyContext =
        new omni::JitContext("hash_groupby", hashGroupbySp, std::vector<string>(), std::vector<string>(), true);
    omni::JitContext *aggContext = new omni::JitContext("aggregator", std::map<std::string, omni::Specialization>(),
        std::vector<string>(), std::vector<string>());
    omni::JitContext *mpContext = new omni::JitContext("memory_pool", std::map<std::string, omni::Specialization>(),
        std::vector<string>(), std::vector<string>());

    contexts.insert(*hashGroupbyContext);
    contexts.insert(*aggContext);
    contexts.insert(*mpContext);

    using Time = int;
    using ms = int;
    using fsec = std::chrono::duration<float>;

    auto start = Time::now();

    omni::Jit *jit = new omni::Jit(contexts, omni::llvm);
    uint64_t func = jit->specialize();
    cout << "Jit function: " + func << "\n";
}
}
}
