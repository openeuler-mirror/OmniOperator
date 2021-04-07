#include "api.h"
#include "../harden/Hammer.h"
#include "../harden/HammerConfig.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
#include <sstream>
#include <string>
#endif

JitContext* optimizeByLlvm( PrepareContext groupByCols, 
                            PrepareContext groupByTypes, 
                            PrepareContext aggCols, 
                            PrepareContext aggTypes,
                            PrepareContext aggFuncTypes)
{
    using namespace codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    int32_t groupColNum = groupByCols.len;
    int32_t aggColNum = aggCols.len;
    int32_t colNum = groupByCols.len + aggCols.len;
    int32_t* colTypes = new int32_t[colNum];
    
    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByCols.context[i]] = groupByTypes.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggCols.context[i]] = aggTypes.context[i];
    }

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByCols.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggCols.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypes.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypes.context, aggColNum);

    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@3"] = &p_col_type;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@4"] = &p_col_count;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@6"] = &p_group_num;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@8"] = &p_agg_num;
    testParam["_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_@9"] = &p_agg_types;
    
    testParam["processAgg@2"] =  &p_agg_types;
    testParam["processAgg@3"] =  &p_agg_num;
    testParam["processAgg@4"] =  &p_col_type;
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    Hammer hammer1("/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/memory_pool.ll", testParam);
    Hammer hammer2("/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/hash_groupby.ll", testParam);
    Hammer hammer3("/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/aggregator.ll", testParam);
    Hammer hammer4("/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/memory_pool.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();
    hammer4.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    deps.push_back(&hammer4);
    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);

    auto func = (opt_module)(jitter->lookup("_Z17createHashGroupByRSt6vectorI11ColumnIndexSaIS0_EES3_RS_IP10AggregatorSaIS5_EE")->getAddress());
    JitContext* jitContext = new JitContext;
    jitContext->func = func;
    jitContext->jitter = jitter.release();

    return jitContext;
}

uint64_t prepareHashGroupBy(PrepareContext groupByCols, 
                        PrepareContext groupByTypes, 
                        PrepareContext aggCols, 
                        PrepareContext aggTypes, 
                        PrepareContext aggFuncTypes,
                        PrepareContext retTypes) 
{
#if DEBUG_LEVEL_LOW
        std::stringstream os;
        os << std::this_thread::get_id();
        DebugPrint("Thread %s is creating module...", os.str().c_str());
#endif
    JitContext* jitContext = optimizeByLlvm(groupByCols, groupByTypes, aggCols, aggTypes, aggFuncTypes);
#if DEBUG_LEVEL_LOW
    auto end_time = Time::now();
    fsec fs = start_time - end_time;
    ms d = std::chrono::duration_cast<ms>(fs);
    DebugPrint("Compilation stage elapsed time: %s ms", std::to_string(d.count()).c_str());
#endif
    return reinterpret_cast<uint64_t>(jitContext);
}

uint64_t createOperator(int64_t moduleAddr,
                        PrepareContext groupByCols, 
                        PrepareContext groupByTypes, 
                        PrepareContext aggCols, 
                        PrepareContext aggTypes, 
                        PrepareContext aggFuncTypes,
                        PrepareContext retTypes)
{
    #if DEBUG_LEVEL_LOW
    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto start_time = Time::now();
#endif

    std::vector<ColumnIndex> groupByIndex;
    std::vector<ColumnIndex> aggIndex;
    std::vector<Aggregator*> aggs;
    
    for (int32_t i = 0; i < groupByCols.len; ++i) {
        ColumnIndex c = {groupByCols.context[i], (ColumnType)groupByTypes.context[i]};
        groupByIndex.push_back(c);
    }
    for (int32_t i = 0; i < aggCols.len; ++i) {
        ColumnIndex c = {aggCols.context[i], (ColumnType)aggTypes.context[i]};
        aggIndex.push_back(c);

        if ((AggregateType)aggFuncTypes.context[i] == SUM) {
            switch (aggTypes.context[i])
            {
                case INT32: {
                    SumAggregator* agg = new SumAggregator(1);
                    aggs.push_back(agg);
                    break;
                }
                case INT64: {
                    SumAggregator* agg = new SumAggregator(2);
                    aggs.push_back(agg);
                    break;
                }
                case DOUBLE: {
                    SumAggregator* agg = new SumAggregator(3);
                    aggs.push_back(agg);
                    break;
                }
                default:
                    break;
            }
        }
    }

    JitContext* context = reinterpret_cast<JitContext*>(moduleAddr);
    HashGroupBy* groupby = context->func(groupByIndex, aggIndex, aggs);
    return reinterpret_cast<uint64_t>(groupby);
}

// temporarily use uint64_t as key. change back to string once llvm optimization supported.
uint64_t executeHashGroupByLlvm(int64_t operatorAddr, uint32_t* colTypes, uint32_t typeCount, void** t, uint32_t columnCount, uint32_t rowNum)
{
#ifdef DEBUG_LEVEL_HIGH 
    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto t0 = Time::now();
#endif
    Table* table = new Table(rowNum, columnCount);
    for (int i = 0; i < columnCount; i++) {
        void* data = t[i];
        ColumnType columnType = static_cast<ColumnType>(colTypes[i]);
        Column* col = new Column(data, columnType, rowNum);
        table->setColumn(col, columnType);
    }
    
    HashGroupBy* groupBy = reinterpret_cast<HashGroupBy*>(operatorAddr);
    groupBy->process(table, table->getPositionCount());
#ifdef DEBUG_LEVEL_HIGH 
        auto t1 = Time::now();
        fsec fs = t1 - t0;
        ms d = std::chrono::duration_cast<ms>(fs);

        std::stringstream os;
        os << std::this_thread::get_id();
        DebugPrint("Thread %s processed one page in %ld ms.", os.str().c_str(), d.count());
#endif
    return reinterpret_cast<uint64_t>(groupBy);
}


Table* executeAggFinal(int64_t opAddr) 
{
#ifdef DEBUG_LEVEL_LOW
    DebugFuncEntry;
#endif
    HashGroupBy* groupBy = reinterpret_cast<HashGroupBy*>(opAddr);
    if (groupBy == nullptr) {
        DebugError("No operator %ld exists.", 0x11111);
    }
    Table* result = groupBy->getResult();
    delete groupBy;
#ifdef DEBUG_LEVEL_LOW
    DebugFuncExit;
#endif
    return result;
}