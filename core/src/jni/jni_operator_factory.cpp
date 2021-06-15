//
// Created by root on 5/26/21.
//

#include "jni_operator_factory.h"
#include "../memory/memory_pool.h"
#include "../jit/param_value.h"
#include "../jit/hammer.h"
#include "../operator/operator_factory.h"
#include "../operator/sort/sort.h"
#include "../operator/aggregation/group_aggregation.h"
#include "../operator/aggregation/non_group_aggregation.h"
#include "../operator/filter/filter.h"
#include "../operator/join/hash_builder.h"
#include "../operator/join/lookup_join.h"
#include "config.h"

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperator
        (JNIEnv *env, jobject jObj, jlong jNativeFactoryObj)
{
    JNI_DEBUG_LOG("create omni operator starting.");
    auto start = START();
    OperatorFactory *operatorFactory = (OperatorFactory *)jNativeFactoryObj;
    JitContext *jitContext = operatorFactory->getJitContext();
    omniruntime::op::Operator *nativeOperator = NULL;

#ifdef DEBUG_OPERATOR
    nativeOperator = operatorFactory->createOperator();
    JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
#else
    if (jitContext == NULL) {
        nativeOperator = operatorFactory->createOperator();
        JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
    }
    else {
        opt_module opModule = (opt_module)(jitContext->func);
        nativeOperator = opModule(operatorFactory);
        JNI_DEBUG_LOG("JIT create omni operator finished, elapsed time: %ld ms.", END(start));
    }
#endif
    return reinterpret_cast<int64_t>(nativeOperator);
}

/**
 * Return an HashAggregationFactory object address.
 **/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jGroupByChannel, jintArray jGroupByType, jintArray jAggChannel, jintArray jAggType, jintArray jAggFuncType, jintArray jOutPutTye)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and type
    jint *groupByCols = env->GetIntArrayElements(jGroupByChannel, JNI_FALSE);
    jint *groupByTypes = env->GetIntArrayElements(jGroupByType, JNI_FALSE);
    jint *aggCols = env->GetIntArrayElements(jAggChannel, JNI_FALSE);
    jint *aggTypes = env->GetIntArrayElements(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);

    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    size_t aggNum = (size_t)env->GetArrayLength(jAggChannel);

    PrepareContext groupByColContext = {(uint32_t*)groupByCols, groupByNum};
    PrepareContext groupByTypeContext = {(uint32_t*)groupByTypes, groupByNum};
    PrepareContext aggColContext = {(uint32_t*)aggCols, aggNum};
    PrepareContext aggTypeContext = {(uint32_t*)aggTypes, aggNum};
    PrepareContext aggFuncTypeContext = {(uint32_t*)aggFuncTypes, aggNum};

    // return prepareHashGroupBy(groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);
    using namespace omniruntime::codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
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

    testParam["_ZN11omniruntime2op23HashAggregationOperator6inLoopEPPcjPiiS4_iS4_iS4_@3"] = &p_col_type;
    testParam["_ZN11omniruntime2op23HashAggregationOperator6inLoopEPPcjPiiS4_iS4_iS4_@4"] = &p_col_count;
    testParam["_ZN11omniruntime2op23HashAggregationOperator6inLoopEPPcjPiiS4_iS4_iS4_@6"] = &p_group_num;
    testParam["_ZN11omniruntime2op23HashAggregationOperator6inLoopEPPcjPiiS4_iS4_iS4_@8"] = &p_agg_num;
    testParam["_ZN11omniruntime2op23HashAggregationOperator6inLoopEPPcjPiiS4_iS4_iS4_@9"] = &p_agg_types;

    testParam["processAgg@2"] =  &p_agg_num;
    testParam["processAgg@3"] =  &p_col_type;

    testParam["_ZN11omniruntime2op23HashAggregationOperator19constructHashColumnEP5TablePiji@2"] = &p_col_type;
    testParam["_ZN11omniruntime2op23HashAggregationOperator19constructHashColumnEP5TablePiji@3"] = &p_group_num;
    testParam["_ZN11omniruntime2op23HashAggregationOperator18constructAggColumnEP5TablePiji@2"] = &p_col_type;
    testParam["_ZN11omniruntime2op23HashAggregationOperator18constructAggColumnEP5TablePiji@3"] = &p_agg_num;
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");
    Hammer hammer1("/opt/lib/ir/memory_pool.ll", testParam);
    Hammer hammer2("/opt/lib/ir/group_aggregation.ll", testParam);
    Hammer hammer3("/opt/lib/ir/aggregator.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);

    auto func = (opt_module)(jitter->lookup("_ZN11omniruntime2op30HashAggregationOperatorFactory14createOperatorEv")->getAddress());
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext);
    nativeOperatorFactory->setJitContext(jitContext);
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

/**
 * Return an AggregationFactory object address.
 **/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jAggType, jintArray jAggFuncType)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    jint *aggTypes = env->GetIntArrayElements(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    size_t aggNum = (size_t)env->GetArrayLength(jAggType);

    PrepareContext aggTypeContext = {(uint32_t*)aggTypes, aggNum};
    PrepareContext aggFuncTypeContext = {(uint32_t*)aggFuncTypes, aggNum};
    int32_t aggColNum = aggTypeContext.len;

    using namespace omniruntime::codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    
    ParamValue p_col_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    testParam["_ZN11omniruntime2op19AggregationOperator6inLoopEPPcjPiS4_@3"] = &p_agg_num;
    testParam["_ZN11omniruntime2op19AggregationOperator6inLoopEPPcjPiS4_@4"] = &p_col_type;   
    testParam["_ZN11omniruntime2op19AggregationOperator6inLoopEPPcjPiS4_@5"] = &p_agg_types;
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");
    Hammer hammer1("/opt/lib/ir/memory_pool.ll", testParam);
    Hammer hammer2("/opt/lib/ir/non_group_aggregation.ll", testParam);
    Hammer hammer3("/opt/lib/ir/aggregator.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);

    auto func = (opt_module)(jitter->lookup("_ZN11omniruntime2op26AggregationOperatorFactory14createOperatorEv")->getAddress());
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());
    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext);
    nativeOperatorFactory->setJitContext(jitContext); 
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JitContext *createSortJitContext(
        int32_t *sourceTypes,
        int32_t typesCount,
        int32_t *outputCols,
        int32_t outputColsCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColsCount);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: ([I[I[I[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jSourceTypes, jintArray jOutputCols, jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts)
{
    JNI_DEBUG_LOG("create sort operator factory starting.");
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    jint sourceTypesCount = env->GetArrayLength(jSourceTypes);
    jint outputColsCount = env->GetArrayLength(jOutputCols);
    jint sortColsCount = env->GetArrayLength(jSortCols);

    JNI_DEBUG_LOG("before create sort operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::SortOperatorFactory *sortOperatorFactory = omniruntime::op::SortOperatorFactory::createSortOperatorFactory(
            sourceTypesArr,
            sourceTypesCount,
            outputColsArr,
            outputColsCount,
            sortColsArr,
            ascendingsArr,
            nullFirstsArr,
            sortColsCount);
    JitContext *jitContext = createSortJitContext(
            sortOperatorFactory->getSourceTypes(),
            sortOperatorFactory->getSourceTypeCount(),
            sortOperatorFactory->getOutputCols(),
            sortOperatorFactory->getOutputColCount(),
            sortOperatorFactory->getSortCols(),
            sortOperatorFactory->getSortAscendings(),
            sortOperatorFactory->getSortNullFirsts(),
            sortOperatorFactory->getSortColCount());
    sortOperatorFactory->setJitContext(jitContext);
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)sortOperatorFactory;
}

JitContext *createSortJitContext(
        int32_t *sourceTypes,
        int32_t typesCount,
        int32_t *outputCols,
        int32_t outputColsCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColsCount)
{
    JNI_DEBUG_LOG("create jit sort context starting.");
    auto start = START();
    using namespace omniruntime::codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    int sortColTypes[sortColsCount];

    for (int32_t i = 0; i < sortColsCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue p_typeCount = ParamValue(&typesCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColsCount);
    ParamValue p_outputColCount = ParamValue(&outputColsCount);
    ParamValue p_sortCols = ParamValue(sortCols, sortColsCount);
    ParamValue p_sortColTypes = ParamValue(sortColTypes, sortColsCount);
    ParamValue p_sortAscendings = ParamValue(sortAscendings, sortColsCount);
    ParamValue p_sortNullFirsts = ParamValue(sortNullFirsts, sortColsCount);
    ParamValue p_sortColCount = ParamValue(&sortColsCount);

    testParam["_Z9compareTolPiS_S_S_iii@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_iii@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_iii@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_iii@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_iii@5"] = &p_sortColCount;

    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@1"] = &p_sourceTypes;
    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@2"] = &p_outputCols;
    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@3"] = &p_outputColCount;

    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@1"] = &p_outputCols;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@2"] = &p_outputColCount;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@4"] = &p_sourceTypes;

    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

    Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
    Hammer hammer2("/opt/lib/ir/pages_index.ll", testParam);
    Hammer hammer3("/opt/lib/ir/memory_pool.ll", testParam);

    hammer1.harden();
    hammer2.harden();
    hammer3.harden();
    deps.push_back(&hammer2);
    deps.push_back(&hammer3);

    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto func = (opt_module)(jitter->lookup("_ZN11omniruntime2op19SortOperatorFactory14createOperatorEv")->getAddress());

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);;
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    JNI_DEBUG_LOG("create jit sort context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jInputTypes, jint jInputLength, jstring jExpression, jintArray jProjectIndices, jint jProjectLength)
{
    std::string filterExpression = std::string(env->GetStringUTFChars(jExpression, JNI_FALSE));
    jint *inputTypes = env->GetIntArrayElements(jInputTypes, JNI_FALSE);
    int32_t inputLength = (int32_t) jInputLength;
    jint *projectIndices = env->GetIntArrayElements(jProjectIndices, JNI_FALSE);
    int32_t projectLength = (int32_t) jProjectLength;
    omniruntime::op::FilterAndProjectOperatorFactory *factory = new omniruntime::op::FilterAndProjectOperatorFactory(filterExpression, inputTypes, inputLength, projectIndices, projectLength);
    return (int64_t) factory;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jBuildTypes, jintArray jBuildOutputCols, jintArray jBuildHashCols, jint jOperatorCount)
{
    JNI_DEBUG_LOG("create hash builder operator factory starting.");
    auto start = START();
    jint *buildTypesArr = env->GetIntArrayElements(jBuildTypes, JNI_FALSE);
    jint *buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    jint *buildHashColsArr = env->GetIntArrayElements(jBuildHashCols, JNI_FALSE);

    jint buildTypesCount = env->GetArrayLength(jBuildTypes);
    jint buildOutputColsCount = env->GetArrayLength(jBuildOutputCols);
    jint buildHashColsCount = env->GetArrayLength(jBuildHashCols);

    JNI_DEBUG_LOG("before create hash builder operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::HashBuilderOperatorFactory *hashBuilderOperatorFactory = omniruntime::op::HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
            buildTypesArr,
            buildTypesCount,
            buildOutputColsArr,
            buildOutputColsCount,
            buildHashColsArr,
            buildHashColsCount,
            jOperatorCount);
    JitContext *jitContext = NULL;
    hashBuilderOperatorFactory->setJitContext(jitContext);
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)hashBuilderOperatorFactory;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory
        (JNIEnv *env, jobject jObj, jintArray jProbeTypes, jintArray jProbeOutputCols, jintArray jProbeHashCols,
        jintArray jBuildOutputCols, jintArray jBuildOutputTypes, jlong jHashBuilderOperatorFactory)
{
    JNI_DEBUG_LOG("create lookup join operator factory starting.");
    auto start = START();
    jint *probeTypesArr = env->GetIntArrayElements(jProbeTypes, JNI_FALSE);
    jint *probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    jint *probeHashColsArr = env->GetIntArrayElements(jProbeHashCols, JNI_FALSE);
    jint *buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    jint *buildOutputTypesArr = env->GetIntArrayElements(jBuildOutputTypes, JNI_FALSE);

    jint probeTypesCount = env->GetArrayLength(jProbeTypes);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);
    jint probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    jint buildOutputColsCount = env->GetArrayLength(jBuildOutputCols);

    JNI_DEBUG_LOG("before create lookup join operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::LookupJoinOperatorFactory *lookupJoinOperatorFactory = omniruntime::op::LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
            probeTypesArr,
            probeTypesCount,
            probeOutputColsArr,
            probeOutputColsCount,
            probeHashColsArr,
            probeHashColsCount,
            buildOutputColsArr,
            buildOutputTypesArr,
            buildOutputColsCount,
            jHashBuilderOperatorFactory);
    JitContext *jitContext = NULL;
    lookupJoinOperatorFactory->setJitContext(jitContext);
    JNI_DEBUG_LOG("create lookup join operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)lookupJoinOperatorFactory;
}
