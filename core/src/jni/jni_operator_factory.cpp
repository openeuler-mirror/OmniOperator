/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "jni_operator_factory.h"
#include "../memory/memory_pool.h"
#include "../jit/param_value.h"
#include "../jit/jit.h"
#include "../operator/operator_factory.h"
#include "../operator/sort/sort.h"
#include "../operator/aggregation/group_aggregation.h"
#include "../operator/aggregation/non_group_aggregation.h"
#include "../operator/filter/filter_and_project.h"
#include "../operator/projection/projection.h"
#include "../operator/window/window.h"
#include "../operator/join/hash_builder.h"
#include "../operator/join/lookup_join.h"
#include "../operator/topn/topn.h"
#include "../operator/optimization.h"
#include "config.h"

using omniruntime::jit::ParamValue;
using omniruntime::jit::Specialization;

using namespace omniruntime::op;

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperatorNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperatorNative(JNIEnv *env, jobject jObj, jlong jNativeFactoryObj)
{
    JNI_DEBUG_LOG("create omni operator starting.");
    auto start = START();
    OperatorFactory *operatorFactory = (OperatorFactory *)jNativeFactoryObj;
    JitContext *jitContext = operatorFactory->GetJitContext();
    omniruntime::op::Operator *nativeOperator = nullptr;

#ifdef DEBUG_OPERATOR
    nativeOperator = operatorFactory->CreateOperator();
    JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
#else
    if (jitContext == nullptr) {
        nativeOperator = operatorFactory->CreateOperator();
        JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
    } else {
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
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jGroupByChannel, jintArray jGroupByType, jintArray jAggChannel, jintArray jAggType,
    jintArray jAggFuncType, jintArray jOutPutTye, jboolean inputRaw, jboolean outputPartial)
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

    using namespace omniruntime::jit;
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

    auto *inloopSp = new Specialization();
    inloopSp->addSpecializedParam(3, &p_col_type);
    inloopSp->addSpecializedParam(4, &p_col_count);
    inloopSp->addSpecializedParam(6, &p_group_num);
    inloopSp->addSpecializedParam(8, &p_agg_num);
    inloopSp->addSpecializedParam(9, &p_agg_types);

    auto *processAggSp = new Specialization();
    processAggSp->addSpecializedParam(2, &p_agg_num);
    processAggSp->addSpecializedParam(3, &p_col_type);

    auto *hashColumnSp = new Specialization();
    hashColumnSp->addSpecializedParam(2, &p_col_type);
    hashColumnSp->addSpecializedParam(3, &p_group_num);

    auto *aggColumnSp = new Specialization();
    aggColumnSp->addSpecializedParam(2, &p_col_type);
    aggColumnSp->addSpecializedParam(3, &p_agg_num);

    std::map<std::string, Specialization> hashGroupbySps = {
        {OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp},
        // TODO: open this optimization
//        {OMNIJIT_HASH_GROUPBY_HASH_COLUMN, *hashColumnSp},
//        {OMNIJIT_HASH_GROUPBY_AGG_COLUMN, *aggColumnSp},
        {OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("group_aggregation", hashGroupbySps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

/**
 * Return an AggregationFactory object address.
 **/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jAggType, jintArray jAggFuncType, jboolean inputRaw, jboolean outputPartial)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    jint *aggTypes = env->GetIntArrayElements(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    size_t aggNum = (size_t)env->GetArrayLength(jAggType);

    PrepareContext aggTypeContext = {(uint32_t*)aggTypes, aggNum};
    PrepareContext aggFuncTypeContext = {(uint32_t*)aggFuncTypes, aggNum};
    int32_t aggColNum = aggTypeContext.len;

    using namespace omniruntime::jit;
    std::map<std::string, ParamValue *> testParam;

    ParamValue p_col_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->addSpecializedParam(3, &p_agg_num);
    inloopSp->addSpecializedParam(4, &p_col_type);
    inloopSp->addSpecializedParam(5, &p_agg_types);

    std::map<std::string, Specialization> nonGroupSps = {
            {OMNIJIT_NON_GROUP_INLOOP, *inloopSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("non_group_aggregation", nonGroupSps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
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
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jSourceTypes, jintArray jOutputCols, jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts)
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
        sortOperatorFactory->GetSourceTypes(),
        sortOperatorFactory->getSourceTypeCount(),
        sortOperatorFactory->GetOutputCols(),
        sortOperatorFactory->GetOutputColCount(),
        sortOperatorFactory->getSortCols(),
        sortOperatorFactory->getSortAscendings(),
        sortOperatorFactory->getSortNullFirsts(),
        sortOperatorFactory->getSortColCount());
    sortOperatorFactory->SetJitContext(jitContext);
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
    JNI_DEBUG_LOG("create sort jit context starting.");
    auto start = START();
    using namespace omniruntime::jit;
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

    auto *compareToSp = new Specialization();
    compareToSp->addSpecializedParam(1, &p_sortCols);
    compareToSp->addSpecializedParam(2, &p_sortColTypes);
    compareToSp->addSpecializedParam(3, &p_sortAscendings);
    compareToSp->addSpecializedParam(4, &p_sortNullFirsts);
    compareToSp->addSpecializedParam(5, &p_sortColCount);

    auto *allocColumnsSp = new Specialization();
    allocColumnsSp->addSpecializedParam(1, &p_sourceTypes);
    allocColumnsSp->addSpecializedParam(2, &p_outputCols);
    allocColumnsSp->addSpecializedParam(3, &p_outputColCount);

    auto *getOutputSp = new Specialization();
    getOutputSp->addSpecializedParam(1, &p_outputCols);
    getOutputSp->addSpecializedParam(2, &p_outputColCount);
    getOutputSp->addSpecializedParam(4, &p_sourceTypes);

    std::map<std::string, Specialization> pagesIndexSps = {
        {OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp},
        {OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp}
    };

    auto *sortContext = new omniruntime::jit::Context("sort", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps, std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*sortContext, *memoryPoolContext, *pagesIndexContext});
    auto createOperatorFunc = jit->specialize();

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create sort jit context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *createWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols, int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jSourceTypes,jintArray jOutputChannels,jintArray jWindowFunction, jintArray jPartitionChannels,jintArray JPreGroupedChannels,jintArray jSortChannels,jintArray jSortOrder,jintArray jSortNullFirsts,jint preSortedChannelPrefix,jint expectedPositions,jintArray jArgumentChannels,jintArray jWindowFunctionReturnType)
{
    jint *sourceTypes=env->GetIntArrayElements(jSourceTypes,JNI_FALSE);
    jint *outputChannels=env->GetIntArrayElements(jOutputChannels,JNI_FALSE);
    jint *windowFunction=env->GetIntArrayElements(jWindowFunction,JNI_FALSE);
    jint *partitionChannels=env->GetIntArrayElements(jPartitionChannels,JNI_FALSE);
    jint *preGroupedChannels=env->GetIntArrayElements(JPreGroupedChannels,JNI_FALSE);
    jint *sortChannels=env->GetIntArrayElements(jSortChannels,JNI_FALSE);
    jint *sortOrder=env->GetIntArrayElements(jSortOrder,JNI_FALSE);
    jint *sortNullFirsts=env->GetIntArrayElements(jSortNullFirsts,JNI_FALSE);
    jint *argumentChannels = env->GetIntArrayElements(jArgumentChannels, JNI_FALSE);
    jint *windowFunctionReturnType = env->GetIntArrayElements(jWindowFunctionReturnType, JNI_FALSE);

    jint sourceTypeCount = env->GetArrayLength(jSourceTypes);
    jint outputColsCount=env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount=env->GetArrayLength(jWindowFunction);
    jint partitionCount=env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount=env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount=env->GetArrayLength(jSortChannels);
    jint argumentChannelsCount = env->GetArrayLength(jArgumentChannels);
    jint windowFunctionReturnTypeCount = env->GetArrayLength(jWindowFunctionReturnType);

    int32_t allCount=sourceTypeCount+windowFunctionReturnTypeCount;
    int32_t *allTypes = new int32_t[allCount];
    for (int i = 0; i <sourceTypeCount ; ++i) {
        allTypes[i] = sourceTypes[i];
    }
    for (int i = sourceTypeCount; i < allCount; ++i) {
        allTypes[i] = windowFunctionReturnType[i - sourceTypeCount];
    }

    omniruntime::op::WindowOperatorFactory *windowOperatorFactory =new omniruntime::op::WindowOperatorFactory(
        sourceTypes,
        sourceTypeCount,
        outputChannels,
        outputColsCount,
        windowFunction,
        windowFunctionCount,
        partitionChannels,
        partitionCount,
        preGroupedChannels,
        preGroupedCount,
        sortChannels,
        sortOrder,
        sortNullFirsts,
        sortColCount,
        preSortedChannelPrefix,
        expectedPositions,
        allTypes,
        allCount,
        argumentChannels,
        argumentChannelsCount
        );
    JitContext *jitContext = createWindowJitContext(
        windowOperatorFactory->GetSourceTypes(),
        windowOperatorFactory->getTypesCount(),
        windowOperatorFactory->GetOutputCols(),
        windowOperatorFactory->GetOutputColsCount(),
        windowOperatorFactory->getPartitionCols(),
        windowOperatorFactory->getPartitionCount(),
        windowOperatorFactory->getSortCols(),
        windowOperatorFactory->getSortAscendings(),
        windowOperatorFactory->getSortNullFirsts(),
        windowOperatorFactory->getSortColCount(),
        windowOperatorFactory->getAllTypes(),
        windowOperatorFactory->getAllCount());
    windowOperatorFactory->SetJitContext(jitContext);
    return (int64_t) windowOperatorFactory;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jSourceTypes, jint jN, jintArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    using namespace omniruntime::jit;
    jint *sourceTypes=env->GetIntArrayElements(jSourceTypes,JNI_FALSE);
    jint *sortCols=env->GetIntArrayElements(jSortCols,JNI_FALSE);
    jint *sortAsc=env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts=env->GetIntArrayElements(jSortNullFirsts,JNI_FALSE);
    int32_t n = (int32_t) jN;

    jint sourceTypesCount = env->GetArrayLength(jSourceTypes);
    jint sortColCount = env->GetArrayLength(jSortCols);

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =new omniruntime::op::TopNOperatorFactory(
            sourceTypes,
            sourceTypesCount,
            n,
            sortCols,
            sortAsc,
            sortNullFirsts,
            sortColCount
    );

    ParamValue p_sourceTypes = ParamValue(sourceTypes, sourceTypesCount);
    ParamValue p_sortColCount= ParamValue(&sortColCount);

    auto * topNCompareSp=new Specialization();
    topNCompareSp->addSpecializedParam(4, &p_sortColCount);
    topNCompareSp->addSpecializedParam(5, &p_sourceTypes);

    std::map<string,Specialization> topNCompareSps={{OMNIJIT_TOPN_COMPARE, *topNCompareSp}};

    auto *topNContext=new omniruntime::jit::Context("topn",topNCompareSps,std::vector<std::string>(),std::vector<std::string>(),true);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*topNContext});

    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = createOperatorFunc;

    topNOperatorFactory->SetJitContext(jitContext);
    return (int64_t) topNOperatorFactory;
}

JitContext *createWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
                                   int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
                                   int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount)
{
    using namespace omniruntime::jit;
    int32_t finalSortColsCount = sortColsCount + partitionCount;
    int32_t finalSortCols[finalSortColsCount];
    int32_t finalSortAscendings[finalSortColsCount];
    int32_t finalSortNullFirsts[finalSortColsCount];
    for (int32_t i = 0; i < partitionCount; i++) {
        finalSortCols[i] = partitionCols[i];
        finalSortAscendings[i] = true;
        finalSortNullFirsts[i] = false;
    }
    for (int32_t i = partitionCount; i < partitionCount + sortColsCount; i++) {
        finalSortCols[i] = sortCols[i - partitionCount];
        finalSortAscendings[i] = sortAscendings[i - partitionCount];
        finalSortNullFirsts[i] = sortNullFirsts[i - partitionCount];
    }
    int32_t finalSortColTypes[finalSortColsCount];
    for (int32_t i = 0; i < finalSortColsCount; i++) {
        finalSortColTypes[i] = sourceTypes[finalSortCols[i]];
    }
    int32_t finalOutputCols[allCount];
    int32_t finalOutputColsCount = 0;
    for (int32_t i = 0; i < outputColsCount; i++) {
        finalOutputCols[finalOutputColsCount] = outputCols[i];
        finalOutputColsCount++;
    }
    for (int32_t i = typesCount; i < allCount; i++) {
        finalOutputCols[finalOutputColsCount] = i;
        finalOutputColsCount++;
    }
    ParamValue p_sortCols = ParamValue(finalSortCols, finalSortColsCount);
    ParamValue p_sortColTypes = ParamValue(finalSortColTypes, finalSortColsCount);
    ParamValue p_sortAscendings = ParamValue(finalSortAscendings, finalSortColsCount);
    ParamValue p_sortNullFirsts = ParamValue(finalSortNullFirsts, finalSortColsCount);
    ParamValue p_sortColCount = ParamValue(&finalSortColsCount);
    ParamValue p_sourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColsCount);
    ParamValue p_outputColCount = ParamValue(&outputColsCount);
    auto *compareToSp = new Specialization();
    compareToSp->addSpecializedParam(1, &p_sortCols);
    compareToSp->addSpecializedParam(2, &p_sortColTypes);
    compareToSp->addSpecializedParam(3, &p_sortAscendings);
    compareToSp->addSpecializedParam(4, &p_sortNullFirsts);
    compareToSp->addSpecializedParam(5, &p_sortColCount);
    auto *getOutputSp = new Specialization();
    getOutputSp->addSpecializedParam(1, &p_outputCols);
    getOutputSp->addSpecializedParam(2, &p_outputColCount);
    getOutputSp->addSpecializedParam(4, &p_sourceTypes);
    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
                                                            { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowContext = new omniruntime::jit::Context("window", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>(), true);
    auto *sortContext = new omniruntime::jit::Context("sort", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *aggContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *windowFunctionContext = new omniruntime::jit::Context("window_function",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *windowPartitionContext = new omniruntime::jit::Context("window_partition",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps, std::vector<std::string>(),
        std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *windowContext, *sortContext, *aggContext,
        *windowFunctionContext, *windowPartitionContext, *hashUtilContext, *pagesHashStrategyContext,
        *memoryPoolContext, *pagesIndexContext });
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    return jitContext;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jInputTypes, jint jInputLength, jstring jExpression, jintArray jProjectIndices, jint jProjectLength)
{
    std::string filterExpression = std::string(env->GetStringUTFChars(jExpression, JNI_FALSE));
    jint *inputTypes = env->GetIntArrayElements(jInputTypes, JNI_FALSE);
    int32_t inputLength = (int32_t) jInputLength;
    jint *projectIndices = env->GetIntArrayElements(jProjectIndices, JNI_FALSE);
    int32_t projectLength = (int32_t) jProjectLength;
    omniruntime::op::FilterAndProjectOperatorFactory *factory = new omniruntime::op::FilterAndProjectOperatorFactory(filterExpression, inputTypes, inputLength, projectIndices, projectLength);
    return (int64_t) factory;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(
    JNIEnv *env, jobject jobj, jintArray jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength)
{
    std::string* exprs = new std::string[jExprsLength];
    for (int32_t i = 0; i < jExprsLength; i++) {
        jstring st = (jstring) (env->GetObjectArrayElement(jExprs, i));
        const char *rawString = env->GetStringUTFChars(st, 0);
        exprs[i] = rawString;
    }
    int32_t exprLength = (int32_t) jExprsLength;
    jint* inputTypes = env->GetIntArrayElements(jInputTypes, JNI_FALSE);
    int32_t inputLength = (int32_t) jInputLength;
    omniruntime::op::ProjectionOperatorFactory* factory = new omniruntime::op::ProjectionOperatorFactory(
        exprs,
        exprLength,
        inputTypes,
        inputLength);
    return (int64_t) factory;

    // TODO: ReleaseStringUTFChars
}
JitContext *createHashBuilderJitContext(
    int32_t *buildTypes,
    int32_t buildTypesCount,
    int32_t *buildOutputCols,
    int32_t buildOutputColsCount,
    int32_t *buildHashCols,
    int32_t buildHashColsCount,
    int32_t operatorCount);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jBuildTypes, jintArray jBuildOutputCols, jintArray jBuildHashCols, jint jOperatorCount)
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
    JitContext *jitContext = createHashBuilderJitContext(
        buildTypesArr,
        buildTypesCount,
        buildOutputColsArr,
        buildOutputColsCount,
        buildHashColsArr,
        buildHashColsCount,
        jOperatorCount);
    hashBuilderOperatorFactory->SetJitContext(jitContext);
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)hashBuilderOperatorFactory;
}

JitContext *createHashBuilderJitContext(
    int32_t *buildTypes,
    int32_t buildTypesCount,
    int32_t *buildOutputCols,
    int32_t buildOutputColsCount,
    int32_t *buildHashCols,
    int32_t buildHashColsCount,
    int32_t operatorCount)
{
    JNI_DEBUG_LOG("create hash builder jit context starting.");
    auto start = START();

    if (buildHashColsCount <= 0) {
        std::cerr << "Memory allocation size is illegal!" << std::endl;
        return nullptr;
    }
    int32_t *hashColTypes = new int32_t[buildHashColsCount];
    for (int32_t i = 0; i < buildHashColsCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    using namespace omniruntime::jit;
    ParamValue p_hashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue p_hashColCount = ParamValue(&buildHashColsCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->addSpecializedParam(3, &p_hashColTypes);
    hashPositionSp->addSpecializedParam(4, &p_hashColCount);

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(5, &p_hashColTypes);
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(6, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        {OMNIJIT_HASH_STRATEGY_HASH_POSITION, *hashPositionSp},
        {OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp}
    };

    auto *hashBuilderContext = new omniruntime::jit::Context("hash_builder", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>(), true);
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps, std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*hashBuilderContext, *pagesIndexContext, *joinHashTableContext, *pagesHashStrategyContext, *hashUtilContext});
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create hash builder jit context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *createLookupJoinJitContext(
    int32_t *probeTypes,
    int32_t probeTypesCount,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *probeHashCols,
    int32_t probeHashColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    int64_t hashBuilderFactoryAddr);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(
    JNIEnv *env, jobject jObj, jintArray jProbeTypes, jintArray jProbeOutputCols, jintArray jProbeHashCols,
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
    JitContext *jitContext = createLookupJoinJitContext(
        probeTypesArr, probeTypesCount, probeOutputColsArr,
        probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr, buildOutputTypesArr,
        buildOutputColsCount, jHashBuilderOperatorFactory);
    lookupJoinOperatorFactory->SetJitContext(jitContext);
    JNI_DEBUG_LOG("create lookup join operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)lookupJoinOperatorFactory;
}

JitContext *createLookupJoinJitContext(int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t *buildOutputTypes, int32_t buildOutputColsCount, int64_t hashBuilderFactoryAddr)
{
    JNI_DEBUG_LOG("create lookup join jit context starting.");
    auto start = START();
    using namespace omniruntime::jit;

    ParamValue p_probeTypes = ParamValue(probeTypes, probeTypesCount);
    ParamValue p_probeOutputCols = ParamValue(probeOutputCols, probeOutputColsCount);
    ParamValue p_probeOutputColsCount = ParamValue(&probeOutputColsCount);

    auto *buildProbeColumnsSp = new Specialization();
    buildProbeColumnsSp->addSpecializedParam(2, &p_probeTypes);
    buildProbeColumnsSp->addSpecializedParam(3, &p_probeOutputCols);
    buildProbeColumnsSp->addSpecializedParam(4, &p_probeOutputColsCount);

    ParamValue p_buildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue p_buildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue p_buildOutputColsCount = ParamValue(&buildOutputColsCount);

    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->addSpecializedParam(2, &p_buildOutputTypes);
    buildBuildColumnsSp->addSpecializedParam(3, &p_buildOutputCols);
    buildBuildColumnsSp->addSpecializedParam(4, &p_buildOutputColsCount);
    buildBuildColumnsSp->addSpecializedParam(5, &p_probeOutputColsCount);

    std::map<std::string, Specialization> lookupJoinSps = {
            {OMNIJIT_CONSTRUCT_PROBE_COLUMNS_FROM_COPY, *buildProbeColumnsSp},
            {OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp}
    };

    if (probeHashColsCount <= 0) {
        std::cerr << "Memory allocation size is illegal!" << std::endl;
        return nullptr;
    }
    int32_t *hashColTypes = new int32_t[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }
    ParamValue p_hashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue p_hashColCount = ParamValue(&probeHashColsCount);

    auto *hashRowSp = new Specialization();
    hashRowSp->addSpecializedParam(2, &p_hashColTypes);
    hashRowSp->addSpecializedParam(3, &p_hashColCount);
    std::map<std::string, Specialization> joinHashTableSps = {
            {OMNIJIT_HASH_ROW, *hashRowSp}
    };

    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(5, &p_hashColTypes);
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(6, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        {OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS, *positionEqualsRowIgnoreNullsSp}
    };

    auto *lookupJoinContext = new omniruntime::jit::Context("lookup_join", lookupJoinSps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table", joinHashTableSps, std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps, std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*lookupJoinContext, *joinHashTableContext, *pagesIndexContext, *pagesHashStrategyContext, *hashUtilContext, *memoryPoolContext});
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create lookup join jit context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}
