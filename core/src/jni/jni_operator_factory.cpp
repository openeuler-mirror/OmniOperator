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
#include "../operator/sort/sort_expr.h"
#include "../operator/aggregation/group_aggregation.h"
#include "../operator/aggregation/group_aggregation_expr.h"
#include "../operator/aggregation/non_group_aggregation.h"
#include "../operator/filter/filter_and_project.h"
#include "../operator/window/window.h"
#include "../operator/join/hash_builder.h"
#include "../operator/join/lookup_join.h"
#include "../operator/join/hash_builder_expr.h"
#include "../operator/join/lookup_join_expr.h"
#include "../operator/topn/topn.h"
#include "../operator/topn/topn_expr.h"
#include "../operator/partitionedoutput/partitionedoutput.h"
#include "../operator/union/union.h"
#include "../operator/optimization.h"
#include "../operator/window/window_expr.h"
#include "config.h"

using omniruntime::jit::Context;
using omniruntime::jit::Jit;
using omniruntime::jit::ModuleOptimization;
using omniruntime::jit::Optimization;
using omniruntime::jit::ParamValue;
using omniruntime::jit::Specialization;
using omniruntime::vec::Deserialize;
using omniruntime::vec::VecType;

using namespace omniruntime::op;
using namespace std;

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperatorNative
 * Signature: (J)JJ
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperatorNative(JNIEnv *env,
    jobject jObj, jlong jNativeFactoryObj, jlong jNativeVecAllocatorObj)
{
    JNI_DEBUG_LOG("create omni operator starting.");
    auto start = START();
    VectorAllocator *vectorAllocator = (VectorAllocator *)jNativeVecAllocatorObj;
    OperatorFactory *operatorFactory = (OperatorFactory *)jNativeFactoryObj;
    JitContext *jitContext = operatorFactory->GetJitContext();
    omniruntime::op::Operator *nativeOperator = nullptr;

#if defined(DEBUG_OPERATOR) || defined(DISABLE_JIT)
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
    nativeOperator->SetVecAllocator(vectorAllocator);
    return reinterpret_cast<int64_t>(nativeOperator);
}

void GetColumnsFromExpressions(JNIEnv *env, jobjectArray &jExpressions, int32_t *columns, int32_t length)
{
    for (int32_t i = 0; i < length; i++) {
        jstring jSortCol = static_cast<jstring>(env->GetObjectArrayElement(jExpressions, i));
        const char *columnString = env->GetStringUTFChars(jSortCol, JNI_FALSE);
        columns[i] = std::stoi(columnString + 1);
        env->ReleaseStringUTFChars(jSortCol, columnString);
    }
}


/* *
 * Return an HashAggregationFactory object address.
 *                                                */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationJitContext(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and id
    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    int32_t groupByCols[groupByNum];
    GetColumnsFromExpressions(env, jGroupByChannel, groupByCols, groupByNum);
    auto groupByTypesCharPtr = env->GetStringUTFChars(jGroupByType, JNI_FALSE);
    size_t aggNum = static_cast<size_t>(env->GetArrayLength(jAggChannel));
    int32_t aggCols[aggNum];
    GetColumnsFromExpressions(env, jAggChannel, aggCols, aggNum);
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);

    auto groupByVecTypes = Deserialize(groupByTypesCharPtr);
    auto aggVecTypes = Deserialize(aggTypesCharPtr);
    auto outVecTypes = Deserialize(outTypesCharPtr);

    auto groupByTypeIds = groupByVecTypes.GetIds();
    auto aggTypeIds = aggVecTypes.GetIds();

    PrepareContext groupByColContext = { (uint32_t *)groupByCols, groupByNum };
    PrepareContext groupByTypeContext = { (uint32_t *)groupByTypeIds, groupByNum };
    PrepareContext aggColContext = { (uint32_t *)aggCols, aggNum };
    PrepareContext aggTypeContext = { (uint32_t *)aggTypeIds, aggNum };
    PrepareContext aggFuncTypeContext = { (uint32_t *)aggFuncTypes, aggNum };

    using namespace omniruntime::jit;
    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t *colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    ParamValue pColType = ParamValue(colTypes, colNum);
    ParamValue pColCount = ParamValue(&colNum);
    ParamValue pGroupByColIdx = ParamValue((int32_t *)groupByColContext.context, groupColNum);
    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggColIdx = ParamValue((int32_t *)aggColContext.context, aggColNum);
    ParamValue pAggNum = ParamValue(&aggColNum);
    ParamValue pAggDataType = ParamValue((int32_t *)aggTypeContext.context, aggColNum);
    ParamValue pAggTypes = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &pColType);
    inloopSp->AddSpecializedParam(4, &pColCount);
    inloopSp->AddSpecializedParam(6, &pGroupNum);
    inloopSp->AddSpecializedParam(8, &pAggNum);
    inloopSp->AddSpecializedParam(9, &pAggTypes);

    auto *processAggSp = new Specialization();
    processAggSp->AddSpecializedParam(2, &pAggNum);
    processAggSp->AddSpecializedParam(3, &pColType);

    auto *hashColumnSp = new Specialization();
    hashColumnSp->AddSpecializedParam(2, &pColType);
    hashColumnSp->AddSpecializedParam(3, &pGroupNum);

    auto *aggColumnSp = new Specialization();
    aggColumnSp->AddSpecializedParam(2, &pColType);
    aggColumnSp->AddSpecializedParam(3, &pAggNum);

    std::map<std::string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp },
        // TODO: open this optimization
        //        {OMNIJIT_HASH_GROUPBY_HASH_COLUMN, *hashColumnSp},
        //        {OMNIJIT_HASH_GROUPBY_AGG_COLUMN, *aggColumnSp},
        { OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp } };

    auto *groupAggregationContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *groupAggregationContext });
    jit->Specialize(std::vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        std::vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    delete inloopSp;
    delete processAggSp;
    delete groupAggregationContext;
    delete jit;

    return reinterpret_cast<uint64_t>(jitContext);
}

/* *
 * Return an HashAggregationFactory object address.
 *                                                */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial,
    jlong jitContext)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and id
    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    int32_t groupByCols[groupByNum];
    GetColumnsFromExpressions(env, jGroupByChannel, groupByCols, groupByNum);
    auto groupByTypesCharPtr = env->GetStringUTFChars(jGroupByType, JNI_FALSE);
    size_t aggNum = static_cast<size_t>(env->GetArrayLength(jAggChannel));
    int32_t aggCols[aggNum];
    GetColumnsFromExpressions(env, jAggChannel, aggCols, aggNum);
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);

    auto groupByVecTypes = Deserialize(groupByTypesCharPtr);
    auto aggVecTypes = Deserialize(aggTypesCharPtr);
    auto outVecTypes = Deserialize(outTypesCharPtr);

    auto groupByTypeIds = groupByVecTypes.GetIds();
    auto aggTypeIds = aggVecTypes.GetIds();

    PrepareContext groupByColContext = { (uint32_t *)groupByCols, groupByNum };
    PrepareContext groupByTypeContext = { (uint32_t *)groupByTypeIds, groupByNum };
    PrepareContext aggColContext = { (uint32_t *)aggCols, aggNum };
    PrepareContext aggTypeContext = { (uint32_t *)aggTypeIds, aggNum };
    PrepareContext aggFuncTypeContext = { (uint32_t *)aggFuncTypes, aggNum };

    using namespace omniruntime::jit;
    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t *colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByVecTypes, aggColContext,
        aggVecTypes, outVecTypes, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    nativeOperatorFactory->Init();
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

/* *
 * Return an AggregationFactory object address.
 *                                                */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationJitContext(JNIEnv *env,
    jobject jObj, jstring jAggType, jintArray jAggFuncType, jstring jAggOutputTypes, jboolean inputRaw,
    jboolean outputPartial)
{
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    auto aggVecTypes = Deserialize(aggTypesCharPtr);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);

    auto aggTypeIds = aggVecTypes.GetIds();
    auto aggNum = static_cast<size_t>(aggVecTypes.GetSize());

    PrepareContext aggTypeContext = { (uint32_t *)aggTypeIds, aggNum };
    PrepareContext aggFuncTypeContext = { (uint32_t *)aggFuncTypes, aggNum };
    int32_t aggColNum = aggTypeContext.len;

    using namespace omniruntime::jit;
    std::map<std::string, ParamValue *> testParam;

    ParamValue pColType = ParamValue((int32_t *)aggTypeContext.context, aggColNum);
    ParamValue pAggNum = ParamValue(&aggColNum);
    ParamValue pAggTypes = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &pAggNum);
    inloopSp->AddSpecializedParam(4, &pColType);
    inloopSp->AddSpecializedParam(5, &pAggTypes);

    std::map<std::string, Specialization> nonGroupSps = { { OMNIJIT_NON_GROUP_INLOOP, *inloopSp } };

    auto *groupAggregationContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("non_group_aggregation"), nonGroupSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *groupAggregationContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    return reinterpret_cast<uint64_t>(jitContext);
}

/* *
 * Return an AggregationFactory object address.
 *                                                */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jAggType, jintArray jAggFuncType, jstring jAggOutputTypes, jboolean inputRaw,
    jboolean outputPartial, jlong jitContext)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    auto aggVecTypes = Deserialize(aggTypesCharPtr);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);

    auto aggTypeIds = aggVecTypes.GetIds();
    auto aggNum = static_cast<size_t>(aggVecTypes.GetSize());

    PrepareContext aggTypeContext = { (uint32_t *)aggTypeIds, aggNum };
    PrepareContext aggFuncTypeContext = { (uint32_t *)aggFuncTypes, aggNum };
    int32_t aggColNum = aggTypeContext.len;

    // TODO get agg output types from java
    omniruntime::op::AggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::AggregationOperatorFactory(aggVecTypes, aggVecTypes, aggFuncTypeContext, inputRaw,
        outputPartial);
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    nativeOperatorFactory->Init();
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JitContext *CreateSortJitContext(const int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortJitContext
 * Method:    createSortJitContext
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortJitContext(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortCols, jintArray jAscendings, jintArray jNullFirsts)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    auto sortColsCount = env->GetArrayLength(jSortCols);
    int32_t sortColsArr[sortColsCount];
    GetColumnsFromExpressions(env, jSortCols, sortColsArr, sortColsCount);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputColsCount = env->GetArrayLength(jOutputCols);

    JitContext *jitContext =
        CreateSortJitContext(const_cast<int32_t *>(sourceVecTypes.GetIds()), sourceVecTypes.GetSize(), outputColsArr,
        outputColsCount, sortColsArr, ascendingsArr, nullFirstsArr, sortColsCount);

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)jitContext;
}


/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortCols,
    jintArray jAscendings, jintArray jNullFirsts, jlong jitContext)
{
    JNI_DEBUG_LOG("create sort operator factory starting.");
    auto start = START();
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    auto sortColsCount = env->GetArrayLength(jSortCols);
    int32_t sortColsArr[sortColsCount];
    GetColumnsFromExpressions(env, jSortCols, sortColsArr, sortColsCount);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputColsCount = env->GetArrayLength(jOutputCols);

    JNI_DEBUG_LOG("before create sort operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::SortOperatorFactory *sortOperatorFactory =
        omniruntime::op::SortOperatorFactory::CreateSortOperatorFactory(sourceVecTypes, outputColsArr, outputColsCount,
        sortColsArr, ascendingsArr, nullFirstsArr, sortColsCount);

    sortOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)sortOperatorFactory;
}

JitContext *CreateSortJitContext(const int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
    JNI_DEBUG_LOG("create sort JIT context starting.");
    auto start = START();
    using namespace omniruntime::jit;

    int sortColTypes[sortColsCount];
    for (int32_t i = 0; i < sortColsCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue pSourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue pTypeCount = ParamValue(&typesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColsCount);
    ParamValue pSortColTypes = ParamValue(sortColTypes, sortColsCount);
    ParamValue pSortAscendings = ParamValue(sortAscendings, sortColsCount);
    ParamValue pSortNullFirsts = ParamValue(sortNullFirsts, sortColsCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    auto *compareToSp = new Specialization();
    compareToSp->AddSpecializedParam(0, &pSortCols);
    compareToSp->AddSpecializedParam(1, &pSortColTypes);
    compareToSp->AddSpecializedParam(2, &pSortAscendings);
    compareToSp->AddSpecializedParam(3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(4, &pSortColCount);

    auto *getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &pOutputCols);
    getOutputSp->AddSpecializedParam(2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(4, &pSourceTypes);

    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };

    auto *sortContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("sort"), std::map<std::string, Specialization>());
    auto *pagesIndexContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *sortContext, *pagesIndexContext });
    jit->Specialize(std::vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        std::vector<ModuleOptimization> { ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH,
        ModuleOptimization::CONSTANT_MERGE });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete sortContext;
    delete pagesIndexContext;
    delete jit;

    JNI_DEBUG_LOG("create sort JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *createWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount);


JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowJitContext(JNIEnv *env, jobject jObj,
    jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction, jintArray jPartitionChannels,
    jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder, jintArray jSortNullFirsts,
    jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels, jstring jWindowFunctionReturnType)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *windowFunction = env->GetIntArrayElements(jWindowFunction, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *preGroupedChannels = env->GetIntArrayElements(JPreGroupedChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    jint *argumentChannels = env->GetIntArrayElements(jArgumentChannels, JNI_FALSE);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputVecTypes = Deserialize(windowFunctionReturnTypeCharPtr);

    jint inputTypesCount = inputVecTypes.GetSize();
    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentChannelsCount = env->GetArrayLength(jArgumentChannels);
    jint outputTypesCount = outputVecTypes.GetSize();

    std::vector<VecType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), inputVecTypes.Get().begin(), inputVecTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputVecTypes.Get().begin(), outputVecTypes.Get().end());

    VecTypes allTypes(allTypesVec);

    JitContext *jitContext = createWindowJitContext(const_cast<int32_t *>(inputVecTypes.GetIds()),
        inputVecTypes.GetSize(), outputChannels, outputColsCount, partitionChannels, partitionCount, sortChannels,
        sortOrder, sortNullFirsts, sortColCount, const_cast<int32_t *>(allTypes.GetIds()), allTypes.GetSize());

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels,
    jstring jWindowFunctionReturnType, jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *windowFunction = env->GetIntArrayElements(jWindowFunction, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *preGroupedChannels = env->GetIntArrayElements(JPreGroupedChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    jint *argumentChannels = env->GetIntArrayElements(jArgumentChannels, JNI_FALSE);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputVecTypes = Deserialize(windowFunctionReturnTypeCharPtr);

    jint inputTypesCount = inputVecTypes.GetSize();
    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentChannelsCount = env->GetArrayLength(jArgumentChannels);
    jint outputTypesCount = outputVecTypes.GetSize();

    std::vector<VecType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), inputVecTypes.Get().begin(), inputVecTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputVecTypes.Get().begin(), outputVecTypes.Get().end());

    VecTypes allTypes(allTypesVec);

    omniruntime::op::WindowOperatorFactory *windowOperatorFactory =
        omniruntime::op::WindowOperatorFactory::CreateWindowOperatorFactory(inputVecTypes, outputChannels,
        outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount, preGroupedChannels,
        preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, allTypes, argumentChannels, argumentChannelsCount);

    windowOperatorFactory->Init();
    windowOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)windowOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNJitContext(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    using namespace omniruntime::jit;
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortColCount = env->GetArrayLength(jSortCols);
    int32_t *sortCols = std::make_unique<int32_t[]>(sortColCount).release();
    GetColumnsFromExpressions(env, jSortCols, sortCols, sortColCount);
    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    auto sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    auto sourceTypesCount = sourceTypes.GetSize();

    ParamValue pSourceTypes = ParamValue(sourceTypeIds, sourceTypesCount);
    ParamValue pSortColCount = ParamValue(&sortColCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColCount);

    auto *topNCompareSp = new Specialization();
    topNCompareSp->AddSpecializedParam(4, &pSortColCount); // 4teh parameter
    topNCompareSp->AddSpecializedParam(5, &pSortCols);     // 5teh parameter
    topNCompareSp->AddSpecializedParam(6, &pSourceTypes);  // 6teh parameter

    std::map<std::string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, *topNCompareSp } };

    auto *topNContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *topNContext });

    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = createOperatorFunc;

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    delete topNCompareSp;
    delete jit;
    delete topNContext;
    return (int64_t)jitContext;
}


JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc,
    jintArray jSortNullFirsts, jlong jitContext)
{
    using namespace omniruntime::jit;
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortColCount = env->GetArrayLength(jSortCols);
    int32_t *sortCols = std::make_unique<int32_t[]>(sortColCount).release();
    GetColumnsFromExpressions(env, jSortCols, sortCols, sortColCount);
    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new omniruntime::op::TopNOperatorFactory(sourceTypes, n, sortCols, sortAsc, sortNullFirsts, sortColCount);

    topNOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    return (int64_t)topNOperatorFactory;
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

    ParamValue pSortCols = ParamValue(finalSortCols, finalSortColsCount);
    ParamValue pSortColTypes = ParamValue(finalSortColTypes, finalSortColsCount);
    ParamValue pSortAscendings = ParamValue(finalSortAscendings, finalSortColsCount);
    ParamValue pSortNullFirsts = ParamValue(finalSortNullFirsts, finalSortColsCount);
    ParamValue pSortColCount = ParamValue(&finalSortColsCount);

    ParamValue pSourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);

    auto *compareToSp = new Specialization();
    compareToSp->AddSpecializedParam(0, &pSortCols);
    compareToSp->AddSpecializedParam(1, &pSortColTypes);
    compareToSp->AddSpecializedParam(2, &pSortAscendings);
    compareToSp->AddSpecializedParam(3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(4, &pSortColCount);
    auto *getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &pOutputCols);
    getOutputSp->AddSpecializedParam(2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(4, &pSourceTypes);
    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("window"), std::map<std::string, Specialization>());
    auto *pagesIndexContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *windowContext, *pagesIndexContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete windowContext;
    delete pagesIndexContext;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jInputTypes, jint jInputLength, jstring jExpression, jobjectArray jProjections,
    jint jProjectLength, jlong jitContext)
{
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputVecTypes = Deserialize(inputTypesCharPtr);
    auto inputTypeIds = const_cast<int32_t *>(inputVecTypes.GetIds());
    auto inputLength = (int32_t)jInputLength;

    auto *projectExpressions = new std::string[jProjectLength];
    for (int32_t i = 0; i < jProjectLength; i++) {
        auto st = (jstring)(env->GetObjectArrayElement(jProjections, i));
        auto exprStringPtr = env->GetStringUTFChars(st, JNI_FALSE);
        projectExpressions[i] = exprStringPtr;
        env->ReleaseStringUTFChars(st, exprStringPtr);
    }
    auto projectLength = (int32_t)jProjectLength;
    auto *factory = new omniruntime::op::FilterAndProjectOperatorFactory(filterExpression, inputTypeIds, inputLength,
        projectExpressions, projectLength);
    if (!factory->isSupportedExpr) {
        delete factory;
        return 0;
    }
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    env->ReleaseStringUTFChars(jExpression, expressionCharPtr);
    return (int64_t)factory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *env,
    jobject jobj, jstring jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength, jlong jitContext)
{
    std::string *exprs = new std::string[jExprsLength];
    for (int32_t i = 0; i < jExprsLength; i++) {
        jstring st = (jstring)(env->GetObjectArrayElement(jExprs, i));
        auto rawString = env->GetStringUTFChars(st, 0);
        exprs[i] = rawString;
    }
    int32_t exprLength = (int32_t)jExprsLength;
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputVecTypes = Deserialize(inputTypesCharPtr);
    auto inputTypeIds = const_cast<int32_t *>(inputVecTypes.GetIds());
    int32_t inputLength = (int32_t)jInputLength;
    omniruntime::op::ProjectionOperatorFactory *factory =
        new omniruntime::op::ProjectionOperatorFactory(exprs, exprLength, inputTypeIds, inputLength);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);

    if (!factory->IsSupported()) {
        delete factory;
        return 0;
    }

    return (int64_t)factory;
}

JitContext *CreateHashBuilderJitContext(const int32_t *buildTypes, int32_t buildTypesCount, int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderJitContext(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashCols, jint jOperatorCount)
{
    auto buildTypesCharPtr = (env)->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    int32_t buildHashColsArr[buildHashColsCount];
    GetColumnsFromExpressions(env, jBuildHashCols, buildHashColsArr, buildHashColsCount);

    auto buildVecTypes = Deserialize(buildTypesCharPtr);
    JitContext *jitContext = CreateHashBuilderJitContext(buildVecTypes.GetIds(), buildVecTypes.GetSize(),
        buildHashColsArr, buildHashColsCount, jOperatorCount);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    return (int64_t)(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashCols, jint jOperatorCount, jlong jitContext)
{
    JNI_DEBUG_LOG("create hash builder operator factory starting.");
    auto start = START();
    auto buildTypesCharPtr = (env)->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    int32_t buildHashColsArr[buildHashColsCount];
    GetColumnsFromExpressions(env, jBuildHashCols, buildHashColsArr, buildHashColsCount);

    auto buildVecTypes = Deserialize(buildTypesCharPtr);

    JNI_DEBUG_LOG("before create hash builder operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::HashBuilderOperatorFactory *hashBuilderOperatorFactory =
        omniruntime::op::HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildVecTypes, buildHashColsArr,
        buildHashColsCount, jOperatorCount);

    hashBuilderOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    return (int64_t)hashBuilderOperatorFactory;
}

JitContext *CreateHashBuilderJitContext(const int32_t *buildTypes, int32_t buildTypesCount, int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount)
{
    JNI_DEBUG_LOG("create hash builder JIT context starting.");
    auto start = START();

    if (buildHashColsCount <= 0) {
        std::cerr << "Memory allocation size is illegal!" << std::endl;
        return nullptr;
    }
    int32_t hashColTypes[buildHashColsCount];
    for (int32_t i = 0; i < buildHashColsCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    using namespace omniruntime::jit;
    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue pHashColCount = ParamValue(&buildHashColsCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->AddSpecializedParam(3, &pHashColTypes);
    hashPositionSp->AddSpecializedParam(4, &pHashColCount);
    std::map<std::string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION,
        *hashPositionSp } };

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp }
    };

    auto *hashBuilderContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("hash_builder"),
        std::map<std::string, Specialization>());
    auto *joinHashTableContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto *pagesHashStrategyContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *hashBuilderContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete hashPositionSp;
    delete positionEqualsPositionIgnoreNullsSp;
    delete hashBuilderContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    JNI_DEBUG_LOG("create hash builder JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *CreateLookupJoinJitContext(const int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinJitContext(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashCols,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType)
{
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    jint *probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    int32_t probeHashColsArr[probeHashColsCount];
    GetColumnsFromExpressions(env, jProbeHashCols, probeHashColsArr, probeHashColsCount);
    jint *buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesCharPtr);
    JitContext *jitContext = CreateLookupJoinJitContext(probeVecTypes.GetIds(), probeVecTypes.GetSize(),
        probeOutputColsArr, probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr,
        buildOutputVecTypes.GetIds(), buildOutputVecTypes.GetSize());

    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashCols,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory,
    jlong jitContext)
{
    JNI_DEBUG_LOG("create lookup join operator factory starting.");
    auto start = START();
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    jint *probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    int32_t probeHashColsArr[probeHashColsCount];
    GetColumnsFromExpressions(env, jProbeHashCols, probeHashColsArr, probeHashColsCount);
    jint *buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesCharPtr);

    JNI_DEBUG_LOG("before create lookup join operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::LookupJoinOperatorFactory *lookupJoinOperatorFactory =
        omniruntime::op::LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(probeVecTypes, probeOutputColsArr,
        probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr, buildOutputVecTypes,
        (JoinType)jJoinType, jHashBuilderOperatorFactory);

    lookupJoinOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create lookup join operator factory finished, elapsed time: %ld ms.", END(start));
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);
    return (int64_t)lookupJoinOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_partitionedoutput_OmniPartitionedOutPutOperatorFactory_createPartitionedOutputOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jReplicatesAnyRow, jint jNullChannel,
    jintArray jPartitionChannels, jint jPartitionCount, jintArray jBucketToPartition, jboolean isHashPrecomputed,
    jstring jHashChannelTypes, jintArray jHashChannels, jlong jitContext)
{
    JNI_DEBUG_LOG("create partitionedoutput operator factory starting.");
    auto start = START();
    auto sourceTypesArrCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *partitionChannelsArr = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *bucketToPartitionArr = env->GetIntArrayElements(jBucketToPartition, JNI_FALSE);
    auto bucketToPartitionArrPtr = env->GetStringUTFChars(jHashChannelTypes, JNI_FALSE);
    jint *hashChannels = env->GetIntArrayElements(jHashChannels, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesArrCharPtr);
    auto hashChannelVecTypes = Deserialize(bucketToPartitionArrPtr);
    jint sourceTypesCount = sourceVecTypes.GetSize();
    jint partitionChannelsCount = env->GetArrayLength(jPartitionChannels);
    jint bucketToPartitionCount = env->GetArrayLength(jBucketToPartition);
    jint hashChannelTypesCount = hashChannelVecTypes.GetSize();
    jint hashChannelCount = env->GetArrayLength(jHashChannels);

    auto hashChannelTypesArr = const_cast<int32_t *>(hashChannelVecTypes.GetIds());
    JNI_DEBUG_LOG("before create partitionedoutput operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        omniruntime::op::PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceVecTypes,
        sourceTypesCount, jReplicatesAnyRow, jNullChannel, partitionChannelsArr, partitionChannelsCount,
        jPartitionCount, bucketToPartitionArr, bucketToPartitionCount, isHashPrecomputed, hashChannelTypesArr,
        hashChannelTypesCount, hashChannels, hashChannelCount);
    JNI_DEBUG_LOG("create partitionedoutput operator factory finished, elapsed time: %ld ms.", END(start));
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesArrCharPtr);
    env->ReleaseStringUTFChars(jHashChannelTypes, bucketToPartitionArrPtr);
    return (int64_t)partitionedOutputOperatorFactory;
}

void InitializeHashColTypes(const int32_t *probeTypes, int32_t *hashColTypes, int32_t *probeHashCols,
    int32_t probeHashColsCount)
{
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }
}

JitContext *CreateLookupJoinJitContext(const int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount)
{
    if (probeHashColsCount <= 0) {
        std::cerr << "Memory allocation size is illegal!" << std::endl;
        return nullptr;
    }
    int32_t hashColTypes[probeHashColsCount];
    InitializeHashColTypes(probeTypes, hashColTypes, probeHashCols, probeHashColsCount);

    JNI_DEBUG_LOG("create lookup join JIT context starting.");
    auto start = START();
    using namespace omniruntime::jit;
    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue pHashColCount = ParamValue(&probeHashColsCount);

    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->AddSpecializedParam(3, &pBuildOutputTypes);
    buildBuildColumnsSp->AddSpecializedParam(4, &pBuildOutputCols);
    buildBuildColumnsSp->AddSpecializedParam(5, &pBuildOutputColsCount);
    buildBuildColumnsSp->AddSpecializedParam(6, &pProbeOutputColsCount);

    auto *populateHashesSp = new Specialization();
    populateHashesSp->AddSpecializedParam(2, &pHashColTypes);
    populateHashesSp->AddSpecializedParam(3, &pHashColCount);
    std::map<std::string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp },
        { OMNIJIT_HASH_LOOKUP_JOIN_POPULATE_HASHES, *populateHashesSp } };

    auto *hashRowSp = new Specialization();
    hashRowSp->AddSpecializedParam(2, &pHashColTypes);
    hashRowSp->AddSpecializedParam(3, &pHashColCount);
    std::map<std::string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };

    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);
    std::map<std::string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };

    auto lookupJoinContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    auto joinHashTableContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *lookupJoinContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete buildBuildColumnsSp;
    delete populateHashesSp;
    delete hashRowSp;
    delete positionEqualsRowIgnoreNullsSp;
    delete lookupJoinContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    JNI_DEBUG_LOG("create lookup join JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jDistinct, jlong jitContext)
{
    JNI_DEBUG_LOG("create union operator factory starting.");
    auto start = START();
    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto sourcesTypes = Deserialize(sourceTypesCharPtr);
    int32_t sourceTypesCount = sourcesTypes.GetSize();
    JNI_DEBUG_LOG("before create union operator factory elapsed time: %ld ms.", END(start));
    auto *unionOperatorFactory = new omniruntime::op::UnionOperatorFactory(sourcesTypes, sourceTypesCount, jDistinct);
    JNI_DEBUG_LOG("create union operator factory finished, elapsed time: %ld ms.", END(start));
    unionOperatorFactory->SetJitContext(nullptr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    return (int64_t)unionOperatorFactory;
}

void GetExpressions(JNIEnv *env, jobjectArray jExpressions, std::string *expressions, int32_t expressionCount)
{
    for (int32_t i = 0; i < expressionCount; i++) {
        auto jExpression = static_cast<jstring>(env->GetObjectArrayElement(jExpressions, i));
        auto key = env->GetStringUTFChars(jExpression, JNI_FALSE);
        expressions[i] = key;
        env->ReleaseStringUTFChars(jExpression, key);
    }
}

int32_t GetProjectCol(std::string &expression)
{
    // #0 or #5 is not expression
    if (expression.data()[0] == '#') {
        return std::stoi(std::string(expression.data() + 1));
    } else {
        return -1;
    }
}

int32_t GetExprReturnType(std::string &expression)
{
    const char *chars = expression.data();
    int32_t length = expression.size();
    int32_t start = -1;
    int32_t end;
    for (int32_t i = 0; i < length; i++) {
        if (start == -1 && chars[i] == ':') {
            start = i;
        }
        if (start != -1 && chars[i] == '(') {
            end = i;
            break;
        }
    }

    std::string returnType(chars + start + 1, chars + end);
    if (returnType.find_first_not_of("0123456789") == string::npos && stoi(returnType) < INT32_MAX) {
        return static_cast<VecTypeId>(stoi(returnType));
    }
    std::cout << "Unsupported return type: " + returnType << std::endl;
    return OMNI_VEC_TYPE_INVALID;
}

void GetTypeIds(VecTypes &inputTypes, std::string *projectKeys, int32_t projectKeysCount, std::vector<int32_t> &typeIds,
    int32_t *projectCols)
{
    auto inputTypeIds = const_cast<int32_t *>(inputTypes.GetIds());
    auto inputTypesCount = inputTypes.GetSize();
    typeIds.insert(typeIds.end(), inputTypeIds, inputTypeIds + inputTypesCount);

    int32_t newProjectCol = inputTypesCount;
    for (int32_t i = 0; i < projectKeysCount; i++) {
        int32_t projectCol = GetProjectCol(projectKeys[i]);
        projectCols[i] = projectCol;
        if (projectCol == -1) {
            int32_t returnType = GetExprReturnType(projectKeys[i]);
            typeIds.push_back(returnType);
            projectCols[i] = newProjectCol++;
        }
    }
}

void GetRequiredTypeIds(VecTypes &inputTypes, std::string *projectKeys, int32_t projectKeysCount,
    std::vector<int32_t> &typeIds, int32_t *projectCols)
{
    auto inputTypeIds = const_cast<int32_t *>(inputTypes.GetIds());

    int32_t newProjectCol = 0;
    std::map<int32_t, int32_t> colIdMap;
    for (int32_t i = 0; i < projectKeysCount; i++) {
        int32_t projectCol = GetProjectCol(projectKeys[i]);
        if (projectCol == -1) {
            // expression col
            int32_t returnType = GetExprReturnType(projectKeys[i]);
            typeIds.push_back(returnType);
            projectCols[i] = newProjectCol++;
        } else {
            typeIds.push_back(inputTypeIds[projectCol]);
            if (colIdMap.find(projectCol) != colIdMap.end()) {
                // already exists
                projectCols[i] = colIdMap[projectCol];
            } else {
                projectCols[i] = newProjectCol++;
                colIdMap[projectCol] = projectCols[i];
            }
        }
    }
}

JitContext *CreateSortWithExprJitContext(VecTypes &sourceVecTypes, int32_t *outputCols, int32_t outputColsCount,
    string *sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortKeysCount)
{
    vector<int32_t> newSourceTypes;
    int32_t sortCols[sortKeysCount];
    GetTypeIds(sourceVecTypes, sortKeys, sortKeysCount, newSourceTypes, sortCols);

    int sortColTypes[sortKeysCount];
    for (int32_t i = 0; i < sortKeysCount; ++i) {
        sortColTypes[i] = newSourceTypes[sortCols[i]];
    }

    int32_t newSourceTypesCount = newSourceTypes.size();
    ParamValue pSourceTypes = ParamValue(newSourceTypes.data(), newSourceTypesCount);
    ParamValue pTypeCount = ParamValue(&newSourceTypesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    ParamValue pSortCols = ParamValue(sortCols, sortKeysCount);
    ParamValue pSortColTypes = ParamValue(sortColTypes, sortKeysCount);
    ParamValue pSortAscendings = ParamValue(sortAscendings, sortKeysCount);
    ParamValue pSortNullFirsts = ParamValue(sortNullFirsts, sortKeysCount);
    ParamValue pSortColCount = ParamValue(&sortKeysCount);

    auto compareToSp = new Specialization();
    compareToSp->AddSpecializedParam(0, &pSortCols);
    compareToSp->AddSpecializedParam(1, &pSortColTypes);
    compareToSp->AddSpecializedParam(2, &pSortAscendings);
    compareToSp->AddSpecializedParam(3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(4, &pSortColCount);

    auto getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &pOutputCols);
    getOutputSp->AddSpecializedParam(2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(4, &pSourceTypes);

    map<string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };

    auto sortWithExprContext = new Context(GenerateOperatorTemplatePath("sort_expr"), map<string, Specialization>());
    auto sortContext = new Context(GenerateOperatorTemplatePath("sort"), map<string, Specialization>());
    auto pagesIndexContext = new Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit = new Jit(vector<Context> { *sortWithExprContext, *sortContext, *pagesIndexContext });
    jit->Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH,
        ModuleOptimization::CONSTANT_MERGE });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete sortWithExprContext;
    delete sortContext;
    delete pagesIndexContext;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortKeys, jintArray jAscendings,
    jintArray jNullFirsts)
{
    auto sourceTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputCols = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    auto outputColsCount = env->GetArrayLength(jOutputCols);
    auto sortKeysCount = env->GetArrayLength(jSortKeys);
    std::string sortKeysArr[sortKeysCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeysCount);
    jint *ascendings = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirsts = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesChars);

    JitContext *jitContext = CreateSortWithExprJitContext(sourceVecTypes, outputCols, outputColsCount, sortKeysArr,
        ascendings, nullFirsts, sortKeysCount);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortKeys, jintArray jAscendings,
    jintArray jNullFirsts, jlong jitContext)
{
    JNI_DEBUG_LOG("create sort with expression operator factory starting.");
    auto start = START();
    auto sourceTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputCols = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    auto outputColsCount = env->GetArrayLength(jOutputCols);
    auto sortKeysCount = env->GetArrayLength(jSortKeys);
    std::string sortKeysArr[sortKeysCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeysCount);
    jint *ascendings = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirsts = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesChars);

    JNI_DEBUG_LOG("before create sort with expression operator factory elapsed time: %ld ms.", END(start));
    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
        sourceVecTypes, outputCols, outputColsCount, sortKeysArr, ascendings, nullFirsts, sortKeysCount);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create sort with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}

JitContext *CreateHashBuilderWithExprJitContext(VecTypes &buildVecTypes, string *buildHashKeys,
    int32_t buildHashKeysCount, int32_t operatorCount)
{
    vector<int32_t> buildTypes;
    int32_t buildHashCols[buildHashKeysCount];
    GetTypeIds(buildVecTypes, buildHashKeys, buildHashKeysCount, buildTypes, buildHashCols);

    int32_t hashColTypes[buildHashKeysCount];
    for (int32_t i = 0; i < buildHashKeysCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashKeysCount);
    ParamValue pHashColCount = ParamValue(&buildHashKeysCount);

    auto hashPositionSp = new Specialization();
    hashPositionSp->AddSpecializedParam(3, &pHashColTypes);
    hashPositionSp->AddSpecializedParam(4, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION, *hashPositionSp } };

    auto positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);

    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS,
        *positionEqualsPositionIgnoreNullsSp } };

    auto hashBuilderWithExprContext =
        new Context(GenerateOperatorTemplatePath("hash_builder_expr"), map<string, Specialization>());
    auto hashBuilderContext = new Context(GenerateOperatorTemplatePath("hash_builder"), map<string, Specialization>());
    auto joinHashTableContext = new Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext = new Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(vector<Context> { *hashBuilderWithExprContext, *hashBuilderContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete hashPositionSp;
    delete positionEqualsPositionIgnoreNullsSp;
    delete hashBuilderWithExprContext;
    delete hashBuilderContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashKeys, jstring jFilter, jint jOperatorCount)
{
    auto buildTypesChars = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashKeysCount = env->GetArrayLength(jBuildHashKeys);
    std::string buildHashKeysArr[buildHashKeysCount];
    GetExpressions(env, jBuildHashKeys, buildHashKeysArr, buildHashKeysCount);
    auto buildVecTypes = Deserialize(buildTypesChars);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesChars);

    JitContext *jitContext =
        CreateHashBuilderWithExprJitContext(buildVecTypes, buildHashKeysArr, buildHashKeysCount, jOperatorCount);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprOperatorFactory
    (JNIEnv *env, jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashKeys, jstring jFilter, jint jHashTableCount,
    jlong jitContext)
{
    JNI_DEBUG_LOG("create hash builder with expression operator factory starting.");
    auto start = START();
    auto buildTypesChars = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashKeysCount = env->GetArrayLength(jBuildHashKeys);
    std::string buildHashKeysArr[buildHashKeysCount];
    GetExpressions(env, jBuildHashKeys, buildHashKeysArr, buildHashKeysCount);
    auto buildVecTypes = Deserialize(buildTypesChars);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesChars);
    std::string filterExpression;
    if (jFilter == nullptr) {
        filterExpression = "";
    } else {
        auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
        std::string filterExpression = std::string(filterChars);
        env->ReleaseStringUTFChars(jFilter, filterChars);
    }

    JNI_DEBUG_LOG("before create hash builder with expression operator factory elapsed time: %ld ms.", END(start));
    HashBuilderWithExprOperatorFactory *operatorFactory =
        HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildVecTypes, buildHashKeysArr,
        buildHashKeysCount, filterExpression, jHashTableCount);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}

JitContext *CreateLookupJoinWithExprJitContext(VecTypes &probeVecTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, string *probeHashKeys, int32_t probeHashKeysCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount)
{
    vector<int32_t> probeTypes;
    int32_t probeHashCols[probeHashKeysCount];
    GetTypeIds(probeVecTypes, probeHashKeys, probeHashKeysCount, probeTypes, probeHashCols);

    int32_t hashColTypes[probeHashKeysCount];
    for (int32_t i = 0; i < probeHashKeysCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashKeysCount);
    ParamValue pHashColCount = ParamValue(&probeHashKeysCount);

    auto buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->AddSpecializedParam(3, &pBuildOutputTypes);
    buildBuildColumnsSp->AddSpecializedParam(4, &pBuildOutputCols);
    buildBuildColumnsSp->AddSpecializedParam(5, &pBuildOutputColsCount);
    buildBuildColumnsSp->AddSpecializedParam(6, &pProbeOutputColsCount);

    auto populateHashesSp = new Specialization();
    populateHashesSp->AddSpecializedParam(2, &pHashColTypes);
    populateHashesSp->AddSpecializedParam(3, &pHashColCount);
    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp },
        { OMNIJIT_HASH_LOOKUP_JOIN_POPULATE_HASHES, *populateHashesSp } };

    auto hashRowSp = new Specialization();
    hashRowSp->AddSpecializedParam(2, &pHashColTypes);
    hashRowSp->AddSpecializedParam(3, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };

    auto positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };

    auto lookupJoinWithExprContext =
        new Context(GenerateOperatorTemplatePath("lookup_join_expr"), map<string, Specialization>());
    auto lookupJoinContext = new Context(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    auto joinHashTableContext = new Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext = new Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(vector<Context> { *lookupJoinWithExprContext, *lookupJoinContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete buildBuildColumnsSp;
    delete populateHashesSp;
    delete hashRowSp;
    delete positionEqualsRowIgnoreNullsSp;
    delete lookupJoinWithExprContext;
    delete lookupJoinContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashKeys,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType)
{
    auto probeTypesChars = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputCols = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashKeysCount = env->GetArrayLength(jProbeHashKeys);
    std::string probeHashKeysArr[probeHashKeysCount];
    GetExpressions(env, jProbeHashKeys, probeHashKeysArr, probeHashKeysCount);
    auto buildOutputCols = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesChars = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesChars);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    JitContext *jitContext =
        CreateLookupJoinWithExprJitContext(probeVecTypes, probeOutputCols, probeOutputColsCount, probeHashKeysArr,
        probeHashKeysCount, buildOutputCols, buildOutputVecTypes.GetIds(), buildOutputVecTypes.GetSize());
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashKeys,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory,
    jlong jitContext)
{
    JNI_DEBUG_LOG("create lookup join with expression operator factory starting.");
    auto start = START();
    auto probeTypesChars = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputCols = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashKeysCount = env->GetArrayLength(jProbeHashKeys);
    std::string probeHashKeysArr[probeHashKeysCount];
    GetExpressions(env, jProbeHashKeys, probeHashKeysArr, probeHashKeysCount);
    auto buildOutputCols = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesChars = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesChars);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    JNI_DEBUG_LOG("before create lookup join with expression operator factory elapsed time: %ld ms.", END(start));
    LookupJoinWithExprOperatorFactory *operatorFactory =
        LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(probeVecTypes, probeOutputCols,
        probeOutputColsCount, probeHashKeysArr, probeHashKeysCount, buildOutputCols, buildOutputVecTypes,
        (JoinType)jJoinType, jHashBuilderOperatorFactory);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create lookup join with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}

JitContext *CreateWindowWithExprJitContext(VecTypes &sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, VecTypes &outputTypes, string* argumentKeys,
    int argumentKeysCount);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprJitContext(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jobjectArray jArgumentKeys,
    jstring jWindowFunctionReturnType)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *windowFunction = env->GetIntArrayElements(jWindowFunction, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *preGroupedChannels = env->GetIntArrayElements(JPreGroupedChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    auto argumentKeysCount = env->GetArrayLength(jArgumentKeys);
    std::string argumentKeysArr[argumentKeysCount];
    GetExpressions(env, jArgumentKeys, argumentKeysArr, argumentKeysCount);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputVecTypes = Deserialize(windowFunctionReturnTypeCharPtr);

    jint inputTypesCount = inputVecTypes.GetSize();
    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint outputTypesCount = outputVecTypes.GetSize();

    JitContext *jitContext = CreateWindowWithExprJitContext(inputVecTypes,
        inputVecTypes.GetSize(), outputChannels, outputColsCount, partitionChannels, partitionCount, sortChannels,
        sortOrder, sortNullFirsts, sortColCount, outputVecTypes, argumentKeysArr, argumentKeysCount);

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)jitContext;
}

JitContext *CreateWindowWithExprJitContext(VecTypes &sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, VecTypes &outputTypes, string* argumentKeys,
    int argumentKeysCount)
{
    using namespace omniruntime::jit;
    std::vector<VecType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), sourceTypes.Get().begin(), sourceTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputTypes.Get().begin(), outputTypes.Get().end());

    VecTypes allTypes(allTypesVec);
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

    auto inputTypes = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t finalSortColTypes[finalSortColsCount];
    for (int32_t i = 0; i < finalSortColsCount; i++) {
        finalSortColTypes[i] = inputTypes[finalSortCols[i]];
    }
    auto allCount = allTypes.GetSize();
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

    ParamValue pSortCols = ParamValue(finalSortCols, finalSortColsCount);
    ParamValue pSortColTypes = ParamValue(finalSortColTypes, finalSortColsCount);
    ParamValue pSortAscendings = ParamValue(finalSortAscendings, finalSortColsCount);
    ParamValue pSortNullFirsts = ParamValue(finalSortNullFirsts, finalSortColsCount);
    ParamValue pSortColCount = ParamValue(&finalSortColsCount);

    ParamValue pSourceTypes = ParamValue(inputTypes, typesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);

    auto *compareToSp = new Specialization();
    compareToSp->AddSpecializedParam(0, &pSortCols);
    compareToSp->AddSpecializedParam(1, &pSortColTypes);
    compareToSp->AddSpecializedParam(2, &pSortAscendings);
    compareToSp->AddSpecializedParam(3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(4, &pSortColCount);
    auto *getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &pOutputCols);
    getOutputSp->AddSpecializedParam(2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(4, &pSourceTypes);
    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowWithExprContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("window_expr"),
            std::map<std::string, Specialization>());
    auto *windowContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("window"), std::map<std::string, Specialization>());
    auto *pagesIndexContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit =
        new Jit(std::vector<omniruntime::jit::Context> { *windowWithExprContext, *windowContext, *pagesIndexContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete windowWithExprContext;
    delete windowContext;
    delete pagesIndexContext;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jobjectArray jArgumentKeys,
    jstring jWindowFunctionReturnType, jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *windowFunction = env->GetIntArrayElements(jWindowFunction, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *preGroupedChannels = env->GetIntArrayElements(JPreGroupedChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    auto argumentKeysArrCount = env->GetArrayLength(jArgumentKeys);
    std::string argumentKeysArr[argumentKeysArrCount];
    GetExpressions(env, jArgumentKeys, argumentKeysArr, argumentKeysArrCount);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputVecTypes = Deserialize(windowFunctionReturnTypeCharPtr);

    jint inputTypesCount = inputVecTypes.GetSize();
    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentKeysCount = env->GetArrayLength(jArgumentKeys);
    jint outputTypesCount = outputVecTypes.GetSize();

    omniruntime::op::WindowWithExprOperatorFactory *windowWithExprOperatorFactory =
        omniruntime::op::WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(inputVecTypes,
            outputChannels, outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount,
            preGroupedChannels, preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount,
            preSortedChannelPrefix, expectedPositions, outputVecTypes, argumentKeysArr, argumentKeysCount);

    windowWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)windowWithExprOperatorFactory;
}

JitContext *CreateHashAggregationWithExprJitContext(std::string *projectKeys, int32_t groupByNum, int32_t aggNum,
    int32_t *projectCols, std::vector<int32_t> projectTypes, uint32_t *aggFuncTypes)
{
    int32_t groupByCols[groupByNum];
    std::copy(projectCols, projectCols + groupByNum, groupByCols);
    int32_t aggCols[aggNum];
    std::copy(projectCols + groupByNum, projectCols + groupByNum + aggNum, aggCols);

    int32_t groupByTypeIds[groupByNum];
    std::copy(projectTypes.begin(), projectTypes.begin() + groupByNum, groupByTypeIds);
    int32_t aggTypeIds[aggNum];
    std::copy(projectTypes.begin() + groupByNum, projectTypes.begin() + groupByNum + aggNum, aggTypeIds);

    PrepareContext groupByColContext = { (uint32_t *)groupByCols, static_cast<size_t>(groupByNum) };
    PrepareContext groupByTypeContext = { (uint32_t *)groupByTypeIds, static_cast<size_t>(groupByNum) };
    PrepareContext aggColContext = { (uint32_t *)aggCols, static_cast<size_t>(aggNum) };
    PrepareContext aggTypeContext = { (uint32_t *)aggTypeIds, static_cast<size_t>(aggNum) };
    PrepareContext aggFuncTypeContext = { aggFuncTypes, static_cast<size_t>(aggNum) };

    using namespace omniruntime::jit;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByNum + aggNum;
    ParamValue pColType = ParamValue(projectTypes.data(), colNum);
    ParamValue pColCount = ParamValue(&colNum);
    ParamValue pGroupNum = ParamValue(&groupByNum);
    ParamValue pAggNum = ParamValue(&aggColNum);
    ParamValue pAggTypes = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);
    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &pColType);
    inloopSp->AddSpecializedParam(4, &pColCount);
    inloopSp->AddSpecializedParam(6, &pGroupNum);
    inloopSp->AddSpecializedParam(8, &pAggNum);
    inloopSp->AddSpecializedParam(9, &pAggTypes);

    auto *processAggSp = new Specialization();
    processAggSp->AddSpecializedParam(2, &pAggNum);
    processAggSp->AddSpecializedParam(3, &pColType);

    std::map<std::string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp },
        { OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp } };

    auto *groupAggWithExprContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("group_aggregation_expr"), map<string, Specialization>());
    auto *groupAggContext = new Context(GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *groupAggWithExprContext, *groupAggContext });
    jit->Specialize(std::vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        std::vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete groupAggWithExprContext;
    delete groupAggContext;
    delete inloopSp;
    delete processAggSp;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprJitContext(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannel, jstring jSourceType,
    jintArray jAggFuncType, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial)
{
    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);
    size_t aggNum = static_cast<size_t>(env->GetArrayLength(jAggChannel));
    std::string aggKeys[aggNum];
    GetExpressions(env, jAggChannel, aggKeys, aggNum);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);
    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    auto outVecTypes = Deserialize(outTypesCharPtr);

    size_t colNum = aggNum + groupByNum;
    std::string projectKeys[groupByNum + aggNum];
    std::copy(groupByKeys, groupByKeys + groupByNum, projectKeys);
    std::copy(aggKeys, aggKeys + aggNum, projectKeys + groupByNum);

    std::vector<int32_t> projectTypes;
    int32_t projectCols[colNum];
    GetRequiredTypeIds(sourceVecTypes, projectKeys, colNum, projectTypes, projectCols);

    JitContext *jitContext = CreateHashAggregationWithExprJitContext(projectKeys, groupByNum, aggNum, projectCols,
        projectTypes, reinterpret_cast<uint32_t *>(aggFuncTypes));

    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    return reinterpret_cast<uint64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannel, jstring jSourceType,
    jintArray jAggFuncType, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial, jlong jitContext)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and id
    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);
    size_t aggNum = static_cast<size_t>(env->GetArrayLength(jAggChannel));
    std::string aggKeys[aggNum];
    GetExpressions(env, jAggChannel, aggKeys, aggNum);

    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);

    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    auto outVecTypes = Deserialize(outTypesCharPtr);

    omniruntime::op::HashAggregationWithExprOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys, aggNum,
        sourceVecTypes, outVecTypes, (uint32_t *)aggFuncTypes, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JitContext *CreateTopNWithExprJitContext(std::vector<int32_t> sourceTypes, int32_t sortKeyCount, int32_t *sortCols)
{
    ParamValue pSourceTypes = ParamValue(sourceTypes.data(), sourceTypes.size());
    ParamValue pSortKeyCount = ParamValue(&sortKeyCount);
    ParamValue pSortCols = ParamValue(sortCols, sortKeyCount);

    auto *topNCompareSp = new Specialization();
    topNCompareSp->AddSpecializedParam(4, &pSortKeyCount); // 4teh parameter
    topNCompareSp->AddSpecializedParam(5, &pSortCols);     // 5teh parameter
    topNCompareSp->AddSpecializedParam(6, &pSourceTypes);  // 6teh parameter

    std::map<std::string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, *topNCompareSp } };

    auto *topNWithExprContext =
            new Context(GenerateOperatorTemplatePath("topn_expr"), map<string, Specialization>());
    auto *topNContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *topNWithExprContext, *topNContext });

    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete topNWithExprContext;
    delete topNContext;
    delete topNCompareSp;
    delete jit;

    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    using namespace omniruntime::jit;
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortKeyCount = env->GetArrayLength(jSortKeys);

    std::string sortKeysArr[sortKeyCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeyCount);
    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);

    std::vector<int32_t> sourceTypes;
    int32_t sortCols[sortKeyCount];
    GetTypeIds(sourceVecTypes, sortKeysArr, sortKeyCount, sourceTypes, sortCols);

    JitContext *jitContext = CreateTopNWithExprJitContext(sourceTypes, reinterpret_cast<int32_t>(sortKeyCount),
        sortCols);

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return reinterpret_cast<uint64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc,
    jintArray jSortNullFirsts, jlong jitContext)
{
    using namespace omniruntime::jit;
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortKeyCount = env->GetArrayLength(jSortKeys);
    std::string sortKeysArr[sortKeyCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeyCount);

    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    omniruntime::op::TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new omniruntime::op::TopNWithExprOperatorFactory(sourceVecTypes, n, sortKeysArr, sortAsc, sortNullFirsts,
        sortKeyCount);
    topNWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)topNWithExprOperatorFactory;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_closeNativeOperatorFactory
        (JNIEnv *env, jclass jclz, jlong jNativeOperatorFactory)
{
    OperatorFactory *nativeOperatorFactory = reinterpret_cast<OperatorFactory *>(jNativeOperatorFactory);
    if (nativeOperatorFactory != nullptr) {
        delete nativeOperatorFactory;
    }
}
