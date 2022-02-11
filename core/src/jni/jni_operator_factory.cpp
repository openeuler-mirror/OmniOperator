/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "jni_operator_factory.h"
#include "../operator/operator_factory.h"
#include "../operator/jit_context/jit_context.h"
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
#include "../operator/join/sortmergejoin/sort_merge_join_expr.h"
#include "../operator/topn/topn.h"
#include "../operator/topn/topn_expr.h"
#include "../operator/partitionedoutput/partitionedoutput.h"
#include "../operator/union/union.h"
#include "../operator/window/window_expr.h"
#include "../operator/limit/limit.h"
#include "../operator/limit/distinct_limit.h"
#include "config.h"

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

#if defined(DEBUG_OPERATOR)
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

/**
 * Return an HashAggregationFactory object address.
 *                                                                  */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationJitContext(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial)
{
    // groupby channel and id
    auto groupByNum = env->GetArrayLength(jGroupByChannel);
    int32_t groupByCols[groupByNum];
    GetColumnsFromExpressions(env, jGroupByChannel, groupByCols, groupByNum);
    auto groupByTypesCharPtr = env->GetStringUTFChars(jGroupByType, JNI_FALSE);
    auto aggNum = env->GetArrayLength(jAggChannel);
    int32_t aggCols[aggNum];
    GetColumnsFromExpressions(env, jAggChannel, aggCols, aggNum);
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    auto aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    auto aggFuncsCount = env->GetArrayLength(jAggFuncType);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);

    auto groupByVecTypes = Deserialize(groupByTypesCharPtr);
    auto aggVecTypes = Deserialize(aggTypesCharPtr);
    auto outputVecTypes = Deserialize(outTypesCharPtr);
    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    auto jitContext = CreateHashAggregationJitContext(groupByVecTypes, groupByCols, aggVecTypes, aggCols, aggFuncTypes,
        aggFuncsCount, outputVecTypes);
    return reinterpret_cast<uint64_t>(jitContext);
}

/**
 * Return an HashAggregationFactory object address.
 *                                                                  */
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

/**
 * Return an AggregationFactory object address.
 *                                                                  */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jAggFuncTypes, jintArray jAggInputCols, jintArray jMaskCols,
    jstring jAggOutputTypes, jboolean inputRaw, jboolean outputPartial)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    auto aggFuncTypes = env->GetIntArrayElements(jAggFuncTypes, JNI_FALSE);
    auto aggFuncsCount = env->GetArrayLength(jAggFuncTypes);
    auto aggInputCols = env->GetIntArrayElements(jAggInputCols, JNI_FALSE);
    auto aggMaskCols = env->GetIntArrayElements(jMaskCols, JNI_FALSE);
    auto aggOutputTypesCharPtr = env->GetStringUTFChars(jAggOutputTypes, JNI_FALSE);
    auto aggOutputTypes = Deserialize(aggOutputTypesCharPtr);
    env->ReleaseStringUTFChars(jAggOutputTypes, aggOutputTypesCharPtr);

    auto jitContext = CreateAggregationJitContext(sourceTypes, aggInputCols, aggMaskCols, aggFuncTypes, aggFuncsCount,
        aggOutputTypes);
    return reinterpret_cast<uint64_t>(jitContext);
}

/**
 * Return an AggregationFactory object address.
 *                                                                   */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jintArray jAggFuncTypes, jintArray jAggInputCols,
    jintArray jMaskCols, jstring jAggOutputTypes, jboolean inputRaw, jboolean outputPartial, jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    auto aggFuncTypes = env->GetIntArrayElements(jAggFuncTypes, JNI_FALSE);
    auto aggInputCols = env->GetIntArrayElements(jAggInputCols, JNI_FALSE);
    auto maskCols = env->GetIntArrayElements(jMaskCols, JNI_FALSE);
    auto aggOutputTypesCharPtr = env->GetStringUTFChars(jAggOutputTypes, JNI_FALSE);
    auto aggOutputTypes = Deserialize(aggOutputTypesCharPtr);
    env->ReleaseStringUTFChars(jAggOutputTypes, aggOutputTypesCharPtr);

    auto sourceTypeIds = sourceTypes.GetIds();
    auto aggCount = static_cast<size_t>(aggOutputTypes.GetSize());

    PrepareContext aggInputColsContext = { (uint32_t *)aggInputCols, aggCount };
    PrepareContext maskColsContext = { (uint32_t *)maskCols, aggCount };
    PrepareContext aggFuncTypesContext = { (uint32_t *)aggFuncTypes, aggCount };

    omniruntime::op::AggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::AggregationOperatorFactory(sourceTypes, aggFuncTypesContext, aggInputColsContext,
        maskColsContext, aggOutputTypes, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    nativeOperatorFactory->Init();

    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

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

    auto jitContext = CreateSortJitContext(sourceVecTypes, outputColsArr, outputColsCount, sortColsArr, ascendingsArr,
        nullFirstsArr, sortColsCount);

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
    auto sortOperatorFactory = omniruntime::op::SortOperatorFactory::CreateSortOperatorFactory(sourceVecTypes,
        outputColsArr, outputColsCount, sortColsArr, ascendingsArr, nullFirstsArr, sortColsCount);

    sortOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)sortOperatorFactory;
}

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

    JitContext *jitContext = CreateWindowJitContext(inputVecTypes, outputChannels, outputColsCount, partitionChannels,
        partitionCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, const_cast<int32_t *>(allTypes.GetIds()),
        allTypes.GetSize());

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
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortColCount = env->GetArrayLength(jSortCols);
    int32_t sortCols[sortColCount];
    GetColumnsFromExpressions(env, jSortCols, sortCols, sortColCount);
    jint *sortAscendings = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    auto jitContext = CreateTopNJitContext(sourceTypes, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc,
    jintArray jSortNullFirsts, jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortColCount = env->GetArrayLength(jSortCols);
    int32_t sortColsArr[sortColCount];
    GetColumnsFromExpressions(env, jSortCols, sortColsArr, sortColCount);
    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new omniruntime::op::TopNOperatorFactory(sourceTypes, n, sortColsArr, sortAsc, sortNullFirsts, sortColCount);

    topNOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    return (int64_t)topNOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jInputTypes, jint jInputLength, jstring jExpression, jobjectArray jProjections,
    jint jProjectLength, jlong jitContext, jint jParseFormat)
{
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputVecTypes = Deserialize(inputTypesCharPtr);
    auto inputTypeIds = const_cast<int32_t *>(inputVecTypes.GetIds());
    auto inputLength = (int32_t)jInputLength;

    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    auto *projectExpressions = new std::string[jProjectLength];
    for (int32_t i = 0; i < jProjectLength; i++) {
        auto st = (jstring)(env->GetObjectArrayElement(jProjections, i));
        auto exprStringPtr = env->GetStringUTFChars(st, JNI_FALSE);
        projectExpressions[i] = exprStringPtr;
        env->ReleaseStringUTFChars(st, exprStringPtr);
    }
    auto projectLength = (int32_t)jProjectLength;
    std::vector<omniruntime::expressions::Expr *> projectExprs;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    if (parseFormat == JSON) {
        nlohmann::json *jsonProjectExprs = new nlohmann::json[jProjectLength];
        for (int32_t i = 0; i < projectLength; i++) {
            jsonProjectExprs[i] = nlohmann::json::parse(projectExpressions[i]);
        }
        projectExprs = JSONParser::ParseJSON(jsonProjectExprs, projectLength);
        filterExpr = JSONParser::ParseJSON(nlohmann::json::parse(filterExpression));
    } else {
        Parser parser;
        filterExpr = parser.ParseRowExpression(filterExpression, inputVecTypes, inputLength);
        projectExprs = parser.ParseExpressions(projectExpressions, projectLength, inputVecTypes);
    }
    if (filterExpr == nullptr || (projectLength > 0 && projectExprs[0] == nullptr)) {
        return 0;
    }
    omniruntime::op::FilterAndProjectOperatorFactory *factory = new omniruntime::op::FilterAndProjectOperatorFactory(
        filterExpr, inputVecTypes, inputLength, projectExprs, projectLength);
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
    jobject jobj, jstring jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength, jlong jitContext,
    jint jParseFormat)
{
    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string *exprs = new std::string[jExprsLength];
    for (int32_t i = 0; i < jExprsLength; i++) {
        jstring st = (jstring)(env->GetObjectArrayElement(jExprs, i));
        auto rawString = env->GetStringUTFChars(st, 0);
        exprs[i] = rawString;
    }
    int32_t exprLength = (int32_t)jExprsLength;
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputVecTypes = Deserialize(inputTypesCharPtr);
    int32_t inputLength = (int32_t)jInputLength;

    std::vector<omniruntime::expressions::Expr *> expressions;
    if (parseFormat == JSON) {
        nlohmann::json *jsonExprs = new nlohmann::json[jExprsLength];
        for (int32_t i = 0; i < exprLength; i++) {
            jsonExprs[i] = nlohmann::json::parse(exprs[i]);
        }
        expressions = JSONParser::ParseJSON(jsonExprs, exprLength);
    } else {
        Parser parser;
        expressions = parser.ParseExpressions(exprs, exprLength, inputVecTypes);
    }

    if (exprLength > 0 && expressions[0] == nullptr) {
        return 0;
    }

    omniruntime::op::ProjectionOperatorFactory *factory =
        new omniruntime::op::ProjectionOperatorFactory(expressions, exprLength, inputVecTypes, inputLength);
    if (!factory->IsSupported()) {
        delete factory;
        return 0;
    }

    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);

    return (int64_t)factory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderJitContext(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jintArray jBuildHashCols, jstring jFilterExpr, jint jSortChannel,
    jobjectArray jSearchExprs, jint jOperatorCount)
{
    auto buildTypesCharPtr = (env)->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    auto buildHashColsArr = env->GetIntArrayElements(jBuildHashCols, JNI_FALSE);

    auto buildVecTypes = Deserialize(buildTypesCharPtr);
    auto jitContext = CreateHashBuilderJitContext(buildVecTypes, buildHashColsArr, buildHashColsCount, jOperatorCount);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    return (int64_t)(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jintArray jBuildHashCols, jstring jFilterExpr, jint jSortChannel,
    jobjectArray jSearchExprs, jint jOperatorCount, jlong jitContext)
{
    JNI_DEBUG_LOG("create hash builder operator factory starting.");
    auto start = START();
    auto buildTypesCharPtr = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    auto buildHashColsArr = env->GetIntArrayElements(jBuildHashCols, JNI_FALSE);

    auto buildVecTypes = Deserialize(buildTypesCharPtr);
    auto filterChars = env->GetStringUTFChars(jFilterExpr, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilterExpr, filterChars);

    JNI_DEBUG_LOG("before create hash builder operator factory elapsed time: %ld ms.", END(start));
    auto hashBuilderOperatorFactory = omniruntime::op::HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildVecTypes, buildHashColsArr, buildHashColsCount, filterExpression, jOperatorCount);

    hashBuilderOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    return (int64_t)hashBuilderOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinJitContext(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jintArray jProbeHashCols, jintArray jBuildOutputCols,
    jstring jBuildOutputTypes, jint jJoinType)
{
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    auto probeHashColsArr = env->GetIntArrayElements(jProbeHashCols, JNI_FALSE);
    auto buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    auto probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesCharPtr);
    auto jitContext = CreateLookupJoinJitContext(probeVecTypes, probeOutputColsArr, probeOutputColsCount,
        probeHashColsArr, probeHashColsCount, buildOutputVecTypes, buildOutputColsArr);

    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);
    return (int64_t)jitContext;
}

omniruntime::expressions::Expr *CreateJoinFilterExpr(Parser &parser, std::string &filterString, VecTypes &buildTypes,
    VecTypes &probeTypes)
{
    omniruntime::expressions::Expr *filterExpr = nullptr;
    if (filterString.compare("") != 0) {
        std::vector<VecType> allTypes;
        for (int32_t i = 0; i < probeTypes.GetSize(); i++) {
            allTypes.push_back(probeTypes.Get().at(i));
        }

        for (int32_t i = 0; i < buildTypes.GetSize(); i++) {
            allTypes.push_back(buildTypes.Get().at(i));
        }
        VecTypes vecTypes(allTypes);

        filterExpr = parser.ParseRowExpression(filterString, vecTypes, vecTypes.GetSize());
    }
    return filterExpr;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jintArray jProbeHashCols, jintArray jBuildOutputCols,
    jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory, jlong jitContext)
{
    JNI_DEBUG_LOG("create lookup join operator factory starting.");
    auto start = START();
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    auto probeHashColsArr = env->GetIntArrayElements(jProbeHashCols, JNI_FALSE);
    auto buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    auto probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeVecTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputVecTypes = Deserialize(buildOutputTypesCharPtr);

    // extract the expression and the BuildVecTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderOperatorFactory *>(jHashBuilderOperatorFactory)->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    VecTypes *buildTypes = hashTables->GetBuildVecTypes();
    Parser parser;
    auto filterExpr = CreateJoinFilterExpr(parser, filterExpression, *buildTypes, probeVecTypes);
    hashTables->SetFilterExpr(filterExpr);

    JNI_DEBUG_LOG("before create lookup join operator factory elapsed time: %ld ms.", END(start));
    auto lookupJoinOperatorFactory = omniruntime::op::LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeVecTypes, probeOutputColsArr, probeOutputColsCount, probeHashColsArr, probeHashColsCount,
        buildOutputColsArr, buildOutputVecTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory);

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

void DeleteExprs(std::vector<omniruntime::expressions::Expr *> &exprs)
{
    int32_t exprsCount = exprs.size();
    for (int32_t i = 0; i < exprsCount; i++) {
        delete exprs[i];
    }
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

    // parse the expressions
    Parser parser;
    auto sortExprsArr = parser.ParseExpressions(sortKeysArr, sortKeysCount, sourceVecTypes);
    auto jitContext =
        CreateSortWithExprJitContext(sourceVecTypes, outputCols, outputColsCount, sortExprsArr, ascendings, nullFirsts);
    DeleteExprs(sortExprsArr);
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

    // parse the expressions
    Parser parser;
    std::vector<omniruntime::expressions::Expr *> sortKeysArrExprs =
        parser.ParseExpressions(sortKeysArr, sortKeysCount, sourceVecTypes);

    JNI_DEBUG_LOG("before create sort with expression operator factory elapsed time: %ld ms.", END(start));
    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceVecTypes, outputCols,
        outputColsCount, sortKeysArrExprs, ascendings, nullFirsts, sortKeysCount);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create sort with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
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

    // parse the expressions
    Parser parser;
    auto buildHashExprsArr = parser.ParseExpressions(buildHashKeysArr, buildHashKeysCount, buildVecTypes);
    auto jitContext = CreateHashBuilderWithExprJitContext(buildVecTypes, buildHashExprsArr, jOperatorCount);
    DeleteExprs(buildHashExprsArr);
    return (int64_t)jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashKeys, jstring jFilter, jint jHashTableCount,
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
    auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilter, filterChars);
    Parser parser;
    std::vector<omniruntime::expressions::Expr *> buildHashKeysArrExprs =
        parser.ParseExpressions(buildHashKeysArr, buildHashKeysCount, buildVecTypes);

    JNI_DEBUG_LOG("before create hash builder with expression operator factory elapsed time: %ld ms.", END(start));
    auto operatorFactory = HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildVecTypes,
        buildHashKeysArrExprs, buildHashKeysCount, filterExpression, jHashTableCount);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
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

    // parse the expressions
    Parser parser;
    auto probeHashExprsArr = parser.ParseExpressions(probeHashKeysArr, probeHashKeysCount, probeVecTypes);
    auto jitContext = CreateLookupJoinWithExprJitContext(probeVecTypes, probeOutputCols, probeOutputColsCount,
        probeHashExprsArr, buildOutputVecTypes, buildOutputCols);
    DeleteExprs(probeHashExprsArr);
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

    // extract the expression and the BuildVecTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderWithExprOperatorFactory *>(jHashBuilderOperatorFactory)
                          ->GetHashBuilderOperatorFactory()
                          ->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    VecTypes *buildTypes = hashTables->GetBuildVecTypes();
    Parser parser;
    auto filterExpr = CreateJoinFilterExpr(parser, filterExpression, *buildTypes, probeVecTypes);
    hashTables->SetFilterExpr(filterExpr);

    std::vector<omniruntime::expressions::Expr *> probeHashKeysArrExprs =
        parser.ParseExpressions(probeHashKeysArr, probeHashKeysCount, probeVecTypes);

    JNI_DEBUG_LOG("before create lookup join with expression operator factory elapsed time: %ld ms.", END(start));
    auto operatorFactory = LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(probeVecTypes,
        probeOutputCols, probeOutputColsCount, probeHashKeysArrExprs, probeHashKeysCount, buildOutputCols,
        buildOutputVecTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create lookup join with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprJitContext(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
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

    // parse the expressions
    Parser parser;
    auto argumentExprsArr = parser.ParseExpressions(argumentKeysArr, argumentKeysCount, inputVecTypes);
    JitContext *jitContext =
        CreateWindowWithExprJitContext(inputVecTypes, outputChannels, outputColsCount, partitionChannels,
        partitionCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, outputVecTypes, argumentExprsArr);

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    DeleteExprs(argumentExprsArr);
    return (int64_t)jitContext;
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

    Parser parser;
    std::vector<omniruntime::expressions::Expr *> argumentKeysArrExprs =
        parser.ParseExpressions(argumentKeysArr, argumentKeysArrCount, inputVecTypes);

    omniruntime::op::WindowWithExprOperatorFactory *windowWithExprOperatorFactory =
        omniruntime::op::WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(inputVecTypes,
        outputChannels, outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount,
        preGroupedChannels, preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount,
        preSortedChannelPrefix, expectedPositions, outputVecTypes, argumentKeysArrExprs, argumentKeysCount);

    windowWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)windowWithExprOperatorFactory;
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
    auto aggNum = env->GetArrayLength(jAggChannel);
    std::string aggKeys[aggNum];
    GetExpressions(env, jAggChannel, aggKeys, aggNum);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    auto aggFuncsCount = env->GetArrayLength(jAggFuncType);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);
    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    auto outputVecTypes = Deserialize(outTypesCharPtr);

    Parser parser;
    auto groupByExprsArr = parser.ParseExpressions(groupByKeys, groupByNum, sourceVecTypes);
    auto aggExprsArr = parser.ParseExpressions(aggKeys, aggNum, sourceVecTypes);
    JitContext *jitContext = CreateHashAggregationWithExprJitContext(sourceVecTypes, groupByExprsArr, aggExprsArr,
        aggFuncTypes, aggFuncsCount, outputVecTypes);

    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);
    DeleteExprs(groupByExprsArr);
    DeleteExprs(aggExprsArr);
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

    Parser parser;
    std::vector<omniruntime::expressions::Expr *> groupByKeysExprs =
        parser.ParseExpressions(groupByKeys, groupByNum, sourceVecTypes);
    std::vector<omniruntime::expressions::Expr *> aggKeysExprs =
        parser.ParseExpressions(aggKeys, aggNum, sourceVecTypes);

    omniruntime::op::HashAggregationWithExprOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationWithExprOperatorFactory(groupByKeysExprs, groupByNum, aggKeysExprs, aggNum,
        sourceVecTypes, outVecTypes, (uint32_t *)aggFuncTypes, inputRaw, outputPartial);

    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortKeysCount = env->GetArrayLength(jSortKeys);

    std::string sortKeysArr[sortKeysCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeysCount);
    jint *sortAscendings = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;

    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    // parse the expressions
    Parser parser;
    auto sortExprsArr = parser.ParseExpressions(sortKeysArr, sortKeysCount, sourceVecTypes);
    auto jitContext = CreateTopNWithExprJitContext(sourceVecTypes, sortExprsArr, sortAscendings, sortNullFirsts);
    DeleteExprs(sortExprsArr);
    return reinterpret_cast<uint64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc, jintArray jSortNullFirsts,
    jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortKeyCount = env->GetArrayLength(jSortKeys);
    std::string sortKeysArr[sortKeyCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeyCount);

    jint *sortAsc = env->GetIntArrayElements(jSortAsc, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    int32_t n = (int32_t)jN;
    auto sourceVecTypes = Deserialize(sourceTypesCharPtr);

    Parser parser;
    std::vector<omniruntime::expressions::Expr *> sortKeysArrExprs =
        parser.ParseExpressions(sortKeysArr, sortKeyCount, sourceVecTypes);

    omniruntime::op::TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new omniruntime::op::TopNWithExprOperatorFactory(sourceVecTypes, n, sortKeysArrExprs, sortAsc, sortNullFirsts,
        sortKeyCount);
    topNWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)topNWithExprOperatorFactory;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_closeNativeOperatorFactory(JNIEnv *env,
    jclass jclz, jlong jNativeOperatorFactory)
{
    OperatorFactory *nativeOperatorFactory = reinterpret_cast<OperatorFactory *>(jNativeOperatorFactory);
    if (nativeOperatorFactory != nullptr) {
        delete nativeOperatorFactory;
    }
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_limit_OmniLimitOperatorFactory_createLimitOperatorFactory(
    JNIEnv *env, jclass jObj, jlong jLimit)
{
    JNI_DEBUG_LOG("create limit operator factory starting.");
    auto start = START();
    JNI_DEBUG_LOG("before create limit operator factory elapsed time: %ld ms.", END(start));
    auto *limitOperatorFactory = omniruntime::op::LimitOperatorFactory::CreateLimitOperatorFactory(jLimit);
    JNI_DEBUG_LOG("create limit operator factory finished, elapsed time: %ld ms.", END(start));
    limitOperatorFactory->SetJitContext(nullptr);

    return (int64_t)limitOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_limit_OmniDistinctLimitOperatorFactory_createDistinctLimitOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSoureTypes, jintArray jDistinctChannel, jint jHashChannel, jlong jLimit)
{
    JNI_DEBUG_LOG("create distinct limit operator factory starting.");
    auto start = START();

    size_t distinctColCount = (size_t)env->GetArrayLength(jDistinctChannel);
    jint *distinctCols = env->GetIntArrayElements(jDistinctChannel, JNI_FALSE);

    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSoureTypes, JNI_FALSE);
    auto sourceTypes = Deserialize(sourceTypesCharPtr);

    JNI_DEBUG_LOG("before create distinct limit operator factory elapsed time: %ld ms.", END(start));
    auto *distinctLimitOperatorFactory =
        omniruntime::op::DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes, distinctCols,
        distinctColCount, jHashChannel, jLimit);
    JNI_DEBUG_LOG("create distinct limit operator factory finished, elapsed time: %ld ms.", END(start));
    distinctLimitOperatorFactory->SetJitContext(nullptr);

    env->ReleaseStringUTFChars(jSoureTypes, sourceTypesCharPtr);

    return (int64_t)distinctLimitOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjStreamedTableWithExprOperatorFactory_createSmjStreamedTableWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jint jJoinType, jstring jFilter)
{
    return 0;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjStreamedTableWithExprOperatorFactory_createSmjStreamedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jint jJoinType, jstring jFilter, jlong jitContext)
{
    if ((JoinType)jJoinType != OMNI_JOIN_TYPE_INNER) {
        return (int64_t)0;
    }

    JNI_DEBUG_LOG("create streamed table with expression operator factory starting.");
    auto start = START();

    auto streamedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto streamedVecTypes = Deserialize(streamedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, streamedTypesChars);

    auto streamedKeyExpsCount = env->GetArrayLength(jEqualKeyExprs);
    std::string streamedKeyExpsArr[streamedKeyExpsCount];
    GetExpressions(env, jEqualKeyExprs, streamedKeyExpsArr, streamedKeyExpsCount);

    auto streamedOutputColsCnt = env->GetArrayLength(jOutputChannels);
    auto streamedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);

    Parser parser;
    std::vector<omniruntime::expressions::Expr *> streamedKeysArrExprs =
        parser.ParseExpressions(streamedKeyExpsArr, streamedKeyExpsCount, streamedVecTypes);

    std::string filterExpression;
    if (jFilter == nullptr) {
        filterExpression = "";
    } else {
        auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
        filterExpression = std::string(filterChars);
        env->ReleaseStringUTFChars(jFilter, filterChars);
    }

    JNI_DEBUG_LOG("before create streamed table with expression operator factory elapsed time: %ld ms.", END(start));
    StreamedTableWithExprOperatorFactory *operatorFactory =
        StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(streamedVecTypes,
        streamedKeysArrExprs, streamedKeyExpsCount, streamedOutputCols, streamedOutputColsCnt, (JoinType)jJoinType,
        filterExpression);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create streamed table with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjBufferedTableWithExprOperatorFactory_createSmjBufferedTableWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels)
{
    return 0;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjBufferedTableWithExprOperatorFactory_createSmjBufferedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jlong jSmjStreamedTableWithExprOperatorFactory, jlong jitContext)
{
    JNI_DEBUG_LOG("create buffered table with expression operator factory starting.");
    auto start = START();

    auto bufferedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto bufferedVecTypes = Deserialize(bufferedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, bufferedTypesChars);

    auto bufferedKeyExpsCnt = env->GetArrayLength(jEqualKeyExprs);
    std::string bufferedKeyExpsArr[bufferedKeyExpsCnt];
    GetExpressions(env, jEqualKeyExprs, bufferedKeyExpsArr, bufferedKeyExpsCnt);

    auto bufferedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    auto bufferedOutputColsCnt = env->GetArrayLength(jOutputChannels);

    Parser parser;
    std::vector<omniruntime::expressions::Expr *> bufferedKeysArrExprs =
        parser.ParseExpressions(bufferedKeyExpsArr, bufferedKeyExpsCnt, bufferedVecTypes);

    JNI_DEBUG_LOG("before create buffered table with expression operator factory elapsed time: %ld ms.", END(start));
    BufferedTableWithExprOperatorFactory *operatorFactory =
        BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(bufferedVecTypes,
        bufferedKeysArrExprs, bufferedKeyExpsCnt, bufferedOutputCols, bufferedOutputColsCnt,
        jSmjStreamedTableWithExprOperatorFactory);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create buffered table operator with expression factory finished, elapsed time: %ld ms.", END(start));

    return (int64_t)operatorFactory;
}
