/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "jni_operator_factory.h"
#include "operator/operator_factory.h"
#include "jit_context/jit_context.h"
#include "operator/sort/sort.h"
#include "operator/sort/sort_expr.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/filter/filter_and_project.h"
#include "operator/window/window.h"
#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/hash_builder_expr.h"
#include "operator/join/lookup_join_expr.h"
#include "operator/join/sortmergejoin/sort_merge_join_expr.h"
#include "operator/topn/topn.h"
#include "operator/topn/topn_expr.h"
#include "operator/partitionedoutput/partitionedoutput.h"
#include "operator/union/union.h"
#include "operator/window/window_expr.h"
#include "operator/limit/limit.h"
#include "operator/limit/distinct_limit.h"
#include "operator/config/operator_config.h"
#include "config.h"
#include "jni_common_def.h"
#include "expression/expr_verifier.h"

using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;

void GetColumnsFromExpressions(JNIEnv *env, jobjectArray &jExpressions, int32_t *columns, int32_t length)
{
    for (int32_t i = 0; i < length; i++) {
        auto jSortCol = static_cast<jstring>(env->GetObjectArrayElement(jExpressions, i));
        const char *columnString = env->GetStringUTFChars(jSortCol, JNI_FALSE);
        columns[i] = std::stoi(columnString + 1);
        env->ReleaseStringUTFChars(jSortCol, columnString);
    }
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

void GetExprsFromJson(const string *keysArr, jint keyCount, vector<omniruntime::expressions::Expr *> &expressions)
{
    for (int32_t i = 0; i < keyCount; i++) {
        auto jsonExpression = nlohmann::json::parse(keysArr[i]);
        auto expression = JSONParser::ParseJSON(jsonExpression);
        if (expression == nullptr) {
            Expr::DeleteExprs(expressions);
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                "The expression is not supported yet.");
        }
        expressions.push_back(expression);
    }
}

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
    auto operatorFactory = (OperatorFactory *)jNativeFactoryObj;
    omniruntime::op::Operator *nativeOperator = nullptr;

    JNI_METHOD_START
#if defined(DEBUG_OPERATOR)
    nativeOperator = operatorFactory->CreateOperator();
    JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
#else
    auto jitContext = operatorFactory->GetJitContext();
    if (jitContext == nullptr) {
        nativeOperator = operatorFactory->CreateOperator();
        JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
    } else {
        auto opModule = reinterpret_cast<omniruntime::op::OptModule>(jitContext->func);
        nativeOperator = opModule(operatorFactory);
        JNI_DEBUG_LOG("JIT create omni operator finished, elapsed time: %ld ms.", END(start));
    }
#endif
    JNI_METHOD_END(0L)

    auto vectorAllocator = reinterpret_cast<VectorAllocator *>(jNativeVecAllocatorObj);
    nativeOperator->SetVecAllocator(vectorAllocator);
    return reinterpret_cast<int64_t>(nativeOperator);
}

/**
 * Return an HashAggregationFactory object address.
 *                                                                                 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationJitContext(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jintArray jMaskCols, jstring jOutPutTye, jboolean inputRaw,
    jboolean outputPartial)
{
    auto groupByTypesCharPtr = env->GetStringUTFChars(jGroupByType, JNI_FALSE);
    auto groupByDataTypes = Deserialize(groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    auto aggFuncsCount = env->GetArrayLength(jAggFuncType);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateHashAggregationJitContext(groupByDataTypes, aggFuncsCount);
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
}

/**
 * Return an HashAggregationFactory object address.
 *                                                                                 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jintArray jMaskCols, jstring jOutPutTye, jboolean inputRaw,
    jboolean outputPartial, jlong jitContext)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and id
    auto groupByNum = static_cast<size_t>(env->GetArrayLength(jGroupByChannel));
    int32_t groupByCols[groupByNum];
    GetColumnsFromExpressions(env, jGroupByChannel, groupByCols, static_cast<int32_t>(groupByNum));
    auto groupByTypesCharPtr = env->GetStringUTFChars(jGroupByType, JNI_FALSE);
    auto aggInputChannelNum = static_cast<size_t>(env->GetArrayLength(jAggChannel));
    int32_t aggCols[aggInputChannelNum];
    GetColumnsFromExpressions(env, jAggChannel, aggCols, static_cast<int32_t>(aggInputChannelNum));
    auto aggTypesCharPtr = env->GetStringUTFChars(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    jint *maskColumns = env->GetIntArrayElements(jMaskCols, JNI_FALSE);
    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);

    auto groupByDataTypes = Deserialize(groupByTypesCharPtr);
    auto aggDataTypes = Deserialize(aggTypesCharPtr);
    auto outDataTypes = Deserialize(outTypesCharPtr);
    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    auto aggNum = static_cast<size_t>(env->GetArrayLength(jAggFuncType));

    vector<uint32_t> groupByColVector = vector<uint32_t>((uint32_t *)groupByCols, (uint32_t *)groupByCols + groupByNum);
    vector<uint32_t> aggColVector = vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + aggInputChannelNum);
    vector<uint32_t> aggFuncTypeVector = vector<uint32_t>((uint32_t *)aggFuncTypes, (uint32_t *)aggFuncTypes + aggNum);
    vector<uint32_t> maskColumnVector = vector<uint32_t>((uint32_t *)maskColumns, (uint32_t *)maskColumns + aggNum);

    HashAggregationOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColVector, groupByDataTypes, aggColVector,
        aggDataTypes, outDataTypes, aggFuncTypeVector, maskColumnVector, inputRaw, outputPartial);
    JNI_METHOD_END(0L)
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    nativeOperatorFactory->Init();
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(nativeOperatorFactory);
}

/**
 * Return an AggregationFactory object address.
 *                                                                                 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jAggFuncTypes, jintArray jAggInputCols, jintArray jMaskCols,
    jstring jAggOutputTypes, jboolean inputRaw, jboolean outputPartial)
{
    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateAggregationJitContext();
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
}

/**
 * Return an AggregationFactory object address.
 *                                                                                  */
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

    auto aggInputColsCount = static_cast<size_t>(env->GetArrayLength(jAggInputCols));
    auto aggCount = static_cast<size_t>(aggOutputTypes.GetSize());

    std::vector<uint32_t> aggInputColsVector =
        vector<uint32_t>((uint32_t *)aggInputCols, (uint32_t *)aggInputCols + aggInputColsCount);
    std::vector<uint32_t> maskColsVector = vector<uint32_t>((uint32_t *)maskCols, (uint32_t *)maskCols + aggCount);
    std::vector<uint32_t> aggFuncTypesVector =
        vector<uint32_t>((uint32_t *)aggFuncTypes, (uint32_t *)aggFuncTypes + aggCount);

    AggregationOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesVector, aggInputColsVector,
        maskColsVector, aggOutputTypes, inputRaw, outputPartial);
    JNI_METHOD_END(0L)
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    nativeOperatorFactory->Init();

    return reinterpret_cast<int64_t>(nativeOperatorFactory);
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

    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    auto outputColsCount = env->GetArrayLength(jOutputCols);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateSortJitContext(sourceDataTypes, outputColsArr, outputColsCount, sortColsArr, ascendingsArr,
        nullFirstsArr, sortColsCount);
    JNI_METHOD_END(0L)

    return reinterpret_cast<int64_t>(jitContext);
}

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[IJLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortCols,
    jintArray jAscendings, jintArray jNullFirsts, jlong jitContext, jstring jOperatorConfig)
{
    JNI_DEBUG_LOG("create sort operator factory starting.");
    auto start = START();
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    auto outputColsCount = env->GetArrayLength(jOutputCols);
    auto sortColsCount = env->GetArrayLength(jSortCols);
    int32_t sortColsArr[sortColsCount];
    GetColumnsFromExpressions(env, jSortCols, sortColsArr, sortColsCount);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    SortOperatorFactory *sortOperatorFactory = nullptr;
    JNI_METHOD_START
    JNI_DEBUG_LOG("before create sort operator factory elapsed time: %ld ms.", END(start));
    sortOperatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceDataTypes, outputColsArr,
        outputColsCount, sortColsArr, ascendingsArr, nullFirstsArr, sortColsCount, operatorConfig);
    sortOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));
    JNI_METHOD_END(0L)

    return reinterpret_cast<int64_t>(sortOperatorFactory);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowJitContext(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputDataTypes = Deserialize(sourceTypesCharPtr);
    auto outputDataTypes = Deserialize(windowFunctionReturnTypeCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);

    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);

    std::vector<DataType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), inputDataTypes.Get().begin(), inputDataTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputDataTypes.Get().begin(), outputDataTypes.Get().end());
    DataTypes allTypes(allTypesVec);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateWindowJitContext(inputDataTypes, outputChannels, outputColsCount, partitionChannels,
        partitionCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, const_cast<int32_t *>(allTypes.GetIds()),
        allTypes.GetSize());
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels,
    jlong jitContext)
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
    jint *windowFrameTypes = env->GetIntArrayElements(jWindowFrameTypes, JNI_FALSE);
    jint *windowFrameStartTypes = env->GetIntArrayElements(jWindowFrameStartTypes, JNI_FALSE);
    jint *windowFrameStartChannels = env->GetIntArrayElements(jWindowFrameStartChannels, JNI_FALSE);
    jint *windowFrameEndTypes = env->GetIntArrayElements(jWindowFrameEndTypes, JNI_FALSE);
    jint *windowFrameEndChannels = env->GetIntArrayElements(jWindowFrameEndChannels, JNI_FALSE);

    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputDataTypes = Deserialize(sourceTypesCharPtr);
    auto outputDataTypes = Deserialize(windowFunctionReturnTypeCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);

    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentChannelsCount = env->GetArrayLength(jArgumentChannels);

    std::vector<DataType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), inputDataTypes.Get().begin(), inputDataTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputDataTypes.Get().begin(), outputDataTypes.Get().end());

    DataTypes allTypes(allTypesVec);

    WindowOperatorFactory *windowOperatorFactory = nullptr;
    JNI_METHOD_START
    windowOperatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(inputDataTypes, outputChannels,
        outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount, preGroupedChannels,
        preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, allTypes, argumentChannels, argumentChannelsCount, windowFrameTypes, windowFrameStartTypes,
        windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    JNI_METHOD_END(0L)
    windowOperatorFactory->Init();
    windowOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    return reinterpret_cast<int64_t>(windowOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNJitContext(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortColCount = env->GetArrayLength(jSortCols);
    int32_t sortCols[sortColCount];
    GetColumnsFromExpressions(env, jSortCols, sortCols, sortColCount);

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateTopNJitContext(sourceTypes, sortCols, sortColCount);
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
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

    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    TopNOperatorFactory *topNOperatorFactory = nullptr;
    JNI_METHOD_START
    topNOperatorFactory = new TopNOperatorFactory(sourceTypes, jN, sortColsArr, sortAsc, sortNullFirsts, sortColCount);
    JNI_METHOD_END(0L)
    topNOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    return reinterpret_cast<int64_t>(topNOperatorFactory);
}

static bool CheckExpressionSupported(bool skipVerify, Expr *filterExpr)
{
    if (!skipVerify) {
        ExprVerifier verifier;
        if (!verifier.VisitExpr(*filterExpr)) {
#ifdef DEBUG
            std::cout << "The filter expression is not supported: " << std::endl;
            ExprPrinter p;
            filterExpr->Accept(p);
            std::cout << std::endl;
#endif
            LogWarn("Verifier failed");
            return false;
        }
    }
    return true;
}

static bool CheckExpressionsSupported(bool skipVerify, const std::vector<Expr *> &projectExprs)
{
    if (!skipVerify) {
        auto exprSize = projectExprs.size();
        ExprVerifier verifier;
        for (size_t i = 0; i < exprSize; i++) {
            if (!verifier.VisitExpr(*projectExprs[i])) {
#ifdef DEBUG
                std::cout << "The " << i << "-th project expression is not supported: " << std::endl;
                ExprPrinter p;
                projectExprs[i]->Accept(p);
                std::cout << std::endl;
#endif
                LogWarn("Verifier failed");
                return false;
            }
        }
    }
    return true;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jInputTypes, jint jInputLength, jstring jExpression, jobjectArray jProjections,
    jint jProjectLength, jlong jitContext, jint jParseFormat, jboolean jIsSkipVerify)
{
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputDataTypes = Deserialize(inputTypesCharPtr);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    env->ReleaseStringUTFChars(jExpression, expressionCharPtr);
    auto inputLength = (int32_t)jInputLength;

    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string projectExpressions[jProjectLength];
    GetExpressions(env, jProjections, projectExpressions, jProjectLength);

    std::vector<omniruntime::expressions::Expr *> projectExprs;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    if (parseFormat == JSON) {
        auto filterJsonExpr = nlohmann::json::parse(filterExpression);
        filterExpr = JSONParser::ParseJSON(filterJsonExpr);
        nlohmann::json jsonProjectExprs[jProjectLength];
        for (int32_t i = 0; i < jProjectLength; i++) {
            jsonProjectExprs[i] = nlohmann::json::parse(projectExpressions[i]);
        }
        projectExprs = JSONParser::ParseJSON(jsonProjectExprs, jProjectLength);
    } else {
        Parser parser;
        filterExpr = parser.ParseRowExpression(filterExpression, inputDataTypes, inputLength);
        projectExprs = parser.ParseExpressions(projectExpressions, jProjectLength, inputDataTypes);
    }
    if (filterExpr == nullptr || (projectExprs.size() != static_cast<size_t>(jProjectLength))) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    if (!CheckExpressionSupported(jIsSkipVerify, filterExpr)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }
    if (!CheckExpressionsSupported(jIsSkipVerify, projectExprs)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    FilterAndProjectOperatorFactory *factory = nullptr;
    try {
        factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputDataTypes, inputLength, projectExprs, jProjectLength);
    } catch (const std::exception &e) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        env->ThrowNew(omniRuntimeExceptionClass, e.what());
        return 0L;
    }

    if (!factory->isSupportedExpr) {
        delete factory;
        return 0;
    }

    return reinterpret_cast<int64_t>(factory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *env,
    jclass jobj, jstring jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength, jlong jitContext,
    jint jParseFormat, jboolean jIsSkipVerify)
{
    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string exprs[jExprsLength];
    GetExpressions(env, jExprs, exprs, jExprsLength);

    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputDataTypes = Deserialize(inputTypesCharPtr);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    auto inputLength = static_cast<int32_t>(jInputLength);

    std::vector<omniruntime::expressions::Expr *> expressions;
    if (parseFormat == JSON) {
        nlohmann::json jsonExprs[jExprsLength];
        for (int32_t i = 0; i < jExprsLength; i++) {
            jsonExprs[i] = nlohmann::json::parse(exprs[i]);
        }
        expressions = JSONParser::ParseJSON(jsonExprs, jExprsLength);
    } else {
        Parser parser;
        expressions = parser.ParseExpressions(exprs, jExprsLength, inputDataTypes);
    }
    if (expressions.size() != static_cast<size_t>(jExprsLength)) {
        Expr::DeleteExprs(expressions);
        return 0;
    }

    if (!CheckExpressionsSupported(jIsSkipVerify, expressions)) {
        Expr::DeleteExprs(expressions);
        return 0;
    }

    ProjectionOperatorFactory *factory = nullptr;
    JNI_METHOD_START
    factory = new ProjectionOperatorFactory(expressions, jExprsLength, inputDataTypes, inputLength);
    JNI_METHOD_END(0L)

    if (!factory->IsSupported()) {
        delete factory;
        return 0;
    }

    return reinterpret_cast<int64_t>(factory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderJitContext(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jintArray jBuildHashCols, jstring jFilterExpr, jint jSortChannel,
    jobjectArray jSearchExprs, jint jOperatorCount)
{
    auto buildTypesCharPtr = (env)->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    auto buildHashColsArr = env->GetIntArrayElements(jBuildHashCols, JNI_FALSE);

    auto buildDataTypes = Deserialize(buildTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateHashBuilderJitContext(buildDataTypes, buildHashColsArr, buildHashColsCount);
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
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

    auto buildDataTypes = Deserialize(buildTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    auto filterChars = env->GetStringUTFChars(jFilterExpr, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilterExpr, filterChars);

    JNI_DEBUG_LOG("before create hash builder operator factory elapsed time: %ld ms.", END(start));
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = nullptr;
    JNI_METHOD_START
    hashBuilderOperatorFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildDataTypes,
        buildHashColsArr, buildHashColsCount, filterExpression, jOperatorCount);
    JNI_METHOD_END(0L)
    hashBuilderOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(hashBuilderOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinJitContext(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jintArray jProbeHashCols, jintArray jBuildOutputCols,
    jstring jBuildOutputTypes, jint jJoinType)
{
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    auto probeHashColsArr = env->GetIntArrayElements(jProbeHashCols, JNI_FALSE);
    auto buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    auto probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeDataTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesCharPtr);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateLookupJoinJitContext(probeDataTypes, probeOutputColsCount, probeHashColsArr, probeHashColsCount,
        buildOutputDataTypes, buildOutputColsArr);
    JNI_METHOD_END(0L)
    return reinterpret_cast<int64_t>(jitContext);
}

omniruntime::expressions::Expr *CreateJoinFilterExpr(const std::string &filterString)
{
    omniruntime::expressions::Expr *filterExpr = nullptr;
    if (!filterString.empty()) {
        filterExpr = JSONParser::ParseJSON(nlohmann::json::parse(filterString));
        if (filterExpr == nullptr) {
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                "The expression is not supported yet.");
        }
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

    auto probeDataTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesCharPtr);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);

    JNI_METHOD_START
    // extract the expression and the BuildDataTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderOperatorFactory *>(jHashBuilderOperatorFactory)->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    auto filterExpr = CreateJoinFilterExpr(filterExpression);
    hashTables->SetFilterExpr(filterExpr);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create lookup join operator factory elapsed time: %ld ms.", END(start));
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = nullptr;
    JNI_METHOD_START
    lookupJoinOperatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(probeDataTypes,
        probeOutputColsArr, probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr,
        buildOutputDataTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory);
    JNI_METHOD_END(0L)
    lookupJoinOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create lookup join operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(lookupJoinOperatorFactory);
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

    auto sourceDataTypes = Deserialize(sourceTypesArrCharPtr);
    auto hashChannelDataTypes = Deserialize(bucketToPartitionArrPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesArrCharPtr);
    env->ReleaseStringUTFChars(jHashChannelTypes, bucketToPartitionArrPtr);

    jint sourceTypesCount = sourceDataTypes.GetSize();
    jint partitionChannelsCount = env->GetArrayLength(jPartitionChannels);
    jint bucketToPartitionCount = env->GetArrayLength(jBucketToPartition);
    jint hashChannelTypesCount = hashChannelDataTypes.GetSize();
    jint hashChannelCount = env->GetArrayLength(jHashChannels);

    auto hashChannelTypesArr = const_cast<int32_t *>(hashChannelDataTypes.GetIds());

    JNI_DEBUG_LOG("before create partitionedoutput operator factory elapsed time: %ld ms.", END(start));
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory = nullptr;
    JNI_METHOD_START
    partitionedOutputOperatorFactory = PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
        sourceDataTypes, sourceTypesCount, jReplicatesAnyRow, jNullChannel, partitionChannelsArr,
        partitionChannelsCount, jPartitionCount, bucketToPartitionArr, bucketToPartitionCount, isHashPrecomputed,
        hashChannelTypesArr, hashChannelTypesCount, hashChannels, hashChannelCount);
    JNI_METHOD_END(0L)
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    JNI_DEBUG_LOG("create partitionedoutput operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(partitionedOutputOperatorFactory);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jDistinct, jlong jitContext)
{
    JNI_DEBUG_LOG("create union operator factory starting.");
    auto start = START();
    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto sourcesTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    int32_t sourceTypesCount = sourcesTypes.GetSize();

    JNI_DEBUG_LOG("before create union operator factory elapsed time: %ld ms.", END(start));
    UnionOperatorFactory *unionOperatorFactory = nullptr;
    JNI_METHOD_START
    unionOperatorFactory = new UnionOperatorFactory(sourcesTypes, sourceTypesCount, jDistinct);
    JNI_METHOD_END(0L)
    unionOperatorFactory->SetJitContext(nullptr);
    JNI_DEBUG_LOG("create union operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(unionOperatorFactory);
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

    auto sourceDataTypes = Deserialize(sourceTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesChars);

    vector<omniruntime::expressions::Expr *> sortExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeysCount, sortExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateSortWithExprJitContext(sourceDataTypes, outputCols, outputColsCount, sortExprsArr, ascendings,
        nullFirsts);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, sortExprsArr)

    Expr::DeleteExprs(sortExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortKeys, jintArray jAscendings,
    jintArray jNullFirsts, jlong jitContext, jstring jOperatorConfig)
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

    auto sourceDataTypes = Deserialize(sourceTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesChars);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> sortKeyExprArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeysCount, sortKeyExprArr);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create sort with expression operator factory elapsed time: %ld ms.", END(start));
    SortWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceDataTypes, outputCols,
        outputColsCount, sortKeyExprArr, ascendings, nullFirsts, sortKeysCount, operatorConfig);
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_METHOD_END(0L)
    JNI_DEBUG_LOG("create sort with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(operatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashKeys, jstring jFilter, jint jOperatorCount)
{
    auto buildTypesChars = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashKeysCount = env->GetArrayLength(jBuildHashKeys);
    std::string buildHashKeysArr[buildHashKeysCount];
    GetExpressions(env, jBuildHashKeys, buildHashKeysArr, buildHashKeysCount);
    auto buildDataTypes = Deserialize(buildTypesChars);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesChars);

    vector<omniruntime::expressions::Expr *> buildHashExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(buildHashKeysArr, buildHashKeysCount, buildHashExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateHashBuilderWithExprJitContext(buildDataTypes, buildHashExprsArr);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, buildHashExprsArr)

    Expr::DeleteExprs(buildHashExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
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
    auto buildDataTypes = Deserialize(buildTypesChars);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesChars);

    auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilter, filterChars);

    vector<omniruntime::expressions::Expr *> buildHashKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(buildHashKeysArr, buildHashKeysCount, buildHashKeysArrExprs);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create hash builder with expression operator factory elapsed time: %ld ms.", END(start));
    HashBuilderWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildDataTypes,
        buildHashKeysArrExprs, buildHashKeysCount, filterExpression, jHashTableCount);
    JNI_METHOD_END(0L)
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hash builder with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(operatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashKeys,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType)
{
    auto probeTypesChars = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeHashKeysCount = env->GetArrayLength(jProbeHashKeys);
    std::string probeHashKeysArr[probeHashKeysCount];
    GetExpressions(env, jProbeHashKeys, probeHashKeysArr, probeHashKeysCount);
    auto buildOutputCols = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesChars = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeDataTypes = Deserialize(probeTypesChars);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    vector<omniruntime::expressions::Expr *> probeHashExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(probeHashKeysArr, probeHashKeysCount, probeHashExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateLookupJoinWithExprJitContext(probeDataTypes, probeOutputColsCount, probeHashExprsArr,
        buildOutputDataTypes, buildOutputCols);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, probeHashExprsArr)

    Expr::DeleteExprs(probeHashExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
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

    auto probeDataTypes = Deserialize(probeTypesChars);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    // extract the expression and the BuildDataTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderWithExprOperatorFactory *>(jHashBuilderOperatorFactory)
                          ->GetHashBuilderOperatorFactory()
                          ->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    JNI_METHOD_START
    auto filterExpr = CreateJoinFilterExpr(filterExpression);
    hashTables->SetFilterExpr(filterExpr);
    JNI_METHOD_END(0L)

    vector<omniruntime::expressions::Expr *> probeHashKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(probeHashKeysArr, probeHashKeysCount, probeHashKeysArrExprs);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create lookup join with expression operator factory elapsed time: %ld ms.", END(start));
    LookupJoinWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(probeDataTypes,
        probeOutputCols, probeOutputColsCount, probeHashKeysArrExprs, probeHashKeysCount, buildOutputCols,
        buildOutputDataTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory);
    JNI_METHOD_END(0L)

    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create lookup join with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(operatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprJitContext(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jobjectArray jArgumentKeys,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    auto argumentKeysCount = env->GetArrayLength(jArgumentKeys);
    std::string argumentKeysArr[argumentKeysCount];
    GetExpressions(env, jArgumentKeys, argumentKeysArr, argumentKeysCount);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputDataTypes = Deserialize(sourceTypesCharPtr);
    auto outputDataTypes = Deserialize(windowFunctionReturnTypeCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);

    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);

    vector<omniruntime::expressions::Expr *> argumentExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(argumentKeysArr, argumentKeysCount, argumentExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateWindowWithExprJitContext(inputDataTypes, outputChannels, outputColsCount, partitionChannels,
        partitionCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, outputDataTypes, argumentExprsArr);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, argumentExprsArr)

    Expr::DeleteExprs(argumentExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jobjectArray jArgumentKeys,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels,
    jlong jitContext)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *outputChannels = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    jint *windowFunction = env->GetIntArrayElements(jWindowFunction, JNI_FALSE);
    jint *partitionChannels = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *preGroupedChannels = env->GetIntArrayElements(JPreGroupedChannels, JNI_FALSE);
    jint *sortChannels = env->GetIntArrayElements(jSortChannels, JNI_FALSE);
    jint *sortOrder = env->GetIntArrayElements(jSortOrder, JNI_FALSE);
    jint *sortNullFirsts = env->GetIntArrayElements(jSortNullFirsts, JNI_FALSE);
    jint *windowFrameTypes = env->GetIntArrayElements(jWindowFrameTypes, JNI_FALSE);
    jint *windowFrameStartTypes = env->GetIntArrayElements(jWindowFrameStartTypes, JNI_FALSE);
    jint *windowFrameStartChannels = env->GetIntArrayElements(jWindowFrameStartChannels, JNI_FALSE);
    jint *windowFrameEndTypes = env->GetIntArrayElements(jWindowFrameEndTypes, JNI_FALSE);
    jint *windowFrameEndChannels = env->GetIntArrayElements(jWindowFrameEndChannels, JNI_FALSE);

    auto argumentKeysArrCount = env->GetArrayLength(jArgumentKeys);
    std::string argumentKeysArr[argumentKeysArrCount];
    GetExpressions(env, jArgumentKeys, argumentKeysArr, argumentKeysArrCount);
    auto windowFunctionReturnTypeCharPtr = env->GetStringUTFChars(jWindowFunctionReturnType, JNI_FALSE);

    auto inputDataTypes = Deserialize(sourceTypesCharPtr);
    auto outputDataTypes = Deserialize(windowFunctionReturnTypeCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);

    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentKeysCount = env->GetArrayLength(jArgumentKeys);

    vector<omniruntime::expressions::Expr *> argumentKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(argumentKeysArr, argumentKeysCount, argumentKeysArrExprs);
    JNI_METHOD_END(0L)

    WindowWithExprOperatorFactory *windowWithExprOperatorFactory = nullptr;
    JNI_METHOD_START
    windowWithExprOperatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(inputDataTypes,
        outputChannels, outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount,
        preGroupedChannels, preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount,
        preSortedChannelPrefix, expectedPositions, outputDataTypes, argumentKeysArrExprs, argumentKeysCount,
        windowFrameTypes, windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    JNI_METHOD_END(0L)
    windowWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    return reinterpret_cast<int64_t>(windowWithExprOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprJitContext(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannel, jstring jSourceType,
    jintArray jAggFuncType, jintArray jMaskCols, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial)
{
    auto groupByNum = (int32_t)env->GetArrayLength(jGroupByChannel);
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);

    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);
    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    auto aggFuncsCount = env->GetArrayLength(jAggFuncType);

    vector<omniruntime::expressions::Expr *> groupByExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(groupByKeys, groupByNum, groupByExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateHashAggregationWithExprJitContext(groupByExprsArr, aggFuncsCount);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, groupByExprsArr)

    Expr::DeleteExprs(groupByExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannel, jstring jSourceType,
    jintArray jAggFuncType, jintArray jMaskCols, jstring jOutPutTye, jboolean inputRaw, jboolean outputPartial,
    jlong jitContext)
{
    JNI_DEBUG_LOG("create hashagg operator factory starting.");
    auto start = START();
    // groupby channel and id
    auto groupByNum = static_cast<int32_t>(env->GetArrayLength(jGroupByChannel));
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);
    auto aggNum = static_cast<int32_t>(env->GetArrayLength(jAggChannel));
    std::string aggKeys[aggNum];
    GetExpressions(env, jAggChannel, aggKeys, aggNum);

    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);
    jint *maskColumns = env->GetIntArrayElements(jMaskCols, JNI_FALSE);

    auto outTypesCharPtr = env->GetStringUTFChars(jOutPutTye, JNI_FALSE);
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);

    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    auto outDataTypes = Deserialize(outTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceType, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    vector<omniruntime::expressions::Expr *> groupByKeysExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(groupByKeys, groupByNum, groupByKeysExprs);
    JNI_METHOD_END(0L)

    vector<omniruntime::expressions::Expr *> aggKeysExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(aggKeys, aggNum, aggKeysExprs);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, groupByKeysExprs)

    HashAggregationWithExprOperatorFactory *nativeOperatorFactory = nullptr;
    try {
        nativeOperatorFactory =
            new HashAggregationWithExprOperatorFactory(groupByKeysExprs, groupByNum, aggKeysExprs, aggNum,
            sourceDataTypes, outDataTypes, (uint32_t *)aggFuncTypes, (uint32_t *)maskColumns, inputRaw, outputPartial);
    } catch (const std::exception &e) {
        Expr::DeleteExprs(groupByKeysExprs);
        Expr::DeleteExprs(aggKeysExprs);
        env->ThrowNew(omniRuntimeExceptionClass, e.what());
        return 0L;
    }
    nativeOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(nativeOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc, jintArray jSortNullFirsts)
{
    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint sortKeysCount = env->GetArrayLength(jSortKeys);

    std::string sortKeysArr[sortKeysCount];
    GetExpressions(env, jSortKeys, sortKeysArr, sortKeysCount);

    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    vector<omniruntime::expressions::Expr *> sortExprsArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeysCount, sortExprsArr);
    JNI_METHOD_END(0L)

    JitContext *jitContext = nullptr;
    JNI_METHOD_START
    jitContext = CreateTopNWithExprJitContext(sourceDataTypes, sortExprsArr);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, sortExprsArr)

    Expr::DeleteExprs(sortExprsArr);
    return reinterpret_cast<int64_t>(jitContext);
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
    auto n = (int32_t)jN;
    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    vector<omniruntime::expressions::Expr *> sortKeyExprArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeyCount, sortKeyExprArr);
    JNI_METHOD_END(0L)

    TopNWithExprOperatorFactory *topNWithExprOperatorFactory = nullptr;
    JNI_METHOD_START
    topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceDataTypes, n, sortKeyExprArr, sortAsc, sortNullFirsts, sortKeyCount);
    JNI_METHOD_END(0L)
    topNWithExprOperatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));

    return reinterpret_cast<int64_t>(topNWithExprOperatorFactory);
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_closeNativeOperatorFactory(JNIEnv *env,
    jclass jclz, jlong jNativeOperatorFactory)
{
    auto nativeOperatorFactory = reinterpret_cast<OperatorFactory *>(jNativeOperatorFactory);
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
    LimitOperatorFactory *limitOperatorFactory = nullptr;
    JNI_METHOD_START
    limitOperatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(jLimit);
    JNI_METHOD_END(0L)
    limitOperatorFactory->SetJitContext(nullptr);
    JNI_DEBUG_LOG("create limit operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(limitOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_limit_OmniDistinctLimitOperatorFactory_createDistinctLimitOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSoureTypes, jintArray jDistinctChannel, jint jHashChannel, jlong jLimit)
{
    JNI_DEBUG_LOG("create distinct limit operator factory starting.");
    auto start = START();

    auto distinctColCount = (int32_t)env->GetArrayLength(jDistinctChannel);
    jint *distinctCols = env->GetIntArrayElements(jDistinctChannel, JNI_FALSE);

    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSoureTypes, JNI_FALSE);
    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSoureTypes, sourceTypesCharPtr);

    JNI_DEBUG_LOG("before create distinct limit operator factory elapsed time: %ld ms.", END(start));
    DistinctLimitOperatorFactory *distinctLimitOperatorFactory = nullptr;
    JNI_METHOD_START
    distinctLimitOperatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes,
        distinctCols, distinctColCount, jHashChannel, jLimit);
    JNI_METHOD_END(0L)
    distinctLimitOperatorFactory->SetJitContext(nullptr);
    JNI_DEBUG_LOG("create distinct limit operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(distinctLimitOperatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjStreamedTableWithExprOperatorFactory_createSmjStreamedTableWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jint jJoinType, jstring jFilter)
{
    return 0L;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjStreamedTableWithExprOperatorFactory_createSmjStreamedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jint jJoinType, jstring jFilter, jlong jitContext)
{
    if ((JoinType)jJoinType != JoinType::OMNI_JOIN_TYPE_INNER) {
        return 0L;
    }

    JNI_DEBUG_LOG("create streamed table with expression operator factory starting.");
    auto start = START();

    auto streamedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto streamedDataTypes = Deserialize(streamedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, streamedTypesChars);

    auto streamedKeyExpsCount = env->GetArrayLength(jEqualKeyExprs);
    std::string streamedKeyExpsArr[streamedKeyExpsCount];
    GetExpressions(env, jEqualKeyExprs, streamedKeyExpsArr, streamedKeyExpsCount);

    auto streamedOutputColsCnt = env->GetArrayLength(jOutputChannels);
    auto streamedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);

    std::string filterExpression;
    if (jFilter == nullptr) {
        filterExpression = "";
    } else {
        auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
        filterExpression = std::string(filterChars);
        env->ReleaseStringUTFChars(jFilter, filterChars);
    }

    vector<omniruntime::expressions::Expr *> streamedKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(streamedKeyExpsArr, streamedKeyExpsCount, streamedKeysArrExprs);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create streamed table with expression operator factory elapsed time: %ld ms.", END(start));
    StreamedTableWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(
        streamedDataTypes, streamedKeysArrExprs, streamedKeyExpsCount, streamedOutputCols, streamedOutputColsCnt,
        (JoinType)jJoinType, filterExpression);
    JNI_METHOD_END(0L)
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create streamed table with expression operator factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(operatorFactory);
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjBufferedTableWithExprOperatorFactory_createSmjBufferedTableWithExprJitContext(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels)
{
    return 0L;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjBufferedTableWithExprOperatorFactory_createSmjBufferedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jlong jSmjStreamedTableWithExprOperatorFactory, jlong jitContext)
{
    JNI_DEBUG_LOG("create buffered table with expression operator factory starting.");
    auto start = START();

    auto bufferedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto bufferedDataTypes = Deserialize(bufferedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, bufferedTypesChars);

    auto bufferedKeyExpsCnt = env->GetArrayLength(jEqualKeyExprs);
    std::string bufferedKeyExpsArr[bufferedKeyExpsCnt];
    GetExpressions(env, jEqualKeyExprs, bufferedKeyExpsArr, bufferedKeyExpsCnt);

    auto bufferedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    auto bufferedOutputColsCnt = env->GetArrayLength(jOutputChannels);

    vector<omniruntime::expressions::Expr *> bufferedKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(bufferedKeyExpsArr, bufferedKeyExpsCnt, bufferedKeysArrExprs);
    JNI_METHOD_END(0L)

    JNI_DEBUG_LOG("before create buffered table with expression operator factory elapsed time: %ld ms.", END(start));
    BufferedTableWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(
        bufferedDataTypes, bufferedKeysArrExprs, bufferedKeyExpsCnt, bufferedOutputCols, bufferedOutputColsCnt,
        jSmjStreamedTableWithExprOperatorFactory);
    JNI_METHOD_END(0L)
    operatorFactory->SetJitContext(reinterpret_cast<JitContext *>(jitContext));
    JNI_DEBUG_LOG("create buffered table operator with expression factory finished, elapsed time: %ld ms.", END(start));

    return reinterpret_cast<int64_t>(operatorFactory);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniExprVerify_exprVerify(JNIEnv *env, jclass jObj,
    jstring jInputTypes, jint jInputLength, jstring jExpression, jobjectArray jProjections, jint jProjectLength,
    jint jParseFormat)
{
    omniruntime::expressions::Expr *filterExpr = nullptr;
    std::vector<omniruntime::expressions::Expr *> projectExprs;
    JNI_METHOD_START
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputDataTypes = Deserialize(inputTypesCharPtr);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    env->ReleaseStringUTFChars(jExpression, expressionCharPtr);
    auto inputLength = (int32_t)jInputLength;

    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string projectExpressions[jProjectLength];
    GetExpressions(env, jProjections, projectExpressions, jProjectLength);

    if (parseFormat == JSON) {
        if (!filterExpression.empty()) {
            auto filterJsonExpr = nlohmann::json::parse(filterExpression);
            filterExpr = JSONParser::ParseJSON(filterJsonExpr);
            if (filterExpr == nullptr) {
                LogWarn("The filter expression is not supported: %s", filterJsonExpr.dump(1).c_str());
                return 0;
            }
        }
        nlohmann::json jsonProjectExprs[jProjectLength];
        for (int32_t i = 0; i < jProjectLength; i++) {
            jsonProjectExprs[i] = nlohmann::json::parse(projectExpressions[i]);
        }
        projectExprs = JSONParser::ParseJSON(jsonProjectExprs, jProjectLength);
    } else {
        Parser parser;
        if (!filterExpression.empty()) {
            filterExpr = parser.ParseRowExpression(filterExpression, inputDataTypes, inputLength);
        }
        projectExprs = parser.ParseExpressions(projectExpressions, jProjectLength, inputDataTypes);
    }

    if ((!filterExpression.empty() && filterExpr == nullptr) ||
        (static_cast<size_t>(jProjectLength) != projectExprs.size())) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    if (filterExpr != nullptr && !CheckExpressionSupported(false, filterExpr)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }
    if (!CheckExpressionsSupported(false, projectExprs)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    delete filterExpr;
    Expr::DeleteExprs(projectExprs);
    return 1;
    JNI_METHOD_END_WITH_MULTI_EXPRS(0, std::vector<omniruntime::expressions::Expr *> { filterExpr }, projectExprs)
}