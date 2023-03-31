/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "jni_operator_factory.h"
#include "operator/operator_factory.h"
#include "operator/sort/sort.h"
#include "operator/sort/sort_expr.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/aggregation/non_group_aggregation_expr.h"
#include "operator/filter/filter_and_project.h"
#include "operator/filter/bloom_filter.h"
#include "operator/window/window.h"
#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/hash_builder_expr.h"
#include "operator/join/lookup_join_expr.h"
#include "operator/join/lookup_outer_join.h"
#include "operator/join/lookup_outer_join_expr.h"
#include "operator/join/sortmergejoin/sort_merge_join_expr.h"
#include "operator/topn/topn.h"
#include "operator/topn/topn_expr.h"
#include "operator/partitionedoutput/partitionedoutput.h"
#include "operator/union/union.h"
#include "operator/window/window_expr.h"
#include "operator/limit/limit.h"
#include "operator/limit/distinct_limit.h"
#include "operator/fusion/fusion_operator.h"
#include "operator/config/operator_config.h"
#include "util/config_util.h"
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
                "The expression is not supported yet: " + jsonExpression.dump());
        }
        expressions.push_back(expression);
    }
}

void GetExprsFromJson(vector<string> &keysArr, jint keyCount, vector<omniruntime::expressions::Expr *> &expressions)
{
    for (int32_t i = 0; i < keyCount; i++) {
        auto jsonExpression = nlohmann::json::parse(keysArr.at(i));
        auto expression = JSONParser::ParseJSON(jsonExpression);
        if (expression == nullptr) {
            Expr::DeleteExprs(expressions);
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                "The expression is not supported yet: " + jsonExpression.dump());
        }
        expressions.push_back(expression);
    }
}

void GetBoolVector(JNIEnv *env, jbooleanArray booleanArray, std::vector<bool> &output)
{
    auto length = static_cast<int32_t>(env->GetArrayLength(booleanArray));
    auto bools = env->GetBooleanArrayElements(booleanArray, JNI_FALSE);
    for (int32_t i = 0; i < length; i++) {
        output.push_back(bools[i]);
    }
}

void GetIntVector(JNIEnv *env, jintArray intArray, std::vector<uint32_t> &output)
{
    auto length = static_cast<int32_t>(env->GetArrayLength(intArray));
    auto ptr = env->GetIntArrayElements(intArray, JNI_FALSE);
    for (int32_t i = 0; i < length; i++) {
        output.push_back(*ptr);
        ptr++;
    }
}


void GetDataTypesVector(JNIEnv *env, jobjectArray jSourceType, std::vector<DataTypes> &output)
{
    auto len = static_cast<int32_t>(env->GetArrayLength(jSourceType));
    for (int i = 0; i < len; ++i) {
        auto str = static_cast<jstring>(env->GetObjectArrayElement(jSourceType, i));
        auto sourceTypesCharPtr = env->GetStringUTFChars(str, JNI_FALSE);
        auto dataTypes = Deserialize(sourceTypesCharPtr);
        env->ReleaseStringUTFChars(str, sourceTypesCharPtr);
        output.push_back(dataTypes);
    }
}

void DeserializeJsonToArray(const char *str, vector<string> &arr)
{
    auto result = nlohmann::json::parse(str);
    for (auto &json : result) {
        arr.push_back(json);
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
    auto operatorFactory = (OperatorFactory *)jNativeFactoryObj;
    omniruntime::op::Operator *nativeOperator = nullptr;

    JNI_METHOD_START
    nativeOperator = operatorFactory->CreateOperator();
    if (nativeOperator == nullptr) {
        throw omniruntime::exception::OmniException("CREATE_OPERATOR_FAILED",
            "return a null pointer when creating operator");
    }
    JNI_METHOD_END(0L)

    auto vectorAllocator = reinterpret_cast<VectorAllocator *>(jNativeVecAllocatorObj);
    nativeOperator->SetVecAllocator(vectorAllocator);
    return reinterpret_cast<intptr_t>(static_cast<void *>(nativeOperator));
}

/*
 * Return an HashAggregationFactory object address.
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jstring jGroupByType, jobjectArray jAggChannel,
    jstring jAggType, jintArray jAggFuncType, jintArray jMaskCols, jstring jOutPutTye, jboolean inputRaw,
    jboolean outputPartial, jstring jOperatorConfig)
{
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

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    auto aggNum = static_cast<size_t>(env->GetArrayLength(jAggFuncType));

    vector<uint32_t> groupByColVector = vector<uint32_t>((uint32_t *)groupByCols, (uint32_t *)groupByCols + groupByNum);
    vector<uint32_t> aggColVector = vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + aggInputChannelNum);
    vector<uint32_t> aggFuncTypeVector = vector<uint32_t>((uint32_t *)aggFuncTypes, (uint32_t *)aggFuncTypes + aggNum);
    vector<uint32_t> maskColumnVector = vector<uint32_t>((uint32_t *)maskColumns, (uint32_t *)maskColumns + aggNum);

    auto aggColVectorWrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggDataTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(outDataTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(inputRaw, aggFuncTypeVector.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(outputPartial, aggFuncTypeVector.size());

    HashAggregationOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColVector, groupByDataTypes, aggColVectorWrap,
        aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypeVector, maskColumnVector, inputRawsWrap, outputPartialsWrap,
        overflowConfig->IsOverflowAsNull());
    JNI_METHOD_END(0L)
    nativeOperatorFactory->Init();

    return reinterpret_cast<intptr_t>(static_cast<void *>(nativeOperatorFactory));
}

/*
 * Return an AggregationFactory object address.
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jintArray jAggFuncTypes, jintArray jAggInputCols,
    jintArray jMaskCols, jstring jAggOutputTypes, jboolean inputRaw, jboolean outputPartial)
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

    auto aggInputColsVectorWrap = AggregatorUtil::WrapWithVector(aggInputColsVector);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(inputRaw, aggFuncTypesVector.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(outputPartial, aggFuncTypesVector.size());

    AggregationOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesVector, aggInputColsVectorWrap,
        maskColsVector, aggOutputTypesWrap, inputRawWrap, outputPartialWrap);
    JNI_METHOD_END(0L)
    nativeOperatorFactory->Init();

    return reinterpret_cast<intptr_t>(static_cast<void *>(nativeOperatorFactory));
}

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[IJLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortCols,
    jintArray jAscendings, jintArray jNullFirsts, jstring jOperatorConfig)
{
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
    sortOperatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceDataTypes, outputColsArr,
        outputColsCount, sortColsArr, ascendingsArr, nullFirstsArr, sortColsCount, operatorConfig);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(sortOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels,
    jstring jOperatorConfig)
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

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    jint outputColsCount = env->GetArrayLength(jOutputChannels);
    jint windowFunctionCount = env->GetArrayLength(jWindowFunction);
    jint partitionCount = env->GetArrayLength(jPartitionChannels);
    jint preGroupedCount = env->GetArrayLength(JPreGroupedChannels);
    jint sortColCount = env->GetArrayLength(jSortChannels);
    jint argumentChannelsCount = env->GetArrayLength(jArgumentChannels);

    std::vector<DataTypePtr> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), inputDataTypes.Get().begin(), inputDataTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputDataTypes.Get().begin(), outputDataTypes.Get().end());

    DataTypes allTypes(allTypesVec);

    WindowOperatorFactory *windowOperatorFactory = nullptr;
    JNI_METHOD_START
    windowOperatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(inputDataTypes, outputChannels,
        outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount, preGroupedChannels,
        preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, allTypes, argumentChannels, argumentChannelsCount, windowFrameTypes, windowFrameStartTypes,
        windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, overflowConfig->IsOverflowAsNull());
    JNI_METHOD_END(0L)
    windowOperatorFactory->Init();

    return reinterpret_cast<intptr_t>(static_cast<void *>(windowOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts)
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

    return reinterpret_cast<intptr_t>(static_cast<void *>(topNOperatorFactory));
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
    jint jProjectLength, jint jParseFormat, jstring jOperatorConfig)
{
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputDataTypes = Deserialize(inputTypesCharPtr);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    env->ReleaseStringUTFChars(jExpression, expressionCharPtr);
    auto inputLength = (int32_t)jInputLength;

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    bool isSkipVerify = operatorConfig.IsSkipVerify();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string projectExpressions[jProjectLength];
    GetExpressions(env, jProjections, projectExpressions, jProjectLength);

    std::vector<omniruntime::expressions::Expr *> projectExprs;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    if (parseFormat == JSON) {
        JNI_METHOD_START
        auto filterJsonExpr = nlohmann::json::parse(filterExpression);
        filterExpr = JSONParser::ParseJSON(filterJsonExpr);
        JNI_METHOD_END(0L)
        JNI_METHOD_START
        nlohmann::json jsonProjectExprs[jProjectLength];
        for (int32_t i = 0; i < jProjectLength; i++) {
            jsonProjectExprs[i] = nlohmann::json::parse(projectExpressions[i]);
        }
        projectExprs = JSONParser::ParseJSON(jsonProjectExprs, jProjectLength);
        JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, { filterExpr })
    } else {
        Parser parser;
        JNI_METHOD_START
        filterExpr = parser.ParseRowExpression(filterExpression, inputDataTypes, inputLength);
        JNI_METHOD_END(0L)
        JNI_METHOD_START
        projectExprs = parser.ParseExpressions(projectExpressions, jProjectLength, inputDataTypes);
        JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, { filterExpr })
    }
    if (filterExpr == nullptr || (projectExprs.size() != static_cast<size_t>(jProjectLength))) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    if (!CheckExpressionSupported(isSkipVerify, filterExpr)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }
    if (!CheckExpressionsSupported(isSkipVerify, projectExprs)) {
        delete filterExpr;
        Expr::DeleteExprs(projectExprs);
        return 0;
    }

    FilterAndProjectOperatorFactory *factory = nullptr;
    auto exprEvaluator =
        std::make_shared<ExpressionEvaluator>(filterExpr, projectExprs, inputDataTypes, overflowConfig);
    if (!exprEvaluator->IsSupportedExpr()) {
        return 0;
    }

    factory = new FilterAndProjectOperatorFactory(std::move(exprEvaluator));

    return reinterpret_cast<intptr_t>(static_cast<void *>(factory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *env,
    jclass jobj, jstring jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength, jint jParseFormat,
    jstring jOperatorConfig)
{
    auto parseFormat = static_cast<ParserFormat>((int8_t)jParseFormat);
    std::string exprs[jExprsLength];
    GetExpressions(env, jExprs, exprs, jExprsLength);

    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputDataTypes = Deserialize(inputTypesCharPtr);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    bool isSkipVerify = operatorConfig.IsSkipVerify();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    std::vector<omniruntime::expressions::Expr *> expressions;
    JNI_METHOD_START
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
    JNI_METHOD_END(0L)
    if (expressions.size() != static_cast<size_t>(jExprsLength)) {
        Expr::DeleteExprs(expressions);
        return 0;
    }

    if (!CheckExpressionsSupported(isSkipVerify, expressions)) {
        Expr::DeleteExprs(expressions);
        return 0;
    }

    ProjectionOperatorFactory *factory = nullptr;
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(expressions, inputDataTypes, overflowConfig);
    if (!exprEvaluator->IsSupportedExpr()) {
        return 0;
    }

    factory = new ProjectionOperatorFactory(std::move(exprEvaluator));
    return reinterpret_cast<intptr_t>(static_cast<void *>(factory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jintArray jBuildHashCols, jstring jFilterExpr, jint jSortChannel,
    jobjectArray jSearchExprs, jint jOperatorCount)
{
    auto buildTypesCharPtr = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashColsCount = env->GetArrayLength(jBuildHashCols);
    auto buildHashColsArr = env->GetIntArrayElements(jBuildHashCols, JNI_FALSE);

    auto buildDataTypes = Deserialize(buildTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    auto filterChars = env->GetStringUTFChars(jFilterExpr, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilterExpr, filterChars);

    HashBuilderOperatorFactory *hashBuilderOperatorFactory = nullptr;
    JNI_METHOD_START
    hashBuilderOperatorFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildDataTypes,
        buildHashColsArr, buildHashColsCount, filterExpression, jOperatorCount);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(hashBuilderOperatorFactory));
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
    jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory, jstring jOperatorConfig)
{
    auto probeTypesCharPtr = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputColsArr = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashColsCount = env->GetArrayLength(jProbeHashCols);
    auto probeHashColsArr = env->GetIntArrayElements(jProbeHashCols, JNI_FALSE);
    auto buildOutputColsArr = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputColsCount = env->GetArrayLength(jBuildOutputCols);
    auto buildOutputTypesCharPtr = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    auto probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeDataTypes = Deserialize(probeTypesCharPtr);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesCharPtr);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    Expr *filterExpr = nullptr;
    JNI_METHOD_START
    // extract the expression and the BuildDataTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderOperatorFactory *>(jHashBuilderOperatorFactory)->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    filterExpr = CreateJoinFilterExpr(filterExpression);
    hashTables->SetFilterExpr(filterExpr);
    JNI_METHOD_END(0L)

    LookupJoinOperatorFactory *lookupJoinOperatorFactory = nullptr;
    JNI_METHOD_START
    lookupJoinOperatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(probeDataTypes,
        probeOutputColsArr, probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr,
        buildOutputColsCount, buildOutputDataTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, { filterExpr })
    Expr::DeleteExprs({ filterExpr });

    return reinterpret_cast<intptr_t>(static_cast<void *>(lookupJoinOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_partitionedoutput_OmniPartitionedOutPutOperatorFactory_createPartitionedOutputOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jReplicatesAnyRow, jint jNullChannel,
    jintArray jPartitionChannels, jint jPartitionCount, jintArray jBucketToPartition, jboolean isHashPrecomputed,
    jstring jHashChannelTypes, jintArray jHashChannels)
{
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
    jint hashChannelCount = env->GetArrayLength(jHashChannels);

    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory = nullptr;
    JNI_METHOD_START
    partitionedOutputOperatorFactory = PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
        sourceDataTypes, sourceTypesCount, jReplicatesAnyRow, jNullChannel, partitionChannelsArr,
        partitionChannelsCount, jPartitionCount, bucketToPartitionArr, bucketToPartitionCount, isHashPrecomputed,
        hashChannelDataTypes, hashChannels, hashChannelCount);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(partitionedOutputOperatorFactory));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jDistinct)
{
    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto sourcesTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    int32_t sourceTypesCount = sourcesTypes.GetSize();

    UnionOperatorFactory *unionOperatorFactory = nullptr;
    JNI_METHOD_START
    unionOperatorFactory = new UnionOperatorFactory(sourcesTypes, sourceTypesCount, jDistinct);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(unionOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortKeys, jintArray jAscendings,
    jintArray jNullFirsts, jstring jOperatorConfig)
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

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> sortKeyExprArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeysCount, sortKeyExprArr);
    JNI_METHOD_END(0L)

    SortWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceDataTypes, outputCols,
        outputColsCount, sortKeyExprArr, ascendings, nullFirsts, sortKeysCount, operatorConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, sortKeyExprArr)
    Expr::DeleteExprs(sortKeyExprArr);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashKeys, jstring jFilter, jint jHashTableCount,
    jstring jOperatorConfig)
{
    auto buildTypesChars = env->GetStringUTFChars(jBuildTypes, JNI_FALSE);
    auto buildHashKeysCount = env->GetArrayLength(jBuildHashKeys);
    std::string buildHashKeysArr[buildHashKeysCount];
    GetExpressions(env, jBuildHashKeys, buildHashKeysArr, buildHashKeysCount);
    auto buildDataTypes = Deserialize(buildTypesChars);
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesChars);

    auto filterChars = env->GetStringUTFChars(jFilter, JNI_FALSE);
    std::string filterExpression = std::string(filterChars);
    env->ReleaseStringUTFChars(jFilter, filterChars);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> buildHashKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(buildHashKeysArr, buildHashKeysCount, buildHashKeysArrExprs);
    JNI_METHOD_END(0L)

    HashBuilderWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildDataTypes,
        buildHashKeysArrExprs, buildHashKeysCount, filterExpression, jHashTableCount, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, buildHashKeysArrExprs)
    Expr::DeleteExprs(buildHashKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashKeys,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory,
    jstring jOperatorConfig)
{
    auto probeTypesChars = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputCols = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto probeHashKeysCount = env->GetArrayLength(jProbeHashKeys);
    std::string probeHashKeysArr[probeHashKeysCount];
    GetExpressions(env, jProbeHashKeys, probeHashKeysArr, probeHashKeysCount);
    auto buildOutputCols = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputColsCount = env->GetArrayLength(jBuildOutputCols);
    auto buildOutputTypesChars = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeDataTypes = Deserialize(probeTypesChars);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    // extract the expression and the BuildDataTypes to parse the expression
    auto hashTables = reinterpret_cast<HashBuilderWithExprOperatorFactory *>(jHashBuilderOperatorFactory)
                          ->GetHashBuilderOperatorFactory()
                          ->GetHashTables();
    std::string filterExpression = hashTables->GetFilterExpression();
    Expr *filterExpr = nullptr;
    JNI_METHOD_START
    filterExpr = CreateJoinFilterExpr(filterExpression);
    hashTables->SetFilterExpr(filterExpr);
    JNI_METHOD_END(0L)

    vector<omniruntime::expressions::Expr *> probeHashKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(probeHashKeysArr, probeHashKeysCount, probeHashKeysArrExprs);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, { filterExpr })

    LookupJoinWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(probeDataTypes,
        probeOutputCols, probeOutputColsCount, probeHashKeysArrExprs, probeHashKeysCount, buildOutputCols,
        buildOutputColsCount, buildOutputDataTypes, (JoinType)jJoinType, jHashBuilderOperatorFactory, overflowConfig);
    JNI_METHOD_END_WITH_MULTI_EXPRS(0L, { filterExpr }, probeHashKeysArrExprs)
    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(probeHashKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupOuterJoinWithExprOperatorFactory_createLookupOuterJoinWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashKeys,
    jintArray jBuildOutputCols, jstring jBuildOutputTypes, jlong jHashBuilderOperatorFactory)
{
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

    vector<omniruntime::expressions::Expr *> probeHashKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(probeHashKeysArr, probeHashKeysCount, probeHashKeysArrExprs);
    JNI_METHOD_END(0L)

    LookupOuterJoinWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = LookupOuterJoinWithExprOperatorFactory::CreateLookupOuterJoinWithExprOperatorFactory(
        probeDataTypes, probeOutputCols, probeOutputColsCount, probeHashKeysArrExprs, probeHashKeysCount,
        buildOutputCols, buildOutputDataTypes, jHashBuilderOperatorFactory);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, probeHashKeysArrExprs)
    Expr::DeleteExprs(probeHashKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupOuterJoinOperatorFactory_createLookupOuterJoinOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jintArray jBuildOutputCols,
    jstring jBuildOutputTypes, jlong jHashBuilderOperatorFactory)
{
    auto probeTypesChars = env->GetStringUTFChars(jProbeTypes, JNI_FALSE);
    auto probeOutputCols = env->GetIntArrayElements(jProbeOutputCols, JNI_FALSE);
    auto buildOutputCols = env->GetIntArrayElements(jBuildOutputCols, JNI_FALSE);
    auto buildOutputTypesChars = env->GetStringUTFChars(jBuildOutputTypes, JNI_FALSE);
    jint probeOutputColsCount = env->GetArrayLength(jProbeOutputCols);

    auto probeDataTypes = Deserialize(probeTypesChars);
    auto buildOutputDataTypes = Deserialize(buildOutputTypesChars);
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesChars);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesChars);

    LookupOuterJoinOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = LookupOuterJoinOperatorFactory::CreateLookupOuterJoinOperatorFactory(probeDataTypes,
        probeOutputCols, probeOutputColsCount, buildOutputCols, buildOutputDataTypes, jHashBuilderOperatorFactory);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jobjectArray jArgumentKeys,
    jstring jWindowFunctionReturnType, jintArray jWindowFrameTypes, jintArray jWindowFrameStartTypes,
    jintArray jWindowFrameStartChannels, jintArray jWindowFrameEndTypes, jintArray jWindowFrameEndChannels,
    jstring jOperatorConfig)
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

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

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
    windowWithExprOperatorFactory =
        WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(inputDataTypes, outputChannels,
        outputColsCount, windowFunction, windowFunctionCount, partitionChannels, partitionCount, preGroupedChannels,
        preGroupedCount, sortChannels, sortOrder, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, outputDataTypes, argumentKeysArrExprs, argumentKeysCount, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, argumentKeysArrExprs)
    Expr::DeleteExprs(argumentKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(windowWithExprOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannels, jstring jSourceType,
    jintArray jAggFuncType, jintArray jMaskCols, jobjectArray jOutputType, jbooleanArray jInputRaws,
    jbooleanArray jOutputPartials, jstring jOperatorConfig)
{
    // groupby channel and id
    auto groupByNum = static_cast<int32_t>(env->GetArrayLength(jGroupByChannel));
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);

    auto aggChannelsLength = static_cast<int32_t>(env->GetArrayLength(jAggChannels));
    vector<vector<string>> aggKeysVector;
    vector<int> aggColsNums;
    for (int i = 0; i < aggChannelsLength; ++i) {
        auto jAggChannel = static_cast<jstring>(env->GetObjectArrayElement(jAggChannels, i));
        auto aggChannelCharPtr = env->GetStringUTFChars(jAggChannel, JNI_FALSE);
        vector<string> expressions;
        DeserializeJsonToArray(aggChannelCharPtr, expressions);
        aggKeysVector.push_back(expressions);
        aggColsNums.push_back(expressions.size());
    }

    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);
    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);

    std::vector<DataTypes> outDataTypes;
    GetDataTypesVector(env, jOutputType, outDataTypes);

    vector<uint32_t> aggFuncTypes;
    GetIntVector(env, jAggFuncType, aggFuncTypes);
    vector<uint32_t> maskColumns;
    GetIntVector(env, jMaskCols, maskColumns);

    std::vector<bool> inputRaws;
    GetBoolVector(env, jInputRaws, inputRaws);
    std::vector<bool> outputPartials;
    GetBoolVector(env, jOutputPartials, outputPartials);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> groupByKeysExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(groupByKeys, groupByNum, groupByKeysExprs);
    JNI_METHOD_END(0L)

    vector<vector<omniruntime::expressions::Expr *>> aggKeysExprsVector;
    for (int i = 0; i < aggChannelsLength; ++i) {
        vector<omniruntime::expressions::Expr *> aggKeysExprs;
        JNI_METHOD_START
        // parse the expressions
        GetExprsFromJson(aggKeysVector.at(i), aggColsNums.at(i), aggKeysExprs);
        aggKeysExprsVector.push_back(aggKeysExprs);
        JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, aggKeysExprs)
    }

    HashAggregationWithExprOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeysExprs, groupByNum, aggKeysExprsVector,
        sourceDataTypes, outDataTypes, aggFuncTypes, maskColumns, inputRaws, outputPartials, overflowConfig);
    JNI_METHOD_END_WITH_MULTI_EXPRS(0L, groupByKeysExprs, aggKeysExprsVector)

    Expr::DeleteExprs(groupByKeysExprs);
    Expr::DeleteExprs(aggKeysExprsVector);

    return reinterpret_cast<intptr_t>(static_cast<void *>(nativeOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationWithExprOperatorFactory_createAggregationWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jobjectArray jGroupByChannel, jobjectArray jAggChannels, jstring jSourceType,
    jintArray jAggFuncType, jintArray jMaskCols, jobjectArray jOutputType, jbooleanArray jInputRaws,
    jbooleanArray jOutputPartials, jstring jOperatorConfig)
{
    // groupby channel and id
    auto groupByNum = static_cast<int32_t>(env->GetArrayLength(jGroupByChannel));
    std::string groupByKeys[groupByNum];
    GetExpressions(env, jGroupByChannel, groupByKeys, groupByNum);

    auto aggChannelsLength = static_cast<int32_t>(env->GetArrayLength(jAggChannels));
    vector<vector<string>> aggKeysVector;
    vector<int> aggColsNums;
    for (int i = 0; i < aggChannelsLength; ++i) {
        auto jAggChannel = static_cast<jstring>(env->GetObjectArrayElement(jAggChannels, i));
        auto aggChannelCharPtr = env->GetStringUTFChars(jAggChannel, JNI_FALSE);
        vector<string> expressions;
        DeserializeJsonToArray(aggChannelCharPtr, expressions);
        aggKeysVector.push_back(expressions);
        aggColsNums.push_back(expressions.size());
    }

    auto sourceTypesCharPtr = env->GetStringUTFChars(jSourceType, JNI_FALSE);
    auto sourceDataTypes = Deserialize(sourceTypesCharPtr);

    std::vector<DataTypes> outDataTypes;
    GetDataTypesVector(env, jOutputType, outDataTypes);

    vector<uint32_t> aggFuncTypes;
    GetIntVector(env, jAggFuncType, aggFuncTypes);
    vector<uint32_t> maskColumns;
    GetIntVector(env, jMaskCols, maskColumns);

    std::vector<bool> inputRaws;
    GetBoolVector(env, jInputRaws, inputRaws);
    std::vector<bool> outputPartials;
    GetBoolVector(env, jOutputPartials, outputPartials);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> groupByKeysExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(groupByKeys, groupByNum, groupByKeysExprs);
    JNI_METHOD_END(0L)

    vector<vector<omniruntime::expressions::Expr *>> aggKeysExprsVector;
    for (int i = 0; i < aggChannelsLength; ++i) {
        vector<omniruntime::expressions::Expr *> aggKeysExprs;
        JNI_METHOD_START
        // parse the expressions
        GetExprsFromJson(aggKeysVector.at(i), aggColsNums.at(i), aggKeysExprs);
        aggKeysExprsVector.push_back(aggKeysExprs);
        JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, aggKeysExprs)
    }

    AggregationWithExprOperatorFactory *nativeOperatorFactory = nullptr;
    JNI_METHOD_START
    nativeOperatorFactory = new AggregationWithExprOperatorFactory(groupByKeysExprs, groupByNum, aggKeysExprsVector,
        sourceDataTypes, outDataTypes, aggFuncTypes, maskColumns, inputRaws, outputPartials, *overflowConfig);
    JNI_METHOD_END_WITH_MULTI_EXPRS(0L, groupByKeysExprs, aggKeysExprsVector)

    Expr::DeleteExprs(groupByKeysExprs);
    Expr::DeleteExprs(aggKeysExprsVector);

    return reinterpret_cast<intptr_t>(static_cast<void *>(nativeOperatorFactory));
}


JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortKeys, jintArray jSortAsc, jintArray jSortNullFirsts,
    jstring jOperatorConfig)
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

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> sortKeyExprArr;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(sortKeysArr, sortKeyCount, sortKeyExprArr);
    JNI_METHOD_END(0L)

    TopNWithExprOperatorFactory *topNWithExprOperatorFactory = nullptr;
    JNI_METHOD_START
    topNWithExprOperatorFactory = new TopNWithExprOperatorFactory(sourceDataTypes, n, sortKeyExprArr, sortAsc,
        sortNullFirsts, sortKeyCount, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, sortKeyExprArr)
    Expr::DeleteExprs(sortKeyExprArr);

    return reinterpret_cast<intptr_t>(static_cast<void *>(topNWithExprOperatorFactory));
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
    LimitOperatorFactory *limitOperatorFactory = nullptr;
    JNI_METHOD_START
    limitOperatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(jLimit);
    JNI_METHOD_END(0L)
    return reinterpret_cast<intptr_t>(static_cast<void *>(limitOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_limit_OmniDistinctLimitOperatorFactory_createDistinctLimitOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSoureTypes, jintArray jDistinctChannel, jint jHashChannel, jlong jLimit)
{
    auto distinctColCount = (int32_t)env->GetArrayLength(jDistinctChannel);
    jint *distinctCols = env->GetIntArrayElements(jDistinctChannel, JNI_FALSE);

    const char *sourceTypesCharPtr = env->GetStringUTFChars(jSoureTypes, JNI_FALSE);
    auto sourceTypes = Deserialize(sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jSoureTypes, sourceTypesCharPtr);

    DistinctLimitOperatorFactory *distinctLimitOperatorFactory = nullptr;
    JNI_METHOD_START
    distinctLimitOperatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes,
        distinctCols, distinctColCount, jHashChannel, jLimit);
    JNI_METHOD_END(0L)
    return reinterpret_cast<intptr_t>(static_cast<void *>(distinctLimitOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjStreamedTableWithExprOperatorFactory_createSmjStreamedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jint jJoinType, jstring jFilter, jstring jOperatorConfig)
{
    switch ((JoinType)jJoinType) {
        case JoinType::OMNI_JOIN_TYPE_INNER:
        case JoinType::OMNI_JOIN_TYPE_LEFT:
        case JoinType::OMNI_JOIN_TYPE_FULL:
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI:
        case JoinType::OMNI_JOIN_TYPE_LEFT_ANTI:
            break;
        default:
            return 0L;
    }

    auto streamedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto streamedDataTypes = Deserialize(streamedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, streamedTypesChars);

    auto streamedKeyExpsCount = env->GetArrayLength(jEqualKeyExprs);
    std::string streamedKeyExpsArr[streamedKeyExpsCount];
    GetExpressions(env, jEqualKeyExprs, streamedKeyExpsArr, streamedKeyExpsCount);

    auto streamedOutputColsCnt = env->GetArrayLength(jOutputChannels);
    auto streamedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

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

    StreamedTableWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(
        streamedDataTypes, streamedKeysArrExprs, streamedKeyExpsCount, streamedOutputCols, streamedOutputColsCnt,
        (JoinType)jJoinType, filterExpression, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, streamedKeysArrExprs)
    Expr::DeleteExprs(streamedKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniSmjBufferedTableWithExprOperatorFactory_createSmjBufferedTableWithExprOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jobjectArray jEqualKeyExprs, jintArray jOutputChannels,
    jlong jSmjStreamedTableWithExprOperatorFactory, jstring jOperatorConfig)
{
    auto bufferedTypesChars = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    auto bufferedDataTypes = Deserialize(bufferedTypesChars);
    env->ReleaseStringUTFChars(jSourceTypes, bufferedTypesChars);

    auto bufferedKeyExpsCnt = env->GetArrayLength(jEqualKeyExprs);
    std::string bufferedKeyExpsArr[bufferedKeyExpsCnt];
    GetExpressions(env, jEqualKeyExprs, bufferedKeyExpsArr, bufferedKeyExpsCnt);

    auto bufferedOutputCols = env->GetIntArrayElements(jOutputChannels, JNI_FALSE);
    auto bufferedOutputColsCnt = env->GetArrayLength(jOutputChannels);

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    vector<omniruntime::expressions::Expr *> bufferedKeysArrExprs;
    JNI_METHOD_START
    // parse the expressions
    GetExprsFromJson(bufferedKeyExpsArr, bufferedKeyExpsCnt, bufferedKeysArrExprs);
    JNI_METHOD_END(0L)

    BufferedTableWithExprOperatorFactory *operatorFactory = nullptr;
    JNI_METHOD_START
    operatorFactory = BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(
        bufferedDataTypes, bufferedKeysArrExprs, bufferedKeyExpsCnt, bufferedOutputCols, bufferedOutputColsCnt,
        jSmjStreamedTableWithExprOperatorFactory, overflowConfig);
    JNI_METHOD_END_WITH_EXPRS_RELEASE(0L, bufferedKeysArrExprs)
    Expr::DeleteExprs(bufferedKeysArrExprs);

    return reinterpret_cast<intptr_t>(static_cast<void *>(operatorFactory));
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
    JNI_METHOD_END_WITH_MULTI_EXPRS(0, { filterExpr }, projectExprs)
    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projectExprs);
    return 1;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_fusion_OmniFusionOperatorFactory_createFusionOperatorFactory(JNIEnv *env,
    jclass jObj, jlongArray jOperatorFactories, jintArray jOperatorTypes, jstring jOperatorConfig)
{
    auto operatorCount = env->GetArrayLength(jOperatorTypes);
    auto operatorTypesArr = env->GetIntArrayElements(jOperatorTypes, JNI_FALSE);
    auto operatorFactoriesArr = env->GetLongArrayElements(jOperatorFactories, JNI_FALSE);

    std::vector<OperatorFactory *> operatorFactories(operatorCount);
    std::vector<omniruntime::op::OperatorType> operatorTypes(operatorCount);
    for (int32_t i = 0; i < operatorCount; i++) {
        operatorFactories[i] = reinterpret_cast<OperatorFactory *>(reinterpret_cast<void *>(operatorFactoriesArr[i]));
        operatorTypes[i] = static_cast<omniruntime::op::OperatorType>(operatorTypesArr[i]);
    }
    env->ReleaseIntArrayElements(jOperatorTypes, operatorTypesArr, 0);
    env->ReleaseLongArrayElements(jOperatorFactories, operatorFactoriesArr, 0);

    for (auto operatorType : operatorTypes) {
        if (operatorType != omniruntime::op::OMNI_FILTER_AND_PROJECT &&
            operatorType != omniruntime::op::OMNI_LOOKUP_JOIN &&
            operatorType != omniruntime::op::OMNI_HASH_AGGREGATION) {
            return reinterpret_cast<intptr_t>(nullptr);
        }
    }

    auto operatorConfigChars = env->GetStringUTFChars(jOperatorConfig, JNI_FALSE);
    auto operatorConfig = OperatorConfig::DeserializeOperatorConfig(operatorConfigChars);
    auto overflowConfig = operatorConfig.GetOverflowConfig();
    env->ReleaseStringUTFChars(jOperatorConfig, operatorConfigChars);

    auto fusionOperatorFactory = new FusionOperatorFactory(operatorFactories, operatorTypes, overflowConfig);
    return reinterpret_cast<intptr_t>(static_cast<void *>(fusionOperatorFactory));
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniBloomFilterOperatorFactory_createBloomFilterOperatorFactory(JNIEnv *env,
    jclass jObj, jint jInputVersion)
{
    auto inputVersion = (int32_t)jInputVersion;

    BloomFilterOperatorFactory *factory = nullptr;
    JNI_METHOD_START
    factory = new BloomFilterOperatorFactory(inputVersion);
    JNI_METHOD_END(0L)

    return reinterpret_cast<intptr_t>(static_cast<void *>(factory));
}