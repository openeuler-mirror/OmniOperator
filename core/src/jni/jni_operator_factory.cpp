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
#include "../operator/window/window.h"
#include "../operator/join/hash_builder.h"
#include "../operator/join/lookup_join.h"
#include "../operator/topn/topn.h"
#include "../operator/partitionedoutput/partitionedoutput.h"
#include "../operator/union/union.h"
#include "../operator/optimization.h"
#include "config.h"

using omniruntime::jit::ParamValue;
using omniruntime::jit::Specialization;
using omniruntime::vec::Deserialize;
using omniruntime::vec::VecType;

using namespace omniruntime::op;

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperatorNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperatorNative(JNIEnv *env,
    jobject jObj, jlong jNativeFactoryObj)
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
 *             */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
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

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t *)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t *)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t *)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_col_type);
    inloopSp->AddSpecializedParam(4, &p_col_count);
    inloopSp->AddSpecializedParam(6, &p_group_num);
    inloopSp->AddSpecializedParam(8, &p_agg_num);
    inloopSp->AddSpecializedParam(9, &p_agg_types);

    auto *processAggSp = new Specialization();
    processAggSp->AddSpecializedParam(2, &p_agg_num);
    processAggSp->AddSpecializedParam(3, &p_col_type);

    auto *hashColumnSp = new Specialization();
    hashColumnSp->AddSpecializedParam(2, &p_col_type);
    hashColumnSp->AddSpecializedParam(3, &p_group_num);

    auto *aggColumnSp = new Specialization();
    aggColumnSp->AddSpecializedParam(2, &p_col_type);
    aggColumnSp->AddSpecializedParam(3, &p_agg_num);

    std::map<std::string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp },
        // TODO: open this optimization
//        {OMNIJIT_HASH_GROUPBY_HASH_COLUMN, *hashColumnSp},
//        {OMNIJIT_HASH_GROUPBY_AGG_COLUMN, *aggColumnSp},
        {OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> {*groupAggregationContext});
    jit->Specialize(
            std::vector<Optimization> {
                Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
                Optimization::SROA, Optimization::AGGRESIVE_DCE
            },
            std::vector<ModuleOptimization> {ModuleOptimization::PRUNE_EH});
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext,
        aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
    JNI_DEBUG_LOG("create hashagg operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jGroupByType, groupByTypesCharPtr);
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    env->ReleaseStringUTFChars(jOutPutTye, outTypesCharPtr);

    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

/* *
 * Return an AggregationFactory object address.
 *             */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jAggType, jintArray jAggFuncType, jboolean inputRaw, jboolean outputPartial)
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

    using namespace omniruntime::jit;
    std::map<std::string, ParamValue *> testParam;

    ParamValue p_col_type = ParamValue((int32_t *)aggTypeContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_agg_num);
    inloopSp->AddSpecializedParam(4, &p_col_type);
    inloopSp->AddSpecializedParam(5, &p_agg_types);

    std::map<std::string, Specialization> nonGroupSps = { { OMNIJIT_NON_GROUP_INLOOP, *inloopSp } };

    auto *groupAggregationContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("non_group_aggregation"), nonGroupSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> {*groupAggregationContext});
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    omniruntime::op::AggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, inputRaw, outputPartial);
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
    env->ReleaseStringUTFChars(jAggType, aggTypesCharPtr);
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JitContext *createSortJitContext(const int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jintArray jOutputCols, jobjectArray jSortCols, jintArray jAscendings, jintArray jNullFirsts)
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
    JitContext *jitContext =
        createSortJitContext(sortOperatorFactory->GetSourceTypes(), sortOperatorFactory->GetSourceTypeCount(),
        sortOperatorFactory->GetOutputCols(), sortOperatorFactory->GetOutputColCount(),
        sortOperatorFactory->GetSortCols(), sortOperatorFactory->GetSortAscendings(),
        sortOperatorFactory->GetSortNullFirsts(), sortOperatorFactory->GetSortColCount());
    sortOperatorFactory->SetJitContext(jitContext);
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);

    return (int64_t)sortOperatorFactory;
}

JitContext *createSortJitContext(const int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
    JNI_DEBUG_LOG("create sort JIT context starting.");
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
    compareToSp->AddSpecializedParam(0, &p_sortCols);
    compareToSp->AddSpecializedParam(1, &p_sortColTypes);
    compareToSp->AddSpecializedParam(2, &p_sortAscendings);
    compareToSp->AddSpecializedParam(3, &p_sortNullFirsts);
    compareToSp->AddSpecializedParam(4, &p_sortColCount);

    auto *getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &p_outputCols);
    getOutputSp->AddSpecializedParam(2, &p_outputColCount);
    getOutputSp->AddSpecializedParam(4, &p_sourceTypes);

    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };

    auto *sortContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("sort"), std::map<std::string, Specialization>());
    auto *pagesIndexContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> {*sortContext, *pagesIndexContext});
    jit->Specialize(
        std::vector<Optimization> {
            Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
            Optimization::SROA, Optimization::AGGRESIVE_DCE
        },
        std::vector<ModuleOptimization> {
            ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH, ModuleOptimization::CONSTANT_MERGE
        });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create sort JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *createWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *env,
    jobject jObj, jstring jSourceTypes, jintArray jOutputChannels, jintArray jWindowFunction,
    jintArray jPartitionChannels, jintArray JPreGroupedChannels, jintArray jSortChannels, jintArray jSortOrder,
    jintArray jSortNullFirsts, jint preSortedChannelPrefix, jint expectedPositions, jintArray jArgumentChannels,
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
    JitContext *jitContext =
        createWindowJitContext(windowOperatorFactory->GetSourceTypes(), windowOperatorFactory->GetTypesCount(),
        windowOperatorFactory->GetOutputCols(), windowOperatorFactory->GetOutputColsCount(),
        windowOperatorFactory->GetPartitionCols(), windowOperatorFactory->GetPartitionCount(),
        windowOperatorFactory->GetSortCols(), windowOperatorFactory->GetSortAscendings(),
        windowOperatorFactory->GetSortNullFirsts(), windowOperatorFactory->GetSortColCount(),
        windowOperatorFactory->GetAllTypes(), windowOperatorFactory->GetAllCount());
    windowOperatorFactory->Init();
    windowOperatorFactory->SetJitContext(jitContext);

    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    env->ReleaseStringUTFChars(jWindowFunctionReturnType, windowFunctionReturnTypeCharPtr);
    return (int64_t)windowOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(JNIEnv *env, jclass jObj,
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
    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new omniruntime::op::TopNOperatorFactory(sourceTypes, n, sortCols, sortAsc, sortNullFirsts, sortColCount);

    auto sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    auto sourceTypesCount = sourceTypes.GetSize();

    ParamValue p_sourceTypes = ParamValue(sourceTypeIds, sourceTypesCount);
    ParamValue p_sortColCount = ParamValue(&sortColCount);
    ParamValue p_sortCols = ParamValue(sortCols, sortColCount);

    auto *topNCompareSp = new Specialization();
    topNCompareSp->AddSpecializedParam(4, &p_sortColCount); // 4teh parameter
    topNCompareSp->AddSpecializedParam(5, &p_sortCols);     // 5teh parameter
    topNCompareSp->AddSpecializedParam(6, &p_sourceTypes);  // 6teh parameter

    std::map<std::string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, *topNCompareSp } };

    auto *topNContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *topNContext });

    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = createOperatorFunc;

    topNOperatorFactory->SetJitContext(jitContext);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesCharPtr);
    return (int64_t)topNOperatorFactory;
}

JitContext *createWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount)
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
    compareToSp->AddSpecializedParam(0, &p_sortCols);
    compareToSp->AddSpecializedParam(1, &p_sortColTypes);
    compareToSp->AddSpecializedParam(2, &p_sortAscendings);
    compareToSp->AddSpecializedParam(3, &p_sortNullFirsts);
    compareToSp->AddSpecializedParam(4, &p_sortColCount);
    auto *getOutputSp = new Specialization();
    getOutputSp->AddSpecializedParam(1, &p_outputCols);
    getOutputSp->AddSpecializedParam(2, &p_outputColCount);
    getOutputSp->AddSpecializedParam(4, &p_sourceTypes);
    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("window"), std::map<std::string, Specialization>());
    auto *pagesIndexContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *windowContext, *pagesIndexContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jInputTypes, jint jInputLength, jstring jExpression, jintArray jProjectIndices,
    jint jProjectLength)
{
    auto expressionCharPtr = env->GetStringUTFChars(jExpression, JNI_FALSE);
    std::string filterExpression = std::string(expressionCharPtr);
    auto inputTypesCharPtr = env->GetStringUTFChars(jInputTypes, JNI_FALSE);
    auto inputVecTypes = Deserialize(inputTypesCharPtr);
    auto inputTypeIds = const_cast<int32_t *>(inputVecTypes.GetIds());
    int32_t inputLength = (int32_t)jInputLength;
    jint *projectIndices = env->GetIntArrayElements(jProjectIndices, JNI_FALSE);
    int32_t projectLength = (int32_t)jProjectLength;
    omniruntime::op::FilterAndProjectOperatorFactory *factory = new omniruntime::op::FilterAndProjectOperatorFactory(
        filterExpression, inputTypeIds, inputLength, projectIndices, projectLength);
    env->ReleaseStringUTFChars(jInputTypes, inputTypesCharPtr);
    env->ReleaseStringUTFChars(jExpression, expressionCharPtr);
    return (int64_t)factory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *env,
    jobject jobj, jstring jInputTypes, jint jInputLength, jobjectArray jExprs, jint jExprsLength)
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
    return (int64_t)factory;

    // TODO: ReleaseStringUTFChars
}

JitContext *createHashBuilderJitContext(const int32_t *buildTypes, int32_t buildTypesCount, int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jBuildTypes, jobjectArray jBuildHashCols, jint jOperatorCount)
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
    JitContext *jitContext = createHashBuilderJitContext(buildVecTypes.GetIds(), buildVecTypes.GetSize(),
        buildHashColsArr, buildHashColsCount, jOperatorCount);
    hashBuilderOperatorFactory->SetJitContext(jitContext);
    JNI_DEBUG_LOG("create hash builder operator factory finished, elapsed time: %ld ms.", END(start));
    env->ReleaseStringUTFChars(jBuildTypes, buildTypesCharPtr);
    return (int64_t)hashBuilderOperatorFactory;
}

JitContext *createHashBuilderJitContext(const int32_t *buildTypes, int32_t buildTypesCount, int32_t *buildHashCols,
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
    ParamValue p_hashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue p_hashColCount = ParamValue(&buildHashColsCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->AddSpecializedParam(3, &p_hashColTypes);
    hashPositionSp->AddSpecializedParam(4, &p_hashColCount);
    std::map<std::string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION,
        *hashPositionSp } };

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(5, &p_hashColTypes);
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(6, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp }
    };

    auto *hashBuilderContext = new omniruntime::jit::Context(
    GenerateOperatorTemplatePath("hash_builder"), std::map<std::string, Specialization>());
    auto *joinHashTableContext = new omniruntime::jit::Context(
    GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto *pagesHashStrategyContext = new omniruntime::jit::Context(
    GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *hashBuilderContext,
        *joinHashTableContext, *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create hash builder JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JitContext *createLookupJoinJitContext(const int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount, int64_t hashBuilderFactoryAddr);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jProbeTypes, jintArray jProbeOutputCols, jobjectArray jProbeHashCols, jintArray
    jBuildOutputCols, jstring jBuildOutputTypes, jint jJoinType, jlong jHashBuilderOperatorFactory)
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
    JitContext *jitContext = createLookupJoinJitContext(probeVecTypes.GetIds(), probeVecTypes.GetSize(),
        probeOutputColsArr, probeOutputColsCount, probeHashColsArr, probeHashColsCount, buildOutputColsArr,
        buildOutputVecTypes.GetIds(), buildOutputVecTypes.GetSize(), jHashBuilderOperatorFactory);
    lookupJoinOperatorFactory->SetJitContext(jitContext);
    JNI_DEBUG_LOG("create lookup join operator factory finished, elapsed time: %ld ms.", END(start));
    env->ReleaseStringUTFChars(jProbeTypes, probeTypesCharPtr);
    env->ReleaseStringUTFChars(jBuildOutputTypes, buildOutputTypesCharPtr);
    return (int64_t)lookupJoinOperatorFactory;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_partitioned_OmniPartitionedOutPutOperatorFactory_createPartitionedOperatorFactory(
    JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jReplicatesAnyRow, jint jNullChannel,
    jintArray jPartitionChannels, jint jPartitionCount, jintArray jBucketToPartition)
{
    JNI_DEBUG_LOG("create partitionedoutput operator factory starting.");
    auto start = START();
    auto sourceTypesArrCharPtr = env->GetStringUTFChars(jSourceTypes, JNI_FALSE);
    jint *partitionChannelsArr = env->GetIntArrayElements(jPartitionChannels, JNI_FALSE);
    jint *bucketToPartitionArr = env->GetIntArrayElements(jBucketToPartition, JNI_FALSE);

    auto sourceVecTypes = Deserialize(sourceTypesArrCharPtr);
    jint sourceTypesCount = sourceVecTypes.GetSize();
    jint partitionChannelsCount = env->GetArrayLength(jPartitionChannels);
    jint bucketToPartitionCount = env->GetArrayLength(jBucketToPartition);

    auto sourceTypesArr = const_cast<int32_t *>(sourceVecTypes.GetIds());
    JNI_DEBUG_LOG("before create partitionedoutput operator factory elapsed time: %ld ms.", END(start));
    omniruntime::op::PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        omniruntime::op::PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypesArr,
        sourceTypesCount, jReplicatesAnyRow, jNullChannel, partitionChannelsArr, partitionChannelsCount,
        jPartitionCount, bucketToPartitionArr, bucketToPartitionCount);
    JNI_DEBUG_LOG("create partitionedoutput operator factory finished, elapsed time: %ld ms.", END(start));
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    env->ReleaseStringUTFChars(jSourceTypes, sourceTypesArrCharPtr);
    return (int64_t)partitionedOutputOperatorFactory;
}

JitContext *createLookupJoinJitContext(const int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount, int64_t hashBuilderFactoryAddr)
{
    if (probeHashColsCount <= 0) {
        std::cerr << "Memory allocation size is illegal!" << std::endl;
        return nullptr;
    }
    int32_t hashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    JNI_DEBUG_LOG("create lookup join JIT context starting.");
    auto start = START();
    using namespace omniruntime::jit;

    ParamValue p_probeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue p_buildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue p_buildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue p_buildOutputColsCount = ParamValue(&buildOutputColsCount);

    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->AddSpecializedParam(3, &p_buildOutputTypes);
    buildBuildColumnsSp->AddSpecializedParam(4, &p_buildOutputCols);
    buildBuildColumnsSp->AddSpecializedParam(5, &p_buildOutputColsCount);
    buildBuildColumnsSp->AddSpecializedParam(6, &p_probeOutputColsCount);

    std::map<std::string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp } };

    ParamValue p_hashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue p_hashColCount = ParamValue(&probeHashColsCount);

    auto *hashRowSp = new Specialization();
    hashRowSp->AddSpecializedParam(2, &p_hashColTypes);
    hashRowSp->AddSpecializedParam(3, &p_hashColCount);
    std::map<std::string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };

    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(5, &p_hashColTypes);
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(6, &p_hashColCount);
    std::map<std::string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };

    auto lookupJoinContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    auto joinHashTableContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *lookupJoinContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    JNI_DEBUG_LOG("create lookup join JIT context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
        JNIEnv *env, jobject jObj, jstring jSourceTypes, jboolean jDistinct)
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