/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Jit Context Source File
 */
#include "jit_context.h"
#include "../../jit/jit.h"
#include "../optimization.h"
#include "../libconfig.h"

using std::map;
using std::string;
using std::vector;
using namespace omniruntime::vec;
using namespace omniruntime::jit;
using namespace omniruntime::expressions;

namespace {
constexpr int32_t PARAM_INDEX_0 = 0;
constexpr int32_t PARAM_INDEX_1 = 1;
constexpr int32_t PARAM_INDEX_2 = 2;
constexpr int32_t PARAM_INDEX_3 = 3;
constexpr int32_t PARAM_INDEX_4 = 4;
constexpr int32_t PARAM_INDEX_5 = 5;
constexpr int32_t PARAM_INDEX_6 = 6;
constexpr int32_t PARAM_INDEX_7 = 7;
constexpr int32_t PARAM_INDEX_8 = 8;
constexpr int32_t PARAM_INDEX_9 = 9;
}

int32_t GetProjectCol(Expr *expr)
{
    // #0 or #5 is not expression
    if (expr->GetType() == DATA_E) {
        auto dataExpr = static_cast<DataExpr *>(expr);
        if (dataExpr->isColumn) {
            return dataExpr->colVal;
        }
    }

    return -1;
}

void GetTypeIds(VecTypes &inputTypes, const vector<Expr *> &projectKeys, vector<int32_t> &typeIds,
    vector<int32_t> &projectCols)
{
    const int32_t *inputTypeIds = inputTypes.GetIds();
    auto inputTypesCount = inputTypes.GetSize();
    typeIds.insert(typeIds.end(), inputTypeIds, inputTypeIds + inputTypesCount);

    int32_t newProjectCol = inputTypesCount;
    auto projectKeysCount = projectKeys.size();
    for (int32_t i = 0; i < projectKeysCount; i++) {
        auto projectCol = GetProjectCol(projectKeys[i]);
        projectCols[i] = projectCol;
        if (projectCol == -1) {
            auto returnType = projectKeys[i]->GetExprDataType();
            typeIds.push_back(returnType);
            projectCols[i] = newProjectCol++;
        }
    }
}

// hash aggregation with expression will use it.
void GetRequiredTypeIds(VecTypes &inputTypes, const vector<Expr *> &projectKeys, vector<int32_t> &typeIds,
    vector<int32_t> &projectCols)
{
    auto inputTypeIds = const_cast<int32_t *>(inputTypes.GetIds());

    int32_t newProjectCol = 0;
    map<int32_t, int32_t> colIdMap;
    auto projectKeysCount = projectKeys.size();
    for (int32_t i = 0; i < projectKeysCount; i++) {
        auto projectCol = GetProjectCol(projectKeys[i]);
        if (projectCol == -1) {
            // expression col
            auto returnType = projectKeys[i]->GetExprDataType();
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

JitContext *CreateSortJitContext(VecTypes &sourceVecTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create sort JIT context starting.");
    auto start = START();

    const int32_t *sourceTypes = sourceVecTypes.GetIds();
    auto typesCount = sourceVecTypes.GetSize();
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

    Specialization compareToSp;
    compareToSp.AddSpecializedParam(PARAM_INDEX_0, &pSortCols);
    compareToSp.AddSpecializedParam(PARAM_INDEX_1, &pSortColTypes);
    compareToSp.AddSpecializedParam(PARAM_INDEX_2, &pSortAscendings);
    compareToSp.AddSpecializedParam(PARAM_INDEX_3, &pSortNullFirsts);
    compareToSp.AddSpecializedParam(PARAM_INDEX_4, &pSortColCount);

    Specialization getOutputSp;
    getOutputSp.AddSpecializedParam(PARAM_INDEX_1, &pOutputCols);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_2, &pOutputColCount);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_4, &pSourceTypes);

    map<string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, getOutputSp } };

    omniruntime::jit::Context sortContext(GenerateOperatorTemplatePath("sort"), map<string, Specialization>());
    omniruntime::jit::Context pagesIndexContext(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit jit(vector<omniruntime::jit::Context> { sortContext, pagesIndexContext });
    jit.Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH,
        ModuleOptimization::CONSTANT_MERGE });
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    JNI_DEBUG_LOG("create sort JIT context finished, elapsed time: %ld ms.", END(start));

    return jitContext;
#endif
}

JitContext *CreateSortWithExprJitContext(VecTypes &sourceVecTypes, int32_t *outputCols, int32_t outputColsCount,
    const vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t sortKeysCount = sortKeys.size();
    vector<int32_t> newSourceTypes;
    vector<int32_t> sortCols(sortKeysCount);
    GetTypeIds(sourceVecTypes, sortKeys, newSourceTypes, sortCols);

    int sortColTypes[sortKeysCount];
    for (int32_t i = 0; i < sortKeysCount; ++i) {
        sortColTypes[i] = newSourceTypes[sortCols[i]];
    }

    int32_t newSourceTypesCount = newSourceTypes.size();
    ParamValue pSourceTypes = ParamValue(newSourceTypes.data(), newSourceTypesCount);
    ParamValue pTypeCount = ParamValue(&newSourceTypesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    ParamValue pSortCols = ParamValue(sortCols.data(), sortKeysCount);
    ParamValue pSortColTypes = ParamValue(sortColTypes, sortKeysCount);
    ParamValue pSortAscendings = ParamValue(sortAscendings, sortKeysCount);
    ParamValue pSortNullFirsts = ParamValue(sortNullFirsts, sortKeysCount);
    ParamValue pSortColCount = ParamValue(&sortKeysCount);

    Specialization compareToSp;
    compareToSp.AddSpecializedParam(PARAM_INDEX_0, &pSortCols);
    compareToSp.AddSpecializedParam(PARAM_INDEX_1, &pSortColTypes);
    compareToSp.AddSpecializedParam(PARAM_INDEX_2, &pSortAscendings);
    compareToSp.AddSpecializedParam(PARAM_INDEX_3, &pSortNullFirsts);
    compareToSp.AddSpecializedParam(PARAM_INDEX_4, &pSortColCount);

    Specialization getOutputSp;
    getOutputSp.AddSpecializedParam(PARAM_INDEX_1, &pOutputCols);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_2, &pOutputColCount);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_4, &pSourceTypes);

    map<string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, getOutputSp } };

    omniruntime::jit::Context sortWithExprContext(GenerateOperatorTemplatePath("sort_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context sortContext(GenerateOperatorTemplatePath("sort"), map<string, Specialization>());
    omniruntime::jit::Context pagesIndexContext(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit jit(vector<omniruntime::jit::Context> { sortWithExprContext, sortContext, pagesIndexContext });
    jit.Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH,
        ModuleOptimization::CONSTANT_MERGE });
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateHashBuilderJitContext(VecTypes &buildVecTypes, int32_t *buildHashCols, int32_t buildHashColsCount,
    int32_t operatorCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create hash builder JIT context starting.");
    auto start = START();

    const int32_t *buildTypes = buildVecTypes.GetIds();
    int32_t hashColTypes[buildHashColsCount];
    for (int32_t i = 0; i < buildHashColsCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue pHashColCount = ParamValue(&buildHashColsCount);

    Specialization processColumnsSp;
    processColumnsSp.AddSpecializedParam(PARAM_INDEX_4, &pHashColTypes);
    processColumnsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_JOIN_HASH_TABLE_PROCESS_COLUMNS, processColumnsSp } };

    Specialization positionEqualsPositionIgnoreNullsSp;
    positionEqualsPositionIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS,
        positionEqualsPositionIgnoreNullsSp } };

    omniruntime::jit::Context hashBuilderContext(GenerateOperatorTemplatePath("hash_builder"),
        map<string, Specialization>());
    omniruntime::jit::Context joinHashTableContext(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    omniruntime::jit::Context pagesHashStrategyContext(GenerateOperatorTemplatePath("pages_hash_strategy"),
        hashStrategySps);

    Jit jit(vector<omniruntime::jit::Context> { hashBuilderContext, joinHashTableContext, pagesHashStrategyContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    JNI_DEBUG_LOG("create hash builder JIT context finished, elapsed time: %ld ms.", END(start));

    return jitContext;
#endif
}

JitContext *CreateLookupJoinJitContext(VecTypes &probeVecTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
    int32_t *probeHashCols, int32_t probeHashColsCount, VecTypes &buildOutputVecTypes, int32_t *buildOutputCols)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create lookup join JIT context starting.");
    auto start = START();
    const int32_t *probeTypes = probeVecTypes.GetIds();
    int32_t hashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    const int32_t *buildOutputTypes = buildOutputVecTypes.GetIds();
    auto buildOutputColsCount = buildOutputVecTypes.GetSize();

    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue pHashColCount = ParamValue(&probeHashColsCount);

    Specialization buildBuildColumnsSp;
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_3, &pBuildOutputTypes);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_4, &pBuildOutputCols);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_5, &pBuildOutputColsCount);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_6, &pProbeOutputColsCount);

    Specialization populateHashesSp;
    populateHashesSp.AddSpecializedParam(PARAM_INDEX_2, &pHashColTypes);
    populateHashesSp.AddSpecializedParam(PARAM_INDEX_3, &pHashColCount);
    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, buildBuildColumnsSp },
        { OMNIJIT_LOOKUP_JOIN_POPULATE_HASHES, populateHashesSp } };

    Specialization positionEqualsRowIgnoreNullsSp;
    positionEqualsRowIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColTypes);
    positionEqualsRowIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        positionEqualsRowIgnoreNullsSp } };

    omniruntime::jit::Context lookupJoinContext(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    omniruntime::jit::Context pagesHashStrategyContext(GenerateOperatorTemplatePath("pages_hash_strategy"),
        hashStrategySps);

    Jit jit(vector<omniruntime::jit::Context> { lookupJoinContext, pagesHashStrategyContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    JNI_DEBUG_LOG("create lookup join JIT context finished, elapsed time: %ld ms.", END(start));

    return jitContext;
#endif
}

JitContext *CreateHashBuilderWithExprJitContext(VecTypes &buildVecTypes,
    const vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t operatorCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t buildHashKeysCount = buildHashKeys.size();
    vector<int32_t> buildTypes;
    vector<int32_t> buildHashCols(buildHashKeysCount);
    GetTypeIds(buildVecTypes, buildHashKeys, buildTypes, buildHashCols);

    int32_t hashColTypes[buildHashKeysCount];
    for (int32_t i = 0; i < buildHashKeysCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashKeysCount);
    ParamValue pHashColCount = ParamValue(&buildHashKeysCount);

    Specialization processColumnsSp;
    processColumnsSp.AddSpecializedParam(PARAM_INDEX_4, &pHashColTypes);
    processColumnsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_JOIN_HASH_TABLE_PROCESS_COLUMNS, processColumnsSp } };

    Specialization positionEqualsPositionIgnoreNullsSp;
    positionEqualsPositionIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS,
        positionEqualsPositionIgnoreNullsSp } };

    omniruntime::jit::Context hashBuilderWithExprContext(GenerateOperatorTemplatePath("hash_builder_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context hashBuilderContext(GenerateOperatorTemplatePath("hash_builder"),
        map<string, Specialization>());
    omniruntime::jit::Context joinHashTableContext(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    omniruntime::jit::Context pagesHashStrategyContext(GenerateOperatorTemplatePath("pages_hash_strategy"),
        hashStrategySps);

    Jit jit(vector<omniruntime::jit::Context> { hashBuilderWithExprContext, hashBuilderContext, joinHashTableContext,
        pagesHashStrategyContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateLookupJoinWithExprJitContext(VecTypes &probeVecTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, const vector<omniruntime::expressions::Expr *> &probeHashKeys,
    VecTypes &buildOutputVecTypes, int32_t *buildOutputCols)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t probeHashKeysCount = probeHashKeys.size();
    vector<int32_t> probeTypes;
    vector<int32_t> probeHashCols(probeHashKeysCount);
    GetTypeIds(probeVecTypes, probeHashKeys, probeTypes, probeHashCols);

    int32_t hashColTypes[probeHashKeysCount];
    for (int32_t i = 0; i < probeHashKeysCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    const int32_t *buildOutputTypes = buildOutputVecTypes.GetIds();
    auto buildOutputColsCount = buildOutputVecTypes.GetSize();

    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashKeysCount);
    ParamValue pHashColCount = ParamValue(&probeHashKeysCount);

    Specialization buildBuildColumnsSp;
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_3, &pBuildOutputTypes);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_4, &pBuildOutputCols);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_5, &pBuildOutputColsCount);
    buildBuildColumnsSp.AddSpecializedParam(PARAM_INDEX_6, &pProbeOutputColsCount);

    Specialization populateHashesSp;
    populateHashesSp.AddSpecializedParam(PARAM_INDEX_2, &pHashColTypes);
    populateHashesSp.AddSpecializedParam(PARAM_INDEX_3, &pHashColCount);
    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, buildBuildColumnsSp },
        { OMNIJIT_LOOKUP_JOIN_POPULATE_HASHES, populateHashesSp } };

    Specialization positionEqualsRowIgnoreNullsSp;
    positionEqualsRowIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_5, &pHashColTypes);
    positionEqualsRowIgnoreNullsSp.AddSpecializedParam(PARAM_INDEX_6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        positionEqualsRowIgnoreNullsSp } };

    omniruntime::jit::Context lookupJoinWithExprContext(GenerateOperatorTemplatePath("lookup_join_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context lookupJoinContext(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    omniruntime::jit::Context pagesHashStrategyContext(GenerateOperatorTemplatePath("pages_hash_strategy"),
        hashStrategySps);

    Jit jit(
        vector<omniruntime::jit::Context> { lookupJoinWithExprContext, lookupJoinContext, pagesHashStrategyContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateTopNJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto sourceTypeIds = sourceVecTypes.GetIds();
    auto sourceTypesCount = sourceVecTypes.GetSize();
    ParamValue pSourceTypes = ParamValue(sourceTypeIds, sourceTypesCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColsCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    Specialization topNCompareSp;
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_4, &pSortColCount);
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_5, &pSortCols);
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_6, &pSourceTypes);
    map<string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, topNCompareSp } };

    omniruntime::jit::Context topNContext(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit jit(vector<omniruntime::jit::Context> { topNContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = createOperatorFunc;

    return jitContext;
#endif
}

JitContext *CreateTopNWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes,
    const vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t sortKeysCount = sortKeys.size();
    vector<int32_t> newSourceTypes;
    vector<int32_t> sortCols(sortKeysCount);
    GetTypeIds(sourceVecTypes, sortKeys, newSourceTypes, sortCols);

    ParamValue pSourceTypes = ParamValue(newSourceTypes.data(), newSourceTypes.size());
    ParamValue pSortKeyCount = ParamValue(&sortKeysCount);
    ParamValue pSortCols = ParamValue(sortCols.data(), sortKeysCount);

    Specialization topNCompareSp;
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_4, &pSortKeyCount);
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_5, &pSortCols);
    topNCompareSp.AddSpecializedParam(PARAM_INDEX_6, &pSourceTypes);
    map<string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, topNCompareSp } };

    omniruntime::jit::Context topNWithExprContext(GenerateOperatorTemplatePath("topn_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context topNContext(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit jit(vector<omniruntime::jit::Context> { topNWithExprContext, topNContext });

    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateWindowJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto typesCount = sourceVecTypes.GetSize();
    const int32_t *sourceTypes = sourceVecTypes.GetIds();
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

    Specialization compareToSp;
    compareToSp.AddSpecializedParam(PARAM_INDEX_0, &pSortCols);
    compareToSp.AddSpecializedParam(PARAM_INDEX_1, &pSortColTypes);
    compareToSp.AddSpecializedParam(PARAM_INDEX_2, &pSortAscendings);
    compareToSp.AddSpecializedParam(PARAM_INDEX_3, &pSortNullFirsts);
    compareToSp.AddSpecializedParam(PARAM_INDEX_4, &pSortColCount);
    Specialization getOutputSp;
    getOutputSp.AddSpecializedParam(PARAM_INDEX_1, &pOutputCols);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_2, &pOutputColCount);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_4, &pSourceTypes);
    map<string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, getOutputSp } };

    omniruntime::jit::Context windowContext(GenerateOperatorTemplatePath("window"), map<string, Specialization>());
    omniruntime::jit::Context pagesIndexContext(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit jit(vector<omniruntime::jit::Context> { windowContext, pagesIndexContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateWindowWithExprJitContext(VecTypes &sourceVecTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount, VecTypes &outputTypes, const vector<omniruntime::expressions::Expr *> &argumentKeys)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto typesCount = sourceVecTypes.GetSize();
    vector<VecType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), sourceVecTypes.Get().begin(), sourceVecTypes.Get().end());
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

    auto inputTypes = const_cast<int32_t *>(sourceVecTypes.GetIds());
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

    Specialization compareToSp;
    compareToSp.AddSpecializedParam(PARAM_INDEX_0, &pSortCols);
    compareToSp.AddSpecializedParam(PARAM_INDEX_1, &pSortColTypes);
    compareToSp.AddSpecializedParam(PARAM_INDEX_2, &pSortAscendings);
    compareToSp.AddSpecializedParam(PARAM_INDEX_3, &pSortNullFirsts);
    compareToSp.AddSpecializedParam(PARAM_INDEX_4, &pSortColCount);
    Specialization getOutputSp;
    getOutputSp.AddSpecializedParam(PARAM_INDEX_1, &pOutputCols);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_2, &pOutputColCount);
    getOutputSp.AddSpecializedParam(PARAM_INDEX_4, &pSourceTypes);
    map<string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, getOutputSp } };

    omniruntime::jit::Context windowWithExprContext(GenerateOperatorTemplatePath("window_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context windowContext(GenerateOperatorTemplatePath("window"), map<string, Specialization>());
    omniruntime::jit::Context pagesIndexContext(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    Jit jit(vector<omniruntime::jit::Context> { windowWithExprContext, windowContext, pagesIndexContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");
    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateHashAggregationJitContext(VecTypes &groupByVecTypes, int32_t *groupByCols, VecTypes &aggVecTypes,
    int32_t *aggCols, int32_t *aggFuncTypes, int32_t aggFuncsCount, VecTypes &outputVecTypes)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    // groupby channel and id
    auto groupColNum = groupByVecTypes.GetSize();
    auto groupByTypeIds = groupByVecTypes.GetIds();
    auto aggColNum = aggVecTypes.GetSize();
    auto aggTypeIds = aggVecTypes.GetIds();

    int32_t colNum = groupColNum + aggColNum;
    int32_t colTypes[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByCols[i]] = groupByTypeIds[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggCols[i]] = aggTypeIds[i];
    }

    ParamValue pColType = ParamValue(colTypes, colNum);
    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggNum = ParamValue(&aggColNum);

    Specialization inloopSp;
    inloopSp.AddSpecializedParam(PARAM_INDEX_3, &pColType);
    inloopSp.AddSpecializedParam(PARAM_INDEX_5, &pGroupNum);
    inloopSp.AddSpecializedParam(PARAM_INDEX_7, &pAggNum);

    map<string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, inloopSp } };

    omniruntime::jit::Context groupAggregationContext(GenerateOperatorTemplatePath("group_aggregation"),
        hashGroupbySps);
    Jit jit(vector<omniruntime::jit::Context> { groupAggregationContext });
    jit.Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateHashAggregationWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes,
    const vector<omniruntime::expressions::Expr *> &groupByKeys,
    const vector<omniruntime::expressions::Expr *> &aggKeys, int32_t *aggFuncTypes, int32_t aggFuncsCount,
    omniruntime::vec::VecTypes &outputVecTypes)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t groupColNum = groupByKeys.size();
    int32_t aggColNum = aggKeys.size();
    int32_t totalNum = groupColNum + aggColNum;
    vector<omniruntime::expressions::Expr *> projectKeys;
    projectKeys.insert(projectKeys.end(), groupByKeys.begin(), groupByKeys.end());
    projectKeys.insert(projectKeys.end(), aggKeys.begin(), aggKeys.end());

    vector<int32_t> projectTypes;
    vector<int32_t> projectCols(totalNum);
    GetRequiredTypeIds(sourceVecTypes, projectKeys, projectTypes, projectCols);

    auto projectColsFront = &projectCols.front();
    int32_t groupByCols[groupColNum];
    std::copy(projectColsFront, projectColsFront + groupColNum, groupByCols);
    int32_t aggCols[aggColNum];
    std::copy(projectColsFront + groupColNum, projectColsFront + groupColNum + aggColNum, aggCols);

    int32_t groupByTypeIds[groupColNum];
    std::copy(projectTypes.begin(), projectTypes.begin() + groupColNum, groupByTypeIds);
    int32_t aggTypeIds[aggColNum];
    std::copy(projectTypes.begin() + groupColNum, projectTypes.begin() + groupColNum + aggColNum, aggTypeIds);

    ParamValue pColType = ParamValue(projectTypes.data(), totalNum);
    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggNum = ParamValue(&aggColNum);

    Specialization inloopSp;
    inloopSp.AddSpecializedParam(PARAM_INDEX_3, &pColType);
    inloopSp.AddSpecializedParam(PARAM_INDEX_5, &pGroupNum);
    inloopSp.AddSpecializedParam(PARAM_INDEX_7, &pAggNum);

    map<string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, inloopSp } };

    omniruntime::jit::Context groupAggWithExprContext(GenerateOperatorTemplatePath("group_aggregation_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context groupAggContext(GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    Jit jit(vector<omniruntime::jit::Context> { groupAggWithExprContext, groupAggContext });
    jit.Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateAggregationJitContext(VecTypes &sourceVecTypes, int32_t *aggCols, int32_t *aggFuncTypes,
    int32_t aggFuncsCount, VecTypes &outputVecTypes)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    omniruntime::jit::Context groupAggregationContext(GenerateOperatorTemplatePath("non_group_aggregation"),
        map<string, Specialization>());
    Jit jit(vector<omniruntime::jit::Context> { groupAggregationContext });
    jit.Specialize();
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}
