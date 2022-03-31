/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Jit Context Source File
 */
#include "jit_context.h"
#include "jit/jit.h"
#include "operator/optimization.h"
#include "../../libconfig.h"

using std::map;
using std::string;
using std::vector;
using namespace omniruntime::type;
using namespace omniruntime::jit;
using namespace omniruntime::expressions;
using namespace omniruntime::LibConfig;

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
    if (expr->GetType() == ExprType::FIELD_E) {
        auto fieldExpr = static_cast<FieldExpr *>(expr);
        return fieldExpr->colVal;
    }

    return -1;
}

void GetTypeIds(DataTypes &inputTypes, const vector<Expr *> &projectKeys, vector<int32_t> &typeIds,
    vector<int32_t> &projectCols)
{
    const int32_t *inputTypeIds = inputTypes.GetIds();
    auto inputTypesCount = inputTypes.GetSize();
    typeIds.insert(typeIds.end(), inputTypeIds, inputTypeIds + inputTypesCount);

    int32_t newProjectCol = inputTypesCount;
    auto projectKeysCount = projectKeys.size();
    for (uint32_t i = 0; i < projectKeysCount; i++) {
        auto projectCol = GetProjectCol(projectKeys[i]);
        projectCols[i] = projectCol;
        if (projectCol == -1) {
            auto returnType = projectKeys[i]->GetReturnTypeId();
            typeIds.push_back(returnType);
            projectCols[i] = newProjectCol++;
        }
    }
}

// hash aggregation with expression will use it.
void GetRequiredTypeIds(DataTypes &inputTypes, const vector<Expr *> &projectKeys, vector<int32_t> &typeIds,
    vector<int32_t> &projectCols)
{
    auto inputTypeIds = const_cast<int32_t *>(inputTypes.GetIds());

    int32_t newProjectCol = 0;
    map<int32_t, int32_t> colIdMap;
    auto projectKeysCount = projectKeys.size();
    for (uint32_t i = 0; i < projectKeysCount; i++) {
        auto projectCol = GetProjectCol(projectKeys[i]);
        if (projectCol == -1) {
            // expression col
            auto returnType = projectKeys[i]->GetReturnTypeId();
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

JitContext *CreateSortJitContext(DataTypes &sourceDataTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create sort JIT context starting.");
    auto start = START();

    const int32_t *sourceTypes = sourceDataTypes.GetIds();
    auto typesCount = sourceDataTypes.GetSize();
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

JitContext *CreateSortWithExprJitContext(DataTypes &sourceDataTypes, int32_t *outputCols, int32_t outputColsCount,
    const vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t sortKeysCount = sortKeys.size();
    vector<int32_t> newSourceTypes;
    vector<int32_t> sortCols(sortKeysCount);
    GetTypeIds(sourceDataTypes, sortKeys, newSourceTypes, sortCols);

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

JitContext *CreateHashBuilderJitContext(DataTypes &buildDataTypes, int32_t *buildHashCols, int32_t buildHashColsCount,
    int32_t operatorCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create hash builder JIT context starting.");
    auto start = START();

    const int32_t *buildTypes = buildDataTypes.GetIds();
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

JitContext *CreateLookupJoinJitContext(DataTypes &probeDataTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, DataTypes &buildOutputDataTypes,
    int32_t *buildOutputCols)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    JNI_DEBUG_LOG("create lookup join JIT context starting.");
    auto start = START();
    const int32_t *probeTypes = probeDataTypes.GetIds();
    int32_t hashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    const int32_t *buildOutputTypes = buildOutputDataTypes.GetIds();
    auto buildOutputColsCount = buildOutputDataTypes.GetSize();

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

JitContext *CreateHashBuilderWithExprJitContext(DataTypes &buildDataTypes,
    const vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t operatorCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t buildHashKeysCount = buildHashKeys.size();
    vector<int32_t> buildTypes;
    vector<int32_t> buildHashCols(buildHashKeysCount);
    GetTypeIds(buildDataTypes, buildHashKeys, buildTypes, buildHashCols);

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

JitContext *CreateLookupJoinWithExprJitContext(DataTypes &probeDataTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, const vector<omniruntime::expressions::Expr *> &probeHashKeys,
    DataTypes &buildOutputDataTypes, int32_t *buildOutputCols)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t probeHashKeysCount = probeHashKeys.size();
    vector<int32_t> probeTypes;
    vector<int32_t> probeHashCols(probeHashKeysCount);
    GetTypeIds(probeDataTypes, probeHashKeys, probeTypes, probeHashCols);

    int32_t hashColTypes[probeHashKeysCount];
    for (int32_t i = 0; i < probeHashKeysCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    const int32_t *buildOutputTypes = buildOutputDataTypes.GetIds();
    auto buildOutputColsCount = buildOutputDataTypes.GetSize();

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

JitContext *CreateTopNJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto sourceTypeIds = sourceDataTypes.GetIds();
    auto sourceTypesCount = sourceDataTypes.GetSize();
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

JitContext *CreateTopNWithExprJitContext(omniruntime::type::DataTypes &sourceDataTypes,
    const vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    int32_t sortKeysCount = sortKeys.size();
    vector<int32_t> newSourceTypes;
    vector<int32_t> sortCols(sortKeysCount);
    GetTypeIds(sourceDataTypes, sortKeys, newSourceTypes, sortCols);

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

JitContext *CreateWindowJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto typesCount = sourceDataTypes.GetSize();
    const int32_t *sourceTypes = sourceDataTypes.GetIds();
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

JitContext *CreateWindowWithExprJitContext(DataTypes &sourceDataTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount, DataTypes &outputTypes, const vector<omniruntime::expressions::Expr *> &argumentKeys)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    auto typesCount = sourceDataTypes.GetSize();
    vector<DataType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), sourceDataTypes.Get().begin(), sourceDataTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), outputTypes.Get().begin(), outputTypes.Get().end());

    DataTypes allTypes(allTypesVec);
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

    auto inputTypes = const_cast<int32_t *>(sourceDataTypes.GetIds());
    int32_t finalSortColTypes[finalSortColsCount];
    for (int32_t i = 0; i < finalSortColsCount; i++) {
        finalSortColTypes[i] = inputTypes[finalSortCols[i]];
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

JitContext *CreateHashAggregationJitContext(DataTypes &groupByDataTypes, int32_t *groupByCols, DataTypes &aggDataTypes,
    int32_t *aggCols, int32_t *aggFuncTypes, int32_t aggFuncsCount, DataTypes &outputDataTypes)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    // groupby channel and id
    auto groupColNum = groupByDataTypes.GetSize();
    auto aggNum = aggFuncsCount;

    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggNum = ParamValue(&aggNum);

    Specialization inloopSp;
    inloopSp.AddSpecializedParam(PARAM_INDEX_4, &pGroupNum);
    inloopSp.AddSpecializedParam(PARAM_INDEX_5, &pAggNum);

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

JitContext *CreateHashAggregationWithExprJitContext(omniruntime::type::DataTypes &sourceDataTypes,
    const vector<omniruntime::expressions::Expr *> &groupByKeys,
    const vector<omniruntime::expressions::Expr *> &aggKeys, int32_t *aggFuncTypes, int32_t aggFuncsCount,
    omniruntime::type::DataTypes &outputDataTypes)
{
#if defined(DISABLE_JIT)
    return nullptr;
#else
    // groupby channel and id
    int32_t groupColNum = groupByKeys.size();
    auto aggNum = aggFuncsCount;

    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggNum = ParamValue(&aggNum);

    Specialization inloopSp;
    inloopSp.AddSpecializedParam(PARAM_INDEX_4, &pGroupNum);
    inloopSp.AddSpecializedParam(PARAM_INDEX_5, &pAggNum);

    map<string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, inloopSp } };

    omniruntime::jit::Context groupAggWithExprContext(GenerateOperatorTemplatePath("group_aggregation_expr"),
        map<string, Specialization>());
    omniruntime::jit::Context groupAggregationContext(GenerateOperatorTemplatePath("group_aggregation"),
        hashGroupbySps);
    Jit jit(vector<omniruntime::jit::Context> { groupAggWithExprContext, groupAggregationContext });
    jit.Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit.GetJitedFunction("CreateOperator");

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
#endif
}

JitContext *CreateAggregationJitContext(DataTypes &sourceDataTypes, int32_t *aggCols, int32_t *aggMaskCols,
    int32_t *aggFuncTypes, int32_t aggFuncsCount, DataTypes &outputDataTypes)
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
