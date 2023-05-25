/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: dt fuzz factory create utils implementations
 */

#include "operator/filter/filter_and_project.h"
#include "operator/sort/sort.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/join/lookup_join.h"
#include "operator/limit/distinct_limit.h"
#include "operator/union/union.h"
#include "operator/topn/topn.h"
#include "operator/join/sortmergejoin/sort_merge_join.h"
#include "operator/window/window.h"
#include "dt_fuzz_factory_create_util.h"

using namespace omniruntime::type;
using namespace omniruntime::op;
using namespace omniruntime::expressions;

namespace DtFuzzFactoryCreateUtil {
const int32_t SHORT_DECIMAL_SIZE = 18;
const int32_t LONG_DECIMAL_SIZE = 38;
int32_t CHAR_SIZE = 10;
OperatorFactory *CreateFilterFactory(omniruntime::type::DataTypes &sourceTypes, BinaryExpr *filterExpr,
    std::vector<Expr *> projections, OverflowConfig *overflowConfig)
{
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(filterExpr, projections, sourceTypes, overflowConfig);
    auto operatorFactory = new FilterAndProjectOperatorFactory(move(exprEvaluator));
    return operatorFactory;
}

OperatorFactory *CreateSortFactory(omniruntime::type::DataTypes &sourceTypes)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[sourceTypesSize];
    int32_t sortCols[sourceTypesSize];
    int32_t ascendings[sourceTypesSize];
    int32_t nullFirsts[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        outputCols[i] = i;
        sortCols[i] = i;
        ascendings[i] = 1;
        nullFirsts[i] = 0;
    }

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    return operatorFactory;
}

OperatorFactory *CreateAggregationFactory(omniruntime::type::DataTypes &sourceTypes)
{
    omniruntime::op::FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    DataTypes groupTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    DataTypes aggInputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    uint32_t aggCols[2] = {2, 4};
    std::vector<uint32_t> aggFuncTypeContext =
        std::vector<uint32_t>((uint32_t *)aggFunType, (uint32_t *)aggFunType + 2);
    uint32_t maskCols[2] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};
    std::vector<uint32_t> maskColsContext = std::vector<uint32_t>((uint32_t *)maskCols, (uint32_t *)maskCols + 2);
    std::vector<uint32_t> aggInputColsContext = std::vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + 2);

    auto aggInputColsContextWrap = AggregatorUtil::WrapWithVector(aggInputColsContext);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeContext.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeContext.size());

    auto operatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypeContext, aggInputColsContextWrap,
        maskColsContext, aggOutputTypesWrap, inputRawsWrap, outputPartialsWrap, true);

    return operatorFactory;
}

OperatorFactory *CreateHashAggregationFactory(omniruntime::type::DataTypes &sourceTypes)
{
    omniruntime::op::FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    DataTypes groupTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    DataTypes aggInputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), DoubleType() }));
    uint32_t groupCols[2] = {1, 2};
    uint32_t aggCols[2] = {2, 4};
    std::vector<uint32_t> groupByColContext = std::vector<uint32_t>((uint32_t *)groupCols, (uint32_t *)groupCols + 2);
    std::vector<uint32_t> aggColContext = std::vector<uint32_t>((uint32_t *)aggCols, (uint32_t *)aggCols + 2);
    std::vector<uint32_t> aggFuncTypeContext =
        std::vector<uint32_t>((uint32_t *)aggFunType, (uint32_t *)aggFunType + 2);
    int32_t maskCols[] = {-1, -1};
    std::vector<uint32_t> maskColsContext = std::vector<uint32_t>((uint32_t *)maskCols, (uint32_t *)maskCols + 2);

    auto aggColContextWrap = AggregatorUtil::WrapWithVector(aggColContext);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeContext.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeContext.size());

    auto operatorFactory =
        new HashAggregationOperatorFactory(groupByColContext, groupTypes, aggColContextWrap, aggInputTypesWrap,
        aggOutputTypesWrap, aggFuncTypeContext, maskColsContext, inputRawsWrap, outputPartialsWrap, true);
    return operatorFactory;
}


std::vector<OperatorFactory *> CreateHashJoinFactory(omniruntime::type::DataTypes &sourceTypes,
    OverflowConfig *overflowConfig)
{
    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    std::string filterExpression = "";

    auto hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(sourceTypes, buildJoinCols,
        joinColsCount, filterExpression, 1);

    int32_t probeOutputCols[1] = {2};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    DataTypes buildOutputTypes(std::vector<DataTypePtr>({ LongType() }));
    int32_t buildOutputCols[1] = {2};
    int32_t buildOutputColsCount = 1;

    auto hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderFactory);

    auto lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(sourceTypes, probeOutputCols,
        probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount,
        buildOutputTypes, JoinType::OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr, overflowConfig);
    std::vector<OperatorFactory *> operatorfactories = { hashBuilderFactory, lookupJoinFactory };
    return operatorfactories;
}

OperatorFactory *CreateDistinctLimitFactory(omniruntime::type::DataTypes &sourceTypes, int32_t loopCount)
{
    const int64_t limitCount = loopCount - 1;
    int32_t distinctCols[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    auto operatorFactory =
        DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes, distinctCols, 10, -1, limitCount);
    return operatorFactory;
}

OperatorFactory *CreateProjectFactory(omniruntime::type::DataTypes &sourceTypes, std::vector<Expr *> exprs,
    OverflowConfig *overflowConfig)
{
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, sourceTypes, overflowConfig);
    auto operatorFactory = new ProjectionOperatorFactory(move(exprEvaluator));
    return operatorFactory;
}

OperatorFactory *CreateUnionFactory(omniruntime::type::DataTypes &sourceTypes)
{
    auto operatorFactory = UnionOperatorFactory::CreateUnionOperatorFactory(sourceTypes, sourceTypes.GetSize(), false);
    return operatorFactory;
}

omniruntime::op::Operator *CreateSortMergeJoinOperator(omniruntime::type::DataTypes &sourceTypes,
    OverflowConfig *overflowConfig)
{
    std::string blank = "";
    auto smjOp = new SortMergeJoinOperator(JoinType::OMNI_JOIN_TYPE_INNER, blank);
    DataTypes streamedTblTypes(sourceTypes);
    std::vector<int32_t> streamedKeysCols;
    streamedKeysCols.push_back(1);
    std::vector<int32_t> streamedOutputCols;
    streamedOutputCols.push_back(2);
    smjOp->ConfigStreamedTblInfo(streamedTblTypes, streamedKeysCols, streamedOutputCols, sourceTypes.GetSize());

    DataTypes bufferedTblTypes(sourceTypes);
    std::vector<int32_t> bufferedKeysCols;
    bufferedKeysCols.push_back(2);
    std::vector<int32_t> bufferedOutputCols;
    bufferedOutputCols.push_back(1);
    smjOp->ConfigBufferedTblInfo(bufferedTblTypes, bufferedKeysCols, bufferedOutputCols, sourceTypes.GetSize());
    smjOp->InitScannerAndResultBuilder(overflowConfig);
    return smjOp;
}

OperatorFactory *CreateTopNFactory(omniruntime::type::DataTypes &sourceTypes)
{
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    auto operatorFactory = new TopNOperatorFactory(sourceTypes, 5, sortCols, ascendings, nullFirsts, 1);
    return operatorFactory;
}

OperatorFactory *CreateWindowFactory(omniruntime::type::DataTypes &sourceTypes)
{
    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {2};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_AVG};
    int32_t partitionCols[1] = {1};
    int32_t preGroupedCols[0] = {};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    int32_t argumentChannels[1] = {2};
    DataTypes allTypes(std::vector<DataTypePtr>({ ShortType(), IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(SHORT_DECIMAL_SIZE, 0), Decimal128Type(LONG_DECIMAL_SIZE, 0),
        VarcharType(CHAR_SIZE), CharType(CHAR_SIZE), DoubleType() }));

    // dealing data with the operator
    auto operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols, 9,
        windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, true);
    return operatorFactory;
}
}