/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: dt fuzz factory create utils implementations
 */
#ifndef OMNI_RUNTIME_DT_FUZZ_FACTORY_CREATE_UTIL_H
#define OMNI_RUNTIME_DT_FUZZ_FACTORY_CREATE_UTIL_H
#include "operator/join/sortmergejoin/sort_merge_join_expr_v3.h"
using namespace omniruntime::op;
using namespace omniruntime::expressions;


namespace DtFuzzFactoryCreateUtil {
OperatorFactory *CreateFilterFactory(omniruntime::type::DataTypes &sourceTypes, BinaryExpr *filterExpr,
    std::vector<Expr *> projections, OverflowConfig *overflowConfig);

OperatorFactory *CreateSortFactory(omniruntime::type::DataTypes &sourceTypes);

OperatorFactory *CreateAggregationFactory(omniruntime::type::DataTypes &sourceTypes);

OperatorFactory *CreateHashAggregationFactory(omniruntime::type::DataTypes &sourceTypes);

std::vector<OperatorFactory *> CreateHashJoinFactory(omniruntime::type::DataTypes &sourceTypes,
    OverflowConfig *overflowConfig, std::string filterExpr, int32_t operatorCount);

std::vector<OperatorFactory *> CreateLookupOuterFactory(omniruntime::type::DataTypes &sourceTypes,
    OverflowConfig *overflowConfig, std::string filterExpr, int32_t operatorCount);

OperatorFactory *CreateDistinctLimitFactory(omniruntime::type::DataTypes &sourceTypes, int32_t loopCount);

OperatorFactory *CreateProjectFactory(omniruntime::type::DataTypes &sourceTypes, std::vector<Expr *> exprs,
    OverflowConfig *overflowConfig);

OperatorFactory *CreateUnionFactory(omniruntime::type::DataTypes &sourceTypes);

omniruntime::op::Operator *CreateSortMergeJoinOperator(omniruntime::type::DataTypes &sourceTypes,
    OverflowConfig *overflowConfig);

OperatorFactory *CreateTopNFactory(omniruntime::type::DataTypes &sourceTypes);

OperatorFactory *CreateWindowFactory(omniruntime::type::DataTypes &sourceTypes);

OperatorFactory *CreateTopNSortFactory(omniruntime::type::DataTypes &sourceTypes,
    std::vector<omniruntime::expressions::Expr *> partitionKeys, std::vector<omniruntime::expressions::Expr *> sortKeys,
    int32_t dataSize);
OperatorFactory *CreateStreamedWithExprOperatorFactory();
OperatorFactory *CreateSortMergeJoinV3Factory(StreamedTableWithExprOperatorFactoryV3 *streamedWithExprOperatorFactory);
}

#endif // OMNI_RUNTIME_DT_FUZZ_FACTORY_CREATE_UTIL_H
