/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2023. All rights reserved.
 */

#ifndef __OMNI_OPERATOR_FACTORY_H__
#define __OMNI_OPERATOR_FACTORY_H__

#include "operator.h"

namespace omniruntime {
namespace op {
class OperatorFactory {
public:
    OperatorFactory() = default;

    virtual ~OperatorFactory() = default;

    virtual omniruntime::op::Operator *CreateOperator()
    {
        return nullptr;
    }

    virtual DataTypes *GetOutputDataTypes()
    {
        return nullptr;
    }
};

using OperatorType = enum OperatorType {
    OMNI_FILTER_AND_PROJECT = 0,
    OMNI_PROJECT,
    OMNI_LIMIT,
    OMNI_DISTINCT_LIMIT,
    OMNI_SORT,
    OMNI_TOPN,
    OMNI_AGGREGATION,
    OMNI_HASH_AGGREGATION,
    OMNI_WINDOW,
    OMNI_HASH_BUILDER,
    OMNI_LOOKUP_JOIN,
    OMNI_LOOKUP_OUTER_JOIN,
    OMNI_SMJ_BUFFER,
    OMNI_SMJ_STREAM,
    OMNI_PARTITIONED_OUTPUT,
    OMNI_UNION,
    OMNI_FUSION
};
}
}

#endif // __OMNI_OPERATOR_FACTORY_H__
