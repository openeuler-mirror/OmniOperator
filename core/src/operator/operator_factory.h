/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
};
}
}

#endif // __OMNI_OPERATOR_FACTORY_H__
