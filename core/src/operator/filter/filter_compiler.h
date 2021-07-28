/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Filter compiler header
 */
#ifndef __FILTER_COMPILER_H__
#define __FILTER_COMPILER_H__

#include <memory>

#include "filter_and_project.h"

namespace omniruntime {
namespace op {
class Compiler {
public:
    // // todo getter functions
    Compiler(expressions::Expr *expression, int32_t *inputTypes, int32_t vecCount);
    ~Compiler() = default;
    std::unique_ptr<Filter> Compile() const;

private:
    expressions::Expr *expression;
    int32_t *inputTypes;
    int32_t vecCount;
};
} // end of op
} // end of omniruntime
#endif