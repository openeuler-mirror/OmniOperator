/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Filter compiler source file
 */
#include "filter_compiler.h"

namespace omniruntime {
namespace op {
using namespace std;
using namespace omniruntime::expressions;

unique_ptr<Filter> Compiler::Compile() const
{
    vector<DataType> dataTypes;
    for (int32_t i = 0; i < vecCount; i++) {
        dataTypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    auto codeGenObj = make_unique<FilterCodeGen>("comparisonFunc", *expression, dataTypes);
    return make_unique<Filter>(std::move(codeGenObj), *expression);
}
} // end of op
} // end of omniruntime
