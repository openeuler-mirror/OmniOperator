/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __PARSERHELPER_H__
#define __PARSERHELPER_H__

#include <string>
#include <iostream>
#include <algorithm>
#include <memory>
#include "expressions.h"
#include "util/type_util.h"

class ParserHelper {
public:
    static omniruntime::expressions::LiteralExpr *GetDefaultValueForType(omniruntime::type::DataTypeId destTypeId,
        int32_t precision = 0, int32_t scale = 0);
    static omniruntime::type::DataTypePtr GetReturnDataType(nlohmann::json jsonExpr);
};

#endif