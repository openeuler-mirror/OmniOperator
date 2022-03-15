/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __PARSERHELPER_H__
#define __PARSERHELPER_H__

#include <string>
#include <set>
#include <iostream>
#include <algorithm>
#include <memory>
#include "expressions.h"
#include "util/type_util.h"

class ParserHelper {
public:
    static omniruntime::expressions::LiteralExpr *GetDefaultValueForType(omniruntime::vec::VecTypeId destTypeId);
};

#endif