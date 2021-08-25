/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;

namespace {
    const int32_t ARGS_2 = 2;
    const int32_t ARGS_3 = 3;
}

ParserHelper::~ParserHelper() {
}

// Update with function return type every time a new function is added
DataType ParserHelper::FuncRetTypeMap(string fnName, vector<Expr *> args)
{
    if (fnName == "CAST") {
        if (args[0]->dataType == STRINGD) {
            return INT32D;
        } else {
            return DOUBLED;
        }
    }
    if (fnName == "substr") {
        return STRINGD;
    }
    if (fnName == "concat") {
        return STRINGD;
    }
    if (fnName == "abs") {
        return args[0]->dataType;
    }
    if (fnName == "LIKE") {
        return BOOLD;
    }
    if (fnName == "combine_hash") {
        return INT64D;
    }
    if (externalFuncNames.find(fnName) != externalFuncNames.end()) {
        return externalFuncRetTypeMap[fnName];
    }
    return INVALIDDATAD;
}

// Helper function to determine if a type is INT32D or INT64D
bool ParserHelper::IsIntType(DataType dt) const
{
    return (dt == INT32D || dt == INT64D);
}

// Update with conditions every time a new function is added
bool ParserHelper::FuncDeclMatch(string fnName, vector<Expr *> args, bool checkTypes)
{
    if (fnName == "CAST" && args.size() == 1) {
        return true;
    } else if (fnName == "substr" && (args.size() == ARGS_2 || args.size() == ARGS_3)) {
        return (args[0]->dataType == STRINGD &&
            IsIntType(args[1]->dataType) &&
            (args.size() == ARGS_2 || IsIntType(args[ARGS_2]->dataType)));
    } else if (fnName == "concat" && args.size() == ARGS_2) {
        return (args[0]->dataType == STRINGD && args[1]->dataType == STRINGD);
    } else if (fnName == "abs" && args.size() == 1) {
        return (args[0]->dataType == INT32D || args[0]->dataType == INT64D || args[0]->dataType == DOUBLED);
    } else if (fnName == "LIKE" && args.size() == ARGS_2) {
        // Assuming that like patterns are represented with strings
        return (args[0]->dataType == STRINGD && args[0]->dataType == STRINGD);
    } else if (fnName == "combine_hash" && args.size() == ARGS_2) {
        return (IsIntType(args[0]->dataType) && IsIntType(args[1]->dataType));
    } else if (externalFuncNames.find(fnName) != externalFuncNames.end()) {
        // Don't do any other checks for now for external functions
        return true;
    }
    return false;
}