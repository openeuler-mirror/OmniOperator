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

// Helper function to determine if a type is INT32D or INT64D
bool ParserHelper::IsIntType(DataType dt)
{
    return (dt == INT32D || dt == INT64D);
}

// Update with conditions every time a new function is added
bool ParserHelper::FuncDeclMatch(const string& fnName, vector<Expr *> args, bool checkTypes)
{
    if (fnName == "CAST" && args.size() == 1) {
        return true;
    } else if (fnName == "substr" && (args.size() == ARGS_2 || args.size() == ARGS_3)) {
        return ((args[0]->dataType == VARCHARD || args[0]->dataType == CHARD) &&
                IsIntType(args[1]->dataType) &&
                (args.size() == ARGS_2 || IsIntType(args[ARGS_2]->dataType)));
    } else if (fnName == "concat" && args.size() == ARGS_2) {
        return (args[0]->dataType == VARCHARD || args[0]->dataType == CHARD)
                   && (args[1]->dataType == VARCHARD || args[1]->dataType == CHARD);
    } else if (fnName == "abs" && args.size() == 1) {
        return (args[0]->dataType == INT32D || args[0]->dataType == INT64D
                   || args[0]->dataType == DOUBLED || args[0]->dataType == DECIMAL128D);
    } else if (fnName == "LIKE" && args.size() == ARGS_2) {
        // Assuming that like patterns are represented with strings
        return ((args[0]->dataType == VARCHARD || args[0]->dataType == CHARD) && args[1]->dataType == VARCHARD);
    } else if (fnName == "combine_hash" && args.size() == ARGS_2) {
        return (IsIntType(args[0]->dataType) && IsIntType(args[1]->dataType));
    } else if (fnName == "mm3hash" && args.size() == ARGS_2) {
        return true;
    } else if (externalFuncNames.find(fnName) != externalFuncNames.end()) {
        // Don't do any other checks for now for external functions
        return true;
    }
    return false;
}

omniruntime::expressions::DataExpr *ParserHelper::GetDefaultValueForType(omniruntime::expressions::DataType destType)
{
    switch (destType) {
        case DataType::INT32D:
            return std::make_unique<DataExpr>(0).release();
        case DataType::INT64D:
            return std::make_unique<DataExpr>(0L).release();
        case DataType::DOUBLED:
            return std::make_unique<DataExpr>(0.000).release();
        case DataType::BOOLD:
            return std::make_unique<DataExpr>(true).release();
        case DataType::CHARD:
        case DataType::VARCHARD:
            return std::make_unique<DataExpr>("NULL").release();
        case DataType::DECIMAL128D:
            return std::make_unique<DataExpr>(0L).release();
        case DataType::UNKNOWND:
            return std::make_unique<DataExpr>(0).release();
        default:
            return nullptr;
    }
}