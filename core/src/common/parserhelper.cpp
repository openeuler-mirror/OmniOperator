/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "parserhelper.h"

using namespace std;
using namespace omniruntime::expressions;

namespace {
    const int32_t ARGS_2 = 2;
}

ParserHelper::~ParserHelper() {
}

// Helper function to determine if a type is INT32D or INT64D
bool ParserHelper::IsIntType(DataType dt)
{
    return (dt == INT32D || dt == INT64D);
}

bool IsStringFn(std::string opStr)
{
    if (opStr == "substr" || opStr == "concat" || opStr == "LIKE") {
        return true;
    } else {
        return false;
    }
}

std::string ParserHelper::GetFnIdentifier(std::string opStr, std::vector<omniruntime::expressions::Expr*> args,
                                          omniruntime::expressions::DataType ret)
{
    std::string delimeter = "_";
    std::string funcID = opStr;
    std::string funcName = opStr;

    // if base function is a string function
    if (IsStringFn(opStr)) {
        if (opStr == "concat") {
            // unique ID for concat_char(requires width) and concat_string(no width)
            funcID += delimeter + DataTypeString(args[0]->GetExprDataType());
        } else if (opStr == "substr") {
            // unique ID for substr and substr with start index (substr_start)
            if (args.size() == ARGS_2) {
                funcID += delimeter + "start";
                funcName = funcID;
            }
            // unique ID for substr functions with length arg of long dt
            funcID += delimeter + DataTypeString(args.back()->GetExprDataType());
        }
    } else if (externalFuncNames.find(opStr) == externalFuncNames.end()) {
        // get funcIDs for built-in functions
        for (auto &argument: args) {
            funcID += delimeter + DataTypeString(argument->GetExprDataType());
        }
        funcID += delimeter + DataTypeString(ret);
    }

    // check if arguments number of arguments and argument data type match function
    if (HasValidArguments(funcName, args)) {
        return funcID;
    } else {
        return {};
    }
}

// Update with conditions every time a new function is added
bool ParserHelper::HasValidArguments(const string &fnName, vector<Expr *> args)
{
    // check if number of arguments match for given function
    if (FUNC_TO_NUM_ARGS.find(fnName)->second == args.size()) {
        // check datatypes of arguments
        if (fnName == "substr") {
            return ((args[0]->dataType == VARCHARD || args[0]->dataType == CHARD) &&
                    IsIntType(args[1]->dataType) && IsIntType(args[ARGS_2]->dataType));
        } else if (fnName == "substr_start") {
            return ((args[0]->dataType == VARCHARD || args[0]->dataType == CHARD) &&
                    IsIntType(args[1]->dataType));
        } else if (fnName == "concat") {
            return (args[0]->dataType == VARCHARD || args[0]->dataType == CHARD)
                   && (args[1]->dataType == VARCHARD || args[1]->dataType == CHARD);
        } else if (fnName == "abs") {
            return (args[0]->dataType == INT32D || args[0]->dataType == INT64D
                    || args[0]->dataType == DOUBLED || args[0]->dataType == DECIMAL128D);
        } else if (fnName == "LIKE") {
            // Assuming that like patterns are represented with strings
            return ((args[0]->dataType == VARCHARD || args[0]->dataType == CHARD) && args[1]->dataType == VARCHARD);
        } else if (fnName == "combine_hash") {
            return (IsIntType(args[0]->dataType) && IsIntType(args[1]->dataType));
        } else if (fnName == "pmod") {
            return (args[0]->dataType == INT32D && args[1]->dataType == INT32D);
        } else {
            return true;
        }
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
            return std::make_unique<DataExpr>(make_unique<string>("NULL").release()).release();
        case DataType::DECIMAL128D:
            return std::make_unique<DataExpr>(0L).release();
        case DataType::UNKNOWND:
            return std::make_unique<DataExpr>(0).release();
        default:
            return nullptr;
    }
}