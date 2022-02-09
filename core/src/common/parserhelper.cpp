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
bool ParserHelper::IsIntType(VecTypeId typeId)
{
    return (typeId == OMNI_VEC_TYPE_INT || typeId == OMNI_VEC_TYPE_LONG || typeId == OMNI_VEC_TYPE_DATE32);
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
                                          VecTypeId retTypeId)
{
    std::string delimeter = "_";
    std::string funcID = opStr;
    std::string funcName = opStr;

    // if base function is a string function
    if (IsStringFn(opStr)) {
        if (opStr == "concat") {
            // unique ID for concat_char(requires width) and concat_string(no width)
            funcID += delimeter + TypeUtil::TypeToString(args[0]->GetReturnTypeId());
        } else if (opStr == "substr") {
            // unique ID for substr and substr with start index (substr_start)
            if (args.size() == ARGS_2) {
                funcID += delimeter + "start";
                funcName = funcID;
            }
            // unique ID for substr functions with length arg of long dt
            funcID += delimeter + TypeUtil::TypeToString(args.back()->GetReturnTypeId());
        }
    } else if (externalFuncNames.find(opStr) == externalFuncNames.end()) {
        // get funcIDs for built-in functions
        for (auto &argument: args) {
            funcID += delimeter + TypeUtil::TypeToString(argument->GetReturnTypeId());
        }
        funcID += delimeter + TypeUtil::TypeToString(retTypeId);
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
            return ((args[0]->GetReturnTypeId() == OMNI_VEC_TYPE_VARCHAR
            || args[0]->GetReturnTypeId() == OMNI_VEC_TYPE_CHAR)
                    && IsIntType(args[1]->GetReturnTypeId()) && IsIntType(args[ARGS_2]->GetReturnTypeId()));
        } else if (fnName == "substr_start") {
            return (TypeUtil::IsStringType(args[0]->GetReturnTypeId()) &&
                    IsIntType(args[1]->GetReturnTypeId()));
        } else if (fnName == "concat") {
            return (TypeUtil::IsStringType(args[0]->GetReturnTypeId())
                   && TypeUtil::IsStringType(args[1]->GetReturnTypeId()));
        } else if (fnName == "abs") {
            return (IsIntType(args[0]->GetReturnTypeId())
                    || args[0]->GetReturnTypeId() == OMNI_VEC_TYPE_DOUBLE
                    || args[0]->GetReturnTypeId() == OMNI_VEC_TYPE_DECIMAL128);
        } else if (fnName == "LIKE") {
            // Assuming that like patterns are represented with strings
            return (TypeUtil::IsStringType(args[0]->GetReturnTypeId())
            && args[1]->GetReturnTypeId() == OMNI_VEC_TYPE_VARCHAR);
        } else if (fnName == "combine_hash") {
            return (IsIntType(args[0]->GetReturnTypeId()) && IsIntType(args[1]->GetReturnTypeId()));
        } else if (fnName == "pmod") {
            return (args[0]->GetReturnTypeId() == OMNI_VEC_TYPE_INT && args[1]->GetReturnTypeId() == OMNI_VEC_TYPE_INT);
        } else {
            return true;
        }
    }
    return false;
}

omniruntime::expressions::LiteralExpr *ParserHelper::GetDefaultValueForType(VecTypeId destTypeId)
{
    VecTypePtr destType = make_unique<VecType>(destTypeId);
    switch (destTypeId) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return std::make_unique<LiteralExpr>(0, std::move(destType)).release();
        case OMNI_VEC_TYPE_LONG:
            return std::make_unique<LiteralExpr>(0L, std::move(destType)).release();
        case OMNI_VEC_TYPE_DOUBLE:
            return std::make_unique<LiteralExpr>(0.000, std::move(destType)).release();
        case OMNI_VEC_TYPE_BOOLEAN:
            return std::make_unique<LiteralExpr>(true, std::move(destType)).release();
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            return std::make_unique<LiteralExpr>(make_unique<string>("NULL").release(), std::move(destType)).release();
        case OMNI_VEC_TYPE_DECIMAL128:
            return std::make_unique<LiteralExpr>(0L, std::move(destType)).release();
        case OMNI_VEC_TYPE_NONE:
            return std::make_unique<LiteralExpr>(0, std::move(destType)).release();
        default:
            return nullptr;
    }
}