/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 * Description:
 */
#include "jsonparser.h"
#include <vector>
#include <fstream> // for testing

using namespace std;

using namespace omniruntime::expressions;

using Json = nlohmann::json;

OperatorType JSONParser::GetOperatorType(Operator op)
{
    vector<Operator> allCmpOps { LT, LTE, GT, GTE, EQ, NEQ };
    vector<Operator> allLogOps { AND, OR, NOT };
    vector<Operator> allArithOps { ADD, SUB, MUL, DIV, MOD };
    for (Operator cmpOp : allCmpOps) {
        if (op == cmpOp) {
            return OperatorType::COMPARISON;
        }
    }
    for (Operator logOp : allLogOps) {
        if (op == logOp) {
            return OperatorType::LOGICAL;
        }
    }
    for (Operator arithOp : allArithOps) {
        if (op == arithOp) {
            return OperatorType::ARITHMETIC;
        }
    }
    return OperatorType::INVALIDOPTTYPE;
}


Expr *JSONParser::ParseJSONFieldRef(Json jsonExpr)
{
    VecTypeId typeId = static_cast<VecTypeId>(jsonExpr["dataType"].get<int32_t>());
    VecTypePtr retType;
    auto colVal = jsonExpr["colVal"].get<int32_t>();
    if (TypeUtil::IsStringType(typeId)) {
        int width = jsonExpr["width"].get<int32_t>();
        if (typeId == OMNI_VEC_TYPE_CHAR) {
            retType = make_unique<CharVecType>(width);
        } else {
            retType = make_unique<VarcharVecType>(width);
        }
    } else if (TypeUtil::IsDecimalType(typeId)) {
        int precision = jsonExpr["precision"].get<int32_t>();
        int scale = jsonExpr["scale"].get<int32_t>();
        if (typeId == OMNI_VEC_TYPE_DECIMAL64) {
            retType = make_unique<Decimal64VecType>(precision, scale);
        } else {
            retType = make_unique<Decimal128VecType>(precision, scale);
        }
    } else {
            retType = make_unique<VecType>(typeId);
    }
    return make_unique<FieldExpr>(colVal, std::move(retType)).release();
}

Expr *JSONParser::ParseJSONLiteral(Json jsonExpr)
{
    VecTypeId typeId = static_cast<VecTypeId>(jsonExpr["dataType"].get<int32_t>());

    // Null check on Literals
    if (jsonExpr["isNull"].get<bool>()) {
        auto expr = ParserHelper::GetDefaultValueForType(typeId);
        expr->isNull = true;
        return expr;
    }
    // proceed with non-null value
    if (typeId == OMNI_VEC_TYPE_BOOLEAN) {
        bool boolVal = jsonExpr["value"].get<bool>();
        return make_unique<LiteralExpr>(boolVal, make_unique<BooleanVecType>()).release();
    } else if (typeId == OMNI_VEC_TYPE_INT) {
        auto intVal = jsonExpr["value"].get<int32_t>();
        return make_unique<LiteralExpr>(intVal, make_unique<IntVecType>()).release();
    } else if (typeId == OMNI_VEC_TYPE_DATE32) {
        auto intVal = jsonExpr["value"].get<int32_t>();
        return make_unique<LiteralExpr>(intVal, make_unique<VecType>(OMNI_VEC_TYPE_DATE32)).release();
    } else if (typeId == OMNI_VEC_TYPE_LONG) {
        auto longVal = jsonExpr["value"].get<int64_t>();
        return make_unique<LiteralExpr>(longVal, make_unique<LongVecType>()).release();
    } else if (typeId == OMNI_VEC_TYPE_DOUBLE) {
        auto doubleVal = jsonExpr["value"].get<double>();
        return make_unique<LiteralExpr>(doubleVal, make_unique<DoubleVecType>()).release();
    } else if (typeId == OMNI_VEC_TYPE_DECIMAL64) {
        auto decimalVal = jsonExpr["value"].get<int64_t>();
        return make_unique<LiteralExpr>(decimalVal, make_unique<Decimal64VecType>(jsonExpr["precision"].get<int32_t>(),
                jsonExpr["scale"].get<int32_t>())).release();
    } else if (typeId == OMNI_VEC_TYPE_DECIMAL128) {
        // FIXME: Currently only support up to long; Support 128 bits in the future
        auto decimalVal = jsonExpr["value"].get<int64_t>();
        return make_unique<LiteralExpr>(decimalVal, make_unique<Decimal128VecType>(jsonExpr["precision"].get<int32_t>(),
                jsonExpr["scale"].get<int32_t>())).release();
    } else if (typeId == OMNI_VEC_TYPE_NONE) {
        return make_unique<LiteralExpr>(0, make_unique<VecType>()).release();
    } else {
        string *stringVal = make_unique<string>(jsonExpr["value"].get<string>()).release();
        int32_t width = jsonExpr["width"].get<int32_t>();
        return make_unique<LiteralExpr>(stringVal, make_unique<VarcharVecType>(width)).release();
    }
}

Expr *JSONParser::ParseJSONBinary(Json jsonExpr)
{
    Operator op = StringToOperator(jsonExpr["operator"].get<string>());
    Expr *leftExpr = ParseJSON(jsonExpr["left"]);
    if (leftExpr == nullptr) {
        return nullptr;
    }
    Expr *rightExpr = ParseJSON(jsonExpr["right"]);
    if (rightExpr == nullptr) {
        return nullptr;
    }

    OperatorType retType = GetOperatorType(op);
    if (retType == COMPARISON || retType == LOGICAL) {
        return make_unique<BinaryExpr>(op, leftExpr, rightExpr, make_unique<BooleanVecType>()).release();
    }
    return make_unique<BinaryExpr>(op, leftExpr, rightExpr).release();
}

Expr *JSONParser::ParseJSONUnary(Json jsonExpr)
{
    Operator op = StringToOperator(jsonExpr["operator"].get<string>());
    Expr *expr = ParseJSON(jsonExpr["expr"]);
    if (expr == nullptr) {
        return nullptr;
    }
    return make_unique<UnaryExpr>(op, expr, make_unique<BooleanVecType>()).release();
}

Expr *JSONParser::ParseJSONIn(Json jsonExpr)
{
    std::vector<Expr *> args;
    for (const auto &item : jsonExpr["arguments"].items()) {
        Expr *arg = ParseJSON(item.value());
        if (arg) {
            args.push_back(arg);
        } else {
            return nullptr;
        }
    }
    return make_unique<InExpr>(args).release();
}

Expr *JSONParser::ParseJSONBetween(Json jsonExpr)
{
    Expr *val = ParseJSON(jsonExpr["value"]);
    Expr *lowBoundExpr = ParseJSON(jsonExpr["lower_bound"]);
    if (lowBoundExpr == nullptr) {
        return nullptr;
    }
    Expr *upBoundExpr = ParseJSON(jsonExpr["upper_bound"]);
    if (upBoundExpr == nullptr) {
        return nullptr;
    }

    return make_unique<BetweenExpr>(val, lowBoundExpr, upBoundExpr).release();
}

Expr *JSONParser::ParseJSONIf(Json jsonExpr)
{
    Expr *cond = ParseJSON(jsonExpr["condition"]);
    if (cond == nullptr) {
        return nullptr;
    }
    Expr *trueExpr = ParseJSON(jsonExpr["if_true"]);
    if (trueExpr == nullptr) {
        return nullptr;
    }
    Expr *falseExpr = ParseJSON(jsonExpr["if_false"]);
    if (falseExpr == nullptr) {
        return nullptr;
    }
    if (TypeUtil::IsStringType(falseExpr->GetReturnTypeId())
        && falseExpr->GetType() == ExprType::LITERAL_E
        && static_cast<LiteralExpr*>(falseExpr)->stringVal->compare("null") == 0) {
        return make_unique<IfExpr>(cond, trueExpr, ParserHelper().GetDefaultValueForType(trueExpr->GetReturnTypeId())).release();
    }

    return make_unique<IfExpr>(cond, trueExpr, falseExpr).release();
}

Expr *JSONParser::ParseJSONCoalesce(Json jsonExpr)
{
    Expr *val1 = ParseJSON(jsonExpr["value1"]);
    if (val1 == nullptr) {
        return nullptr;
    }
    Expr *val2 = ParseJSON(jsonExpr["value2"]);
    if (val2 == nullptr) {
        return nullptr;
    }

    return make_unique<CoalesceExpr>(val1, val2).release();
}

Expr *JSONParser::ParseJsonIsNull(Json jsonExpr)
{ // Is_Null support
    Expr *val = ParseJSON(jsonExpr["arguments"].at(0));
    if (val == nullptr) {
        return nullptr;
    }
    return make_unique<IsNullExpr>(val).release();
}

Expr *JSONParser::ParseJsonIsNotNull(Json jsonExpr)
{   // Is_Not_Null support
    VecTypeId typeId = static_cast<VecTypeId>(jsonExpr["returnType"].get<int32_t>());
    Expr *val = ParseJSON(jsonExpr["arguments"].at(0));
    return make_unique<UnaryExpr>(Operator::NOT, val, make_unique<VecType>(typeId)).release();
}

Expr *JSONParser::ParseJSONFunc(Json jsonExpr)
{
    string funcName = jsonExpr["function_name"];
    VecTypeId retTypeId = static_cast<VecTypeId>(jsonExpr["returnType"].get<int32_t>());
    std::vector<Expr *> args;
    VecTypePtr retType;
    int32_t width = INT32_MAX;
    int32_t precision;
    int32_t scale;

    for (const auto &item : jsonExpr["arguments"].items()) {
        Expr *arg = ParseJSON(item.value());
        if (arg) {
            args.push_back(arg);
        } else {
            return nullptr;
        }
    }
    // CAST short-circuit - Convert CAST function of a type to its own type to DataExpr
        if (funcName == "CAST" && args.size() == 1 && retTypeId == args[0]->GetReturnTypeId()) {
        if (args[0]->GetType() == LITERAL_E) {
            return static_cast<LiteralExpr *>(args[0]);
        } else if (args[0]->GetType() == FIELD_E) {
            return static_cast<FieldExpr *>(args[0]);
        } else {
            return nullptr;
        }
    }
    // Check that the signature matches
    vector<VecTypeId> argTypes(args.size());
    std::transform(args.begin(), args.end(), argTypes.begin(), [](Expr *expr) -> VecTypeId {return expr->GetReturnTypeId();});
    auto signature = FunctionSignature(funcName, argTypes, retTypeId);
    auto function = omniruntime::FunctionRegistry::LookupFunction(&signature);
    if (TypeUtil::IsStringType(retTypeId)) {
            width = jsonExpr.contains("width") ? jsonExpr["width"].get<int32_t>() : width;
            if (retTypeId == OMNI_VEC_TYPE_CHAR) {
                retType = make_unique<CharVecType>(width);
            } else {
                retType = make_unique<VarcharVecType>(width);
            }
        } else if (TypeUtil::IsDecimalType(retTypeId)) {
            precision = jsonExpr["precision"].get<int32_t>();
            scale = jsonExpr["scale"].get<int32_t>();
            if (retTypeId == OMNI_VEC_TYPE_DECIMAL64) {
                retType = make_unique<Decimal64VecType>(precision, scale);
            } else {
                retType = make_unique<Decimal128VecType>(precision, scale);
            }
        } else {
            retType = make_unique<VecType>(retTypeId);
        }if (function != nullptr) {
        return make_unique<FuncExpr>(funcName, args, std::move(retType), function).release();
    }

    // if operator is not supported, return nullptr
    return nullptr;
}

Expr *JSONParser::ParseJSON(Json jsonExpr)
{
    string exprTypeStr = jsonExpr["exprType"].get<string>();
    if (exprTypeStr == "FIELD_REFERENCE") {
        return ParseJSONFieldRef(jsonExpr);
    } else if (exprTypeStr == "LITERAL") {
        return ParseJSONLiteral(jsonExpr);
    } else if (exprTypeStr == "BINARY") {
        return ParseJSONBinary(jsonExpr);
    } else if (exprTypeStr == "UNARY") {
        return ParseJSONUnary(jsonExpr);
    } else if (exprTypeStr == "IN") {
        return ParseJSONIn(jsonExpr);
    } else if (exprTypeStr == "BETWEEN") {
        return ParseJSONBetween(jsonExpr);
    } else if (exprTypeStr == "IF") {
        return ParseJSONIf(jsonExpr);
    } else if (exprTypeStr == "COALESCE") {
        return ParseJSONCoalesce(jsonExpr);
    } else if (exprTypeStr == "IS_NULL") {
        return ParseJsonIsNull(jsonExpr);
    } else if (exprTypeStr == "FUNC" || exprTypeStr == "FUNCTION") {
        return ParseJSONFunc(jsonExpr);
    }
    // return nullptr if ExprType not supported
    return nullptr;
}

std::vector<omniruntime::expressions::Expr *> JSONParser::ParseJSON(nlohmann::json *expressions,
    int32_t numberOfExpressions)
{
    std::vector<Expr *> result;
    for (int32_t i = 0; i < numberOfExpressions; i++) {
        Expr *expr = ParseJSON(expressions[i]);
        if (expr == nullptr) {
            std::cout<<"The "<<i<<"th expression is not supported: "<<std::endl<<expressions[i].dump(1)<<std::endl;
            return { nullptr };
        }
        result.push_back(expr);
    }
    return result;
}