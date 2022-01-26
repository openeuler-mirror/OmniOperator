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
    DataType dt = OrdinalToDataType(jsonExpr["dataType"].get<int32_t>());
    int32_t colVal = jsonExpr["colVal"].get<int32_t>();
    if (CHARD == dt || VARCHARD == dt) {
        int width = jsonExpr["width"].get<int32_t>();
        return make_unique<DataExpr>(colVal, dt, width).release();
    }
    return make_unique<DataExpr>(colVal, dt).release();
}

Expr *JSONParser::ParseJSONLiteral(Json jsonExpr)
{
    DataType dt = OrdinalToDataType(jsonExpr["dataType"].get<int32_t>());
    // Null check on Literals
    if (jsonExpr["isNull"].get<bool>()) {
        auto expr = ParserHelper::GetDefaultValueForType(dt);
        expr->isNull = true;
        return expr;
    }
    // proceed with non-null value
    if (dt == BOOLD) {
        bool boolVal = jsonExpr["value"].get<bool>();
        return make_unique<DataExpr>(boolVal).release();
    } else if (dt == INT32D) {
        int32_t intVal = jsonExpr["value"].get<int32_t>();
        return make_unique<DataExpr>(intVal).release();
    } else if (dt == INT64D) {
        int64_t longVal = jsonExpr["value"].get<int64_t>();
        return make_unique<DataExpr>(longVal).release();
    } else if (dt == DOUBLED) {
        double doubleVal = jsonExpr["value"].get<double>();
        return make_unique<DataExpr>(doubleVal).release();
    } else if (dt == DECIMAL64D) {
        int64_t decimalVal = jsonExpr["value"].get<int64_t>();
        return make_unique<DataExpr>(decimalVal).release();
    } else if (dt == DECIMAL128D) {
        // FIXME: Currently only support up to long; Support 128 bits in the future
        int64_t decimalVal = jsonExpr["value"].get<int64_t>();
        return make_unique<DataExpr>(decimalVal).release();
    } else if (dt == UNKNOWND) {
        return make_unique<DataExpr>(0).release();
    } else {
        string *stringVal = make_unique<string>(jsonExpr["value"].get<string>()).release();
        return make_unique<DataExpr>(stringVal).release();
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
        return make_unique<BinaryExpr>(op, leftExpr, rightExpr, DataType::BOOLD).release();
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
    return make_unique<UnaryExpr>(op, expr, DataType::BOOLD).release();
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

    if ((falseExpr->GetExprDataType() == VARCHARD || falseExpr->GetExprDataType() == CHARD) &&
        falseExpr->GetType() == ExprType::DATA_E &&
        static_cast<DataExpr *>(falseExpr)->stringVal->compare("null") == 0) {
        return make_unique<IfExpr>(cond, trueExpr, ParserHelper().GetDefaultValueForType(trueExpr->dataType)).release();
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

Expr *JSONParser::ParseJSONFunc(Json jsonExpr)
{
    string funcName = jsonExpr["function_name"];
    DataType retType = OrdinalToDataType(jsonExpr["returnType"].get<int32_t>());
    std::vector<Expr *> args;
    int32_t width = INT32_MAX;

    for (const auto &item : jsonExpr["arguments"].items()) {
        Expr *arg = ParseJSON(item.value());
        if (arg) {
            args.push_back(arg);
        } else {
            return nullptr;
        }
    }
    // CAST short-circuit - Convert CAST function of a type to its own type to DataExpr
    if (funcName == "CAST" && args.size() == 1 && retType == args[0]->dataType) {
        return static_cast<DataExpr *>(args[0]);
    }
    if (retType == CHARD) {
        width = jsonExpr["width"].get<int32_t>();
    }
    // Check that the signature matches
    std::string funcID = ParserHelper().GetFnIdentifier(funcName, args, retType);
    if (!funcID.empty()) {
        auto function = FunctionRegistry::LookupFunction(funcID);
        if (function != nullptr) {
            return make_unique<FuncExpr>(funcName, args, retType, width, *function).release();
        }
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