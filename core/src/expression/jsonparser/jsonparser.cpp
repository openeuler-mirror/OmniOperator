/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 * Description:
 */
#include "jsonparser.h"
#include <vector>
#include <fstream> // for testing

using namespace std;
using namespace omniruntime::expressions;
using namespace omniruntime::type;

using Json = nlohmann::json;

Expr *JSONParser::ParseJSONFieldRef(Json jsonExpr)
{
    DataTypeId typeId = static_cast<DataTypeId>(jsonExpr["dataType"].get<int32_t>());
    DataTypePtr retType;
    auto colVal = jsonExpr["colVal"].get<int32_t>();
    if (TypeUtil::IsStringType(typeId)) {
        int width = jsonExpr["width"].get<int32_t>();
        if (typeId == OMNI_CHAR) {
            retType = make_unique<CharDataType>(width);
        } else {
            retType = make_unique<VarcharDataType>(width);
        }
    } else if (TypeUtil::IsDecimalType(typeId)) {
        int precision = jsonExpr["precision"].get<int32_t>();
        int scale = jsonExpr["scale"].get<int32_t>();
        if (typeId == OMNI_DECIMAL64) {
            retType = make_unique<Decimal64DataType>(precision, scale);
        } else {
            retType = make_unique<Decimal128DataType>(precision, scale);
        }
    } else {
        retType = make_unique<DataType>(typeId);
    }
    return new FieldExpr(colVal, std::move(retType));
}

Expr *JSONParser::ParseJSONLiteral(Json jsonExpr)
{
    DataTypeId typeId = static_cast<DataTypeId>(jsonExpr["dataType"].get<int32_t>());
    // Null check on Literals
    if (jsonExpr["isNull"].get<bool>()) {
        auto expr = ParserHelper::GetDefaultValueForType(typeId);
        expr->isNull = true;
        return expr;
    }
    // proceed with non-null value
    if (typeId == OMNI_BOOLEAN) {
        bool boolVal = jsonExpr["value"].get<bool>();
        return new LiteralExpr(boolVal, make_unique<BooleanDataType>());
    } else if (typeId == OMNI_INT) {
        auto intVal = jsonExpr["value"].get<int32_t>();
        return new LiteralExpr(intVal, make_unique<IntDataType>());
    } else if (typeId == OMNI_DATE32) {
        auto intVal = jsonExpr["value"].get<int32_t>();
        return new LiteralExpr(intVal, make_unique<DataType>(OMNI_DATE32));
    } else if (typeId == OMNI_LONG) {
        auto longVal = jsonExpr["value"].get<int64_t>();
        return new LiteralExpr(longVal, make_unique<LongDataType>());
    } else if (typeId == OMNI_DOUBLE) {
        auto doubleVal = jsonExpr["value"].get<double>();
        return new LiteralExpr(doubleVal, make_unique<DoubleDataType>());
    } else if (typeId == OMNI_DECIMAL64) {
        auto decimalVal = jsonExpr["value"].get<int64_t>();
        return new LiteralExpr(decimalVal,
            make_unique<Decimal64DataType>(jsonExpr["precision"].get<int32_t>(), jsonExpr["scale"].get<int32_t>()));
    } else if (typeId == OMNI_DECIMAL128) {
        string *dec128String = new string(jsonExpr["value"].get<string>());
        return new LiteralExpr(dec128String,
            make_unique<Decimal128DataType>(jsonExpr["precision"].get<int32_t>(), jsonExpr["scale"].get<int32_t>()));
    } else if (typeId == OMNI_NONE) {
        return new LiteralExpr(0, make_unique<DataType>());
    } else {
        string *stringVal = new string(jsonExpr["value"].get<string>());
        int32_t width = jsonExpr["width"].get<int32_t>();
        return new LiteralExpr(stringVal, make_unique<VarcharDataType>(width));
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

    DataTypePtr dataType = ParserHelper::GetReturnDataType(jsonExpr);
    return new BinaryExpr(op, leftExpr, rightExpr, std::move(dataType));
}

Expr *JSONParser::ParseJSONUnary(Json jsonExpr)
{
    Operator op = StringToOperator(jsonExpr["operator"].get<string>());
    Expr *expr = ParseJSON(jsonExpr["expr"]);
    if (expr == nullptr) {
        return nullptr;
    }
    return new UnaryExpr(op, expr, make_unique<BooleanDataType>());
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
    return new InExpr(args);
}

Expr *JSONParser::ParseJSONBetween(Json jsonExpr)
{
    Expr *val = ParseJSON(jsonExpr["value"]);
    if (val == nullptr) {
        return nullptr;
    }
    Expr *lowBoundExpr = ParseJSON(jsonExpr["lower_bound"]);
    if (lowBoundExpr == nullptr) {
        return nullptr;
    }
    Expr *upBoundExpr = ParseJSON(jsonExpr["upper_bound"]);
    if (upBoundExpr == nullptr) {
        return nullptr;
    }

    return new BetweenExpr(val, lowBoundExpr, upBoundExpr);
}

Expr *JSONParser::ParseJSONSwitch(Json jsonExpr)
{
    Expr *elseExpr = ParseJSON(jsonExpr["else"]);
    if (elseExpr == nullptr) {
        return nullptr;
    }
    Expr *left = ParseJSON(jsonExpr["input"]);
    if (left == nullptr) {
        return nullptr;
    }
    int numOfCases = jsonExpr["numOfCases"].get<int>();
    std::vector<std::pair<Expr *, Expr *>> whenClause;

    for (int i = 0; i < numOfCases; i++) {
        std::pair<Expr *, Expr *> when;
        Expr *right = ParseJSON(jsonExpr["Case" + std::to_string(i + 1)]["when"]);
        if (right == nullptr) {
            return nullptr;
        }
        Expr *result = ParseJSON(jsonExpr["Case" + std::to_string(i + 1)]["result"]);
        if (result == nullptr) {
            return nullptr;
        }
        BinaryExpr *condition = new BinaryExpr(Operator::EQ, left, right, BooleanType());
        when = make_pair(condition, result);
        whenClause.push_back(when);
    }
    if (TypeUtil::IsStringType(elseExpr->GetReturnTypeId()) && elseExpr->GetType() == ExprType::LITERAL_E &&
        static_cast<LiteralExpr *>(elseExpr)->stringVal->compare("null") == 0) {
        return new SwitchExpr(whenClause, ParserHelper::GetDefaultValueForType(elseExpr->GetReturnTypeId()));
    }
    return new SwitchExpr(whenClause, elseExpr);
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
    if (TypeUtil::IsStringType(falseExpr->GetReturnTypeId()) && falseExpr->GetType() == ExprType::LITERAL_E &&
        static_cast<LiteralExpr *>(falseExpr)->stringVal->compare("null") == 0) {
        return new IfExpr(cond, trueExpr, ParserHelper::GetDefaultValueForType(trueExpr->GetReturnTypeId()));
    }

    return new IfExpr(cond, trueExpr, falseExpr);
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

    return new CoalesceExpr(val1, val2);
}

Expr *JSONParser::ParseJsonIsNull(Json jsonExpr)
{ // Is_Null support
    Expr *val = ParseJSON(jsonExpr["arguments"].at(0));
    if (val == nullptr) {
        return nullptr;
    }
    return new IsNullExpr(val);
}

Expr *JSONParser::ParseJSONFunc(Json jsonExpr)
{
    string funcName = jsonExpr["function_name"];
    DataTypeId retTypeId = static_cast<DataTypeId>(jsonExpr["returnType"].get<int32_t>());
    std::vector<Expr *> args;
    DataTypePtr retType;
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
        if (args[0]->GetType() == ExprType::LITERAL_E) {
            return static_cast<LiteralExpr *>(args[0]);
        } else if (args[0]->GetType() == ExprType::FIELD_E) {
            return static_cast<FieldExpr *>(args[0]);
        } else {
            return nullptr;
        }
    }
    // Check that the signature matches
    vector<DataTypeId> argTypes(args.size());
    std::transform(args.begin(), args.end(), argTypes.begin(),
        [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    for (int i = 0; i < argTypes.size(); i++) {
        if (argTypes[i] == omniruntime::type::OMNI_DATE32) {
            argTypes[i] = omniruntime::type::OMNI_INT;
        }
    }
    auto signature = FunctionSignature(funcName, argTypes, retTypeId);
    auto function = omniruntime::FunctionRegistry::LookupFunction(&signature);
    if (TypeUtil::IsStringType(retTypeId)) {
        width = jsonExpr.contains("width") ? jsonExpr["width"].get<int32_t>() : width;
        if (retTypeId == OMNI_CHAR) {
            retType = make_unique<CharDataType>(width);
        } else {
            retType = make_unique<VarcharDataType>(width);
        }
    } else if (TypeUtil::IsDecimalType(retTypeId)) {
        precision = jsonExpr["precision"].get<int32_t>();
        scale = jsonExpr["scale"].get<int32_t>();
        if (retTypeId == OMNI_DECIMAL64) {
            retType = make_unique<Decimal64DataType>(precision, scale);
        } else {
            retType = make_unique<Decimal128DataType>(precision, scale);
        }
    } else {
        retType = make_unique<DataType>(retTypeId);
    }
    if (function != nullptr) {
        return new FuncExpr(funcName, args, std::move(retType), function);
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
    } else if (exprTypeStr == "SWITCH") {
        return ParseJSONSwitch(jsonExpr);
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