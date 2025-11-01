/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ParquetExpression.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/array/builder_base.h"

// 将这个类里面的方法都修改成基于nlohmann::json&，逻辑不要改变
namespace omniruntime::reader {
using namespace arrow;

template <typename T>
T get_value_or_throw(const json& json, const std::string& key)
{
    try {
        return json.at(key).get<T>();
    } catch (const std::exception& e) {
        throw std::invalid_argument("Missing or invalid field '" + key + "': " + std::string(e.what()));
    }
}

Result<std::shared_ptr<DataType>> GetLiteralDataType(const json& json)
{
    int type = get_value_or_throw<int>(json, "type");
    switch (static_cast<PredicateDataType>(type)) {
    case PredicateDataType::Int:
        return arrow::int32();
    case PredicateDataType::Long:
        return arrow::int64();
    case PredicateDataType::Timestamp:
        return arrow::timestamp(arrow::TimeUnit::MICRO);
    case PredicateDataType::String:
        return arrow::utf8();
    case PredicateDataType::Date32:
        return arrow::date32();
    case PredicateDataType::Decimal: {
            int32_t precision = static_cast<int32_t>(get_value_or_throw<int>(json, "precision"));
            int32_t scale = static_cast<int32_t>(get_value_or_throw<int>(json, "scale"));
            return arrow::decimal(precision, scale);
    }
    case PredicateDataType::Bool:
        return arrow::boolean();
    case PredicateDataType::Short:
        return arrow::int16();
    case PredicateDataType::Double:
        return arrow::float64();
    default:
        return Status::Invalid("Unsupported literal data type " + std::to_string(type));
    }
}

inline Result<std::shared_ptr<Scalar>> ParseStringToScalar(const std::shared_ptr<DataType>& type, const std::string& str)
{
    if (type->id() == arrow::Type::DECIMAL128) {
        return std::make_shared<Decimal128Scalar>(Decimal128(str), type);
    }
    if (type->id() == arrow::Type::DECIMAL256) {
        return std::make_shared<Decimal256Scalar>(Decimal256(str), type);
    }
    return arrow::Scalar::Parse(type, str);
}

arrow::Result<Expression> GetSubExpr(const json& json, const std::string& sub)
{
    auto it = json.find(sub);
    if (it == json.end() || !it->is_object()) {
        return Status::Invalid("Missing or invalid sub-expression: " + sub);
    }
    return ParseToArrowExpression(*it);
}

arrow::Result<Expression> GetFieldExpr(const json& json)
{
    std::string field = get_value_or_throw<std::string>(json, "field");
    return arrow::compute::field_ref(field);
}

arrow::Result<Expression> GetLiteralExpr(const json& json)
{
    ARROW_ASSIGN_OR_RAISE(auto dataType, GetLiteralDataType(json));

    std::string str = get_value_or_throw<std::string>(json, "literal");

    ARROW_ASSIGN_OR_RAISE(auto scalar, ParseStringToScalar(dataType, str));
    return arrow::compute::literal(scalar);
}

arrow::Result<std::shared_ptr<Array>> GetSetLiteralExpr(const json& json)
{
    ARROW_ASSIGN_OR_RAISE(auto dataType, GetLiteralDataType(json));
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(dataType));

    auto it = json.find("literal");
    if (it == json.end() || !it->is_array()) {
        return Status::Invalid("Expected 'literal' to be an array");
    }

    for (const auto& elem : *it) {
        if (elem.is_null()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            std::string str = elem.get<std::string>();
            ARROW_ASSIGN_OR_RAISE(auto scalar, ParseStringToScalar(dataType, str));
            ARROW_RETURN_NOT_OK(builder->AppendScalar(*scalar));
        }
    }

    return builder->Finish();
}

arrow::Result<Expression> ParseToArrowExpression(const json& json)
{
    if (json.is_null()) {
        return Status::Invalid("expression is null");
    }

    int op = get_value_or_throw<int>(json, "op");
    switch (static_cast<ParquetPredicateOperator>(op)) {
        case ParquetPredicateOperator::And: {
            ARROW_ASSIGN_OR_RAISE(auto leftExpr, GetSubExpr(json, "left"));
            ARROW_ASSIGN_OR_RAISE(auto rightExpr, GetSubExpr(json, "right"));
            return arrow::compute::and_(leftExpr, rightExpr);
        }
        case ParquetPredicateOperator::Or: {
            ARROW_ASSIGN_OR_RAISE(auto leftExpr, GetSubExpr(json, "left"));
            ARROW_ASSIGN_OR_RAISE(auto rightExpr, GetSubExpr(json, "right"));
            return arrow::compute::or_(leftExpr, rightExpr);
        }
        case ParquetPredicateOperator::Not: {
            ARROW_ASSIGN_OR_RAISE(auto predicateExpr, GetSubExpr(json, "predicate"));
            return arrow::compute::not_(predicateExpr);
        }
        case ParquetPredicateOperator::Eq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(json));
            return arrow::compute::equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::Gt: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(json));
            return arrow::compute::greater(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::GtEq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(json));
            return arrow::compute::greater_equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::Lt: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(json));
            return arrow::compute::less(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::LtEq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(json));
            return arrow::compute::less_equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::IsNotNull: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            return arrow::compute::is_valid(fieldExpr);
        }
        case ParquetPredicateOperator::IsNull: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            return arrow::compute::is_null(fieldExpr);
        }
        case ParquetPredicateOperator::IN: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(json));
            ARROW_ASSIGN_OR_RAISE(auto set, GetSetLiteralExpr(json));
            return arrow::compute::call("is_in", {fieldExpr}, compute::SetLookupOptions{set});
        }
        default:
            return Status::Invalid("ParquetPredicateOperator is not supported: " + std::to_string(op));
    }
}

std::vector<std::string> GetFieldNames(const nlohmann::json& json)
{
    std::vector<std::string> fieldNames;

    auto it = json.find("includedColumns");
    if (it == json.end() || !it.value().is_string()) {
        return fieldNames;
    }

    std::string fieldNamesStr = it.value();
    if (fieldNamesStr.empty()) {
        return fieldNames;
    }

    std::stringstream ss(fieldNamesStr);
    std::string fieldName;
    while (std::getline(ss, fieldName, ',')) {
        fieldName.erase(0, fieldName.find_first_not_of(" \t\n\r"));
        fieldName.erase(fieldName.find_last_not_of(" \t\n\r") + 1);
        if (!fieldName.empty()) {
            fieldNames.push_back(fieldName);
        }
    }

    return fieldNames;
}

}