/**
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "OrcColumnarBatchJniReader.h"
#include <boost/algorithm/string.hpp>
#include <memory>
#include "jni_common.h"
#include "reader/common/UriInfo.h"
#include "reader/common/JulianGregorianRebase.h"
#include "reader/common/PredicateUtil.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;

static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;
uint64_t batchLen = 0;

bool StringToBool(const std::string &boolStr)
{
    if (boost::iequals(boolStr, "true")) {
        return true;
    } else if (boost::iequals(boolStr, "false")) {
        return false;
    } else {
        throw std::runtime_error("Invalid input for stringToBool.");
    }
}

int GetLiteral(::orc::Literal &lit, int leafType, const std::string &value)
{
    switch ((::orc::PredicateDataType)leafType) {
        case ::orc::PredicateDataType::LONG: {
            lit = ::orc::Literal(static_cast<int64_t>(std::stol(value)));
            break;
        }
        case ::orc::PredicateDataType::FLOAT: {
            lit = ::orc::Literal(static_cast<double>(std::stod(value)));
            break;
        }
        case ::orc::PredicateDataType::STRING: {
            lit = ::orc::Literal(value.c_str(), value.size());
            break;
        }
        case ::orc::PredicateDataType::DATE: {
            lit = ::orc::Literal(PredicateDataType::DATE, static_cast<int64_t>(std::stol(value)));
            break;
        }
        case ::orc::PredicateDataType::TIMESTAMP: {
            vector<std::string> valList;
            istringstream timestampStr(value);
            string tmpStr;
            while (timestampStr >> tmpStr) {
                valList.push_back(tmpStr);
            }
            lit = ::orc::Literal(std::stoll(valList[0]), std::stoi(valList[1]));
            break;
        }
        case ::orc::PredicateDataType::DECIMAL: {
            vector<std::string> valList;
            // Decimal(22, 6) eg: value ("19999999999998,998000 22 6")
            istringstream tmpAllStr(value);
            string tmpStr;
            while (tmpAllStr >> tmpStr) {
                valList.push_back(tmpStr);
            }
            Decimal decimalVal(valList[0]);
            lit = ::orc::Literal(decimalVal.value, static_cast<int32_t>(std::stoi(valList[1])),
                static_cast<int32_t>(std::stoi(valList[2])));
            break;
        }
        case ::orc::PredicateDataType::BOOLEAN: {
            lit = ::orc::Literal(static_cast<bool>(StringToBool(value)));
            break;
        }
        default: {
            throw std::runtime_error("tableScan jni getLiteral unsupported leafType: " + leafType);
        }
    }
    return 0;
}

int initLeaves(SearchArgumentBuilder& builder,const nlohmann::json& jsonExp,const nlohmann::json& jsonLeaves) {
    if (!jsonExp.contains("leaf") || !jsonExp["leaf"].is_string()) {
        throw std::runtime_error("Missing or invalid 'leaf' field in expression");
    }
    std::string leafKey = jsonExp["leaf"].get<std::string>();

    if (!jsonLeaves.contains(leafKey)) {
        throw std::runtime_error("Leaf definition not found: " + leafKey);
    }
    const auto& leafJson = jsonLeaves[leafKey];

    if (!leafJson.contains("name") || !leafJson["name"].is_string()) {
        throw std::runtime_error("Leaf missing 'name' field: " + leafKey);
    }
    std::string leafNameString = leafJson["name"].get<std::string>();

    if (!leafJson.contains("op") || !leafJson["op"].is_number_integer()) {
        throw std::runtime_error("Leaf missing or invalid 'op' field: " + leafKey);
    }
    int leafOp = leafJson["op"].get<int>();

    if (!leafJson.contains("type") || !leafJson["type"].is_number_integer()) {
        throw std::runtime_error("Leaf missing or invalid 'type' field: " + leafKey);
    }
    int leafType = leafJson["type"].get<int>();

    Literal lit(0L);
    bool hasLiteral = false;
    if (leafJson.contains("literal") && leafJson["literal"].is_string()) {
        std::string leafValueString = leafJson["literal"].get<std::string>();
        if (!leafValueString.empty() || static_cast<PredicateDataType>(leafType) == PredicateDataType::STRING) {
            GetLiteral(lit, leafType, leafValueString);
            hasLiteral = true;
        }
    }

    std::vector<Literal> litList;
    bool invalidList = false;

    if (leafJson.contains("literalList")) {
        try {
            std::string listStr = leafJson["literalList"].get<std::string>();
            auto literalListJson = nlohmann::json::parse(listStr);

            if (literalListJson.is_array()) {
                for (const auto& elem : literalListJson) {
                    if (elem.is_null()) {
                        litList.clear();
                        invalidList = true;
                        break;
                    } else if (elem.is_string()) {
                        std::string val = elem.get<std::string>();
                        GetLiteral(lit, leafType, val);
                        litList.push_back(lit);
                    } else {
                        throw std::runtime_error("Non-string element in literalList");
                    }
                }
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to parse literalList JSON: " + std::string(e.what()));
        }
    }

    if (invalidList) {
        litList.clear();
    }

    BuildLeaves(
        static_cast<PredicateOperatorType>(leafOp),
        litList,
        lit,
        leafNameString,
        static_cast<PredicateDataType>(leafType),
        builder
    );

    return 1;
}

int initExpressionTree(SearchArgumentBuilder& builder,const nlohmann::json& jsonExp, const nlohmann::json& jsonLeaves) {
    if (!jsonExp.contains("op") || !jsonExp["op"].is_number_integer()) {
        throw std::runtime_error("Missing or invalid 'op' field in expression tree");
    }
    int op = jsonExp["op"].get<int>();

    if (op == static_cast<int>(Operator::LEAF)) {
        return initLeaves(builder, jsonExp, jsonLeaves);
    }

    switch (static_cast<Operator>(op)) {
        case Operator::OR: {
            builder.startOr();
            break;
        }
        case Operator::AND: {
            builder.startAnd();
            break;
        }
        case Operator::NOT: {
            builder.startNot();
            break;
        }
        default: {
            throw std::runtime_error("Unsupported operator in expression tree: " + std::to_string(op));
        }
    }

    if (!jsonExp.contains("child")) {
        builder.end();
        return 0;
    }

    const auto& childNode = jsonExp["child"];

    if (childNode.is_string()) {
        try {
            std::string childStr = childNode.get<std::string>();
            auto childArray = nlohmann::json::parse(childStr);

            if (childArray.is_array()) {
                for (const auto& child : childArray) {
                    initExpressionTree(builder, child, jsonLeaves);
                }
            } else {
                throw std::runtime_error("'child' parsed JSON is not an array");
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to parse 'child' string as JSON: " + std::string(e.what()));
        }
    }
    else if (childNode.is_array()) {
        for (const auto& child : childNode) {
            initExpressionTree(builder, child, jsonLeaves);
        }
    }
    else {
        throw std::runtime_error("'child' must be a string or array");
    }

    builder.end();
    return 0;
}

void ParseJson(nlohmann::json &jsonConfig,
    std::list<std::string>& includedColumnsList,
    std::shared_ptr<common::JulianGregorianRebase>& julianPtr,
    std::shared_ptr<common::PredicateCondition>& predicate,
    std::unique_ptr<::::orc::SearchArgument>& searchArgument)
{
    if (jsonConfig.contains("expressionTree") && jsonConfig.contains("leaves")) {
        const auto& exprTree = jsonConfig["expressionTree"];
        const auto& leaves = jsonConfig["leaves"];

        std::unique_ptr<SearchArgumentBuilder> builder = SearchArgumentFactory::newBuilder();
        initExpressionTree(*builder, exprTree, leaves);
        auto sargBuilded = builder->build();
        searchArgument = std::unique_ptr<SearchArgument>(sargBuilded.release());
    }

    if (jsonConfig.contains("tz") && jsonConfig.contains("switches") && jsonConfig.contains("diffs")) {
        julianPtr = common::BuildJulianGregorianRebase(jsonConfig);
    }

    if (jsonConfig.contains("vecPredicateCondition")) {
        predicate = common::BuildVecPredicateCondition(jsonConfig, includedColumnsList.size());
    }
}

nlohmann::json OptimizeJsonPredicate(
        const nlohmann::json& json_predicate,
        const std::unordered_set<std::string>& available_columns) {

    if (json_predicate.contains("field")) {
        std::string field_name = json_predicate["field"];

        if (available_columns.find(field_name) == available_columns.end()) {
            if (!json_predicate.contains("op")) {
                nlohmann::json false_expr;
                false_expr["op"] = static_cast<int>(omniruntime::reader::ParquetPredicateOperator::False);
                return false_expr;
            }

            int op_code = json_predicate["op"];
            auto predicate_op = static_cast<omniruntime::reader::ParquetPredicateOperator>(op_code);

            nlohmann::json constant_expr;
            if (predicate_op == omniruntime::reader::ParquetPredicateOperator::IsNull) {
                constant_expr["op"] = static_cast<int>(omniruntime::reader::ParquetPredicateOperator::True);
            } else {
                constant_expr["op"] = static_cast<int>(omniruntime::reader::ParquetPredicateOperator::False);
            }
            return constant_expr;
        }
        return json_predicate;
    }

    nlohmann::json optimized_json = json_predicate;

    if (json_predicate.contains("left") && json_predicate.contains("right")) {
        optimized_json["left"] = OptimizeJsonPredicate(json_predicate["left"], available_columns);
        optimized_json["right"] = OptimizeJsonPredicate(json_predicate["right"], available_columns);
    } else if (json_predicate.contains("predicate")) {
        optimized_json["predicate"] = OptimizeJsonPredicate(json_predicate["predicate"], available_columns);
    }

    return optimized_json;
}

void ParsePredicateJson(nlohmann::json &jsonConfig, std::shared_ptr<common::PredicateCondition>& predicate,
                        arrow::compute::Expression* pushedFilterArray, std::list<std::string>* includedColumnsList)
{
    if (pushedFilterArray != nullptr && jsonConfig.contains("expressionTree")) {
        std::unordered_set<std::string> available_columns(includedColumnsList->begin(), includedColumnsList->end());

        const nlohmann::json& expr_json = jsonConfig["expressionTree"];

        nlohmann::json optimized_json = OptimizeJsonPredicate(expr_json, available_columns);

        auto parse_result = omniruntime::reader::ParseToArrowExpression(optimized_json);
        if (!parse_result.ok()) {
            throw OmniException(parse_result.status().ToString().c_str());
        }

        *pushedFilterArray = parse_result.MoveValueUnsafe();
    }

    auto new_time_rebase = common::BuildTimeRebaseInfo(jsonConfig);
    if (jsonConfig.contains("vecPredicateCondition")) {
        int32_t columnCount = (includedColumnsList != nullptr) ? includedColumnsList->size() : 0;
        predicate = common::BuildVecPredicateConditionWithRebase(jsonConfig, columnCount, std::move(new_time_rebase));
    }
}

int BuildLeaves(PredicateOperatorType leafOp, vector<Literal> &litList, Literal &lit, const std::string &leafNameString,
    PredicateDataType leafType, SearchArgumentBuilder &builder)
{
    switch (leafOp) {
        case PredicateOperatorType::LESS_THAN: {
            builder.lessThan(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::LESS_THAN_EQUALS: {
            builder.lessThanEquals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::EQUALS: {
            builder.equals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::NULL_SAFE_EQUALS: {
            builder.nullSafeEquals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::IS_NULL: {
            builder.isNull(leafNameString, leafType);
            break;
        }
        case PredicateOperatorType::IN: {
            if (litList.empty()) {
                std::string emptyString;
                builder.in(emptyString, leafType, litList);
            } else {
                builder.in(leafNameString, leafType, litList);
            }
            break;
        }
        case PredicateOperatorType::BETWEEN: {
            throw std::runtime_error("table scan buildLeaves BETWEEN is not supported!");
        }
        default: {
            throw std::runtime_error("table scan buildLeaves illegal input!");
        }
    }
    return 1;
}



inline void FindLastNotEmpty(const char *chars, long &len)
{
    while (len > 0 && chars[len - 1] == ' ') {
        len--;
    }
}

void clearRecordBatch(std::vector<omniruntime::vec::BaseVector*> &recordBatch)
{
    for (auto vec : recordBatch) {
        delete vec;
    }
    recordBatch.clear();
}