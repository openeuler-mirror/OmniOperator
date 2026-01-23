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

/* Header for class OMNI_RUNTIME_ORCCOLUMNARBATCHJNIREADER_H */

#ifndef OMNI_RUNTIME_ORCCOLUMNARBATCHJNIREADER_H
#define OMNI_RUNTIME_ORCCOLUMNARBATCHJNIREADER_H

#include <memory>
#include <iostream>
#include <string>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <orc/ColumnPrinter.hh>
#include <orc/Exceptions.hh>
#include <orc/Type.hh>
#include <orc/Vector.hh>
#include <orc/Reader.hh>
#include <orc/MemoryPool.hh>
#include <orc/sargs/SearchArgument.hh>
#include <orc/sargs/Literal.hh>
#include "reader/orc/OrcFileOverride.hh"
#include <getopt.h>
#include <jni.h>
#include <json/json.h>
#include <vector/vector_common.h>
#include <util/omni_exception.h>
#include <reader/orc/OrcReader.h>
#include "reader/common/debug.h"
#include "../parquet/ParquetExpression.h"

#ifdef __cplusplus
extern "C" {
#endif

enum class Operator {
    OR,
    AND,
    NOT,
    LEAF,
    CONSTANT
};

enum class PredicateOperatorType {
    EQUALS = 0,
    NULL_SAFE_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS, IN, BETWEEN, IS_NULL
};

int GetLiteral(::orc::Literal &lit, int leafType, const std::string &value);

int BuildLeaves(PredicateOperatorType leafOp, std::vector<::orc::Literal> &litList, ::orc::Literal &lit,
    const std::string &leafNameString, ::orc::PredicateDataType leafType, ::orc::SearchArgumentBuilder &builder);

bool StringToBool(const std::string &boolStr);

void ParseJson(nlohmann::json &json,
    std::list<std::string>& includedColumnsList,
    std::shared_ptr<common::JulianGregorianRebase>& julianPtr,
    std::shared_ptr<common::PredicateCondition>& predicate,
    std::unique_ptr<::orc::SearchArgument>& searchArgument);

void ParsePredicateJson(nlohmann::json &jsonConfig,
    std::shared_ptr<common::PredicateCondition>& predicate,
    Expression* pushedFilterArray,
    std::list<std::string>* includedColumnsList);

#ifdef __cplusplus
}
#endif
#endif
