/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: from_json function for JSON operations
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/row_vector.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"
#include "util/debug.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <vector>
#include <string>
#include <string_view>
#include <algorithm>
#include <cctype>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class FromJsonFunction final : public VectorFunction {
public:
    explicit FromJsonFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    std::string_view GetStringValue(BaseVector *vector, int32_t row) const;
    void ParseJsonToRow(const std::string_view &jsonStr, const RowType &rowType, 
        RowVector *resultVec, int32_t row) const;
    void ExtractStringField(const rapidjson::Value &jsonValue, BaseVector *fieldVec, int32_t row) const;
    std::string ToLower(const std::string &str) const;
};
}
