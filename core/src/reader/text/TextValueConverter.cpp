/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextValueConverter.h"

#include <stack>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "operator/execution_context.h"
#include "vector/vector_helper.h"
#include "vectorization/functions/CastExpr.h"

namespace omniruntime::reader::text {

TextValueConverter::TextValueConverter(std::string sessionTimezone)
    : queryConfig_([&sessionTimezone]() {
        std::unordered_map<std::string, std::string> values;
        if (!sessionTimezone.empty()) {
            values[config::QueryConfig::kSessionTimezone] = std::move(sessionTimezone);
        }
        values[config::QueryConfig::kAdjustTimestampToTimezone] = "true";
        return values;
    }())
{}

std::unique_ptr<vec::BaseVector> TextValueConverter::CastOwned(
    std::unique_ptr<vec::BaseVector> input,
    const type::DataTypePtr& fromType,
    const type::DataTypePtr& toType) const
{
    if (input == nullptr) {
        throw std::runtime_error("Text value conversion input is null.");
    }
    if (fromType->GetId() == toType->GetId()) {
        return input;
    }
    op::ExecutionContext context;
    context.SetConfig(queryConfig_);
    context.SetResultRowSize(input->GetSize());
    context.SetThrowOnError(false);
    std::stack<vec::BaseVector*> arguments;
    auto* ownedInput = input.release();
    arguments.push(ownedInput);
    vec::BaseVector* result = nullptr;
    vectorization::CastExpr cast(
        fromType,
        toType,
        true,
        std::make_shared<vectorization::CastHooks>(queryConfig_));
    try {
        cast.Apply(arguments, toType, result, &context);
    } catch (...) {
        delete ownedInput;
        throw;
    }
    if (result == nullptr) {
        throw std::runtime_error("Text value conversion returned a null vector.");
    }
    return std::unique_ptr<vec::BaseVector>(result);
}

std::unique_ptr<vec::BaseVector> TextValueConverter::DecodeColumn(
    std::unique_ptr<vec::BaseVector> strings,
    const type::DataTypePtr& targetType) const
{
    return CastOwned(
        std::move(strings), std::make_shared<type::VarcharDataType>(), targetType);
}

std::unique_ptr<vec::BaseVector> TextValueConverter::EncodeColumn(
    vec::BaseVector* source,
    const type::DataTypePtr& sourceType,
    int64_t start,
    int64_t end) const
{
    if (source == nullptr || start < 0 || end < start || end > source->GetSize()) {
        throw std::runtime_error("Text writer row range is invalid.");
    }
    std::vector<int> positions(static_cast<size_t>(end - start));
    for (size_t index = 0; index < positions.size(); ++index) {
        positions[index] = static_cast<int>(start + index);
    }
    std::unique_ptr<vec::BaseVector> copied(vec::VectorHelper::CopyPositionsVector(
        source, positions.data(), 0, static_cast<int>(positions.size())));
    return CastOwned(
        std::move(copied), sourceType, std::make_shared<type::VarcharDataType>());
}

} // namespace omniruntime::reader::text
