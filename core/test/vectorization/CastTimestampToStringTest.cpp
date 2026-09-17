/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Tests for vectorized timestamp to string casts
 */

#include <gtest/gtest.h>

#include <stack>
#include <string>
#include <string_view>
#include <unordered_map>

#include "operator/execution_context.h"
#include "type/data_type.h"
#include "util/config/QueryConfig.h"
#include "util/type_util.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "vectorization/functions/Cast.h"

using namespace omniruntime;
using namespace omniruntime::config;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;

namespace {
BaseVector *CastTimestampToString(BaseVector *input, const QueryConfig &queryConfig = QueryConfig())
{
    CastFunction function(TimestampType(), VarcharType());
    ExecutionContext context;
    context.SetResultRowSize(input->GetSize());
    context.SetConfig(queryConfig);

    std::stack<BaseVector *> args;
    args.push(input);
    BaseVector *result = nullptr;
    function.Apply(args, VarcharType(), result, &context);
    return result;
}

using StringVector = Vector<LargeStringContainer<std::string_view>>;
} // namespace

TEST(CastTimestampToStringTest, ConstPreservesFractionalPrecision)
{
    constexpr int32_t size = 3;
    auto *input = new ConstVector<int64_t>(123456, OMNI_TIMESTAMP, size);

    BaseVector *result = nullptr;
    ASSERT_NO_THROW(result = CastTimestampToString(input));
    auto *strings = dynamic_cast<StringVector *>(result);
    ASSERT_NE(strings, nullptr);
    for (int32_t row = 0; row < size; ++row) {
        EXPECT_FALSE(result->IsNull(row));
        EXPECT_EQ(strings->GetValue(row), "1970-01-01 00:00:00.123456");
    }
    delete result;
}

TEST(CastTimestampToStringTest, ConstNullProducesNullRows)
{
    constexpr int32_t size = 3;
    auto *input = new ConstVector<int64_t>(0, OMNI_TIMESTAMP, size);
    input->SetNull(0);

    BaseVector *result = nullptr;
    ASSERT_NO_THROW(result = CastTimestampToString(input));
    ASSERT_NE(result, nullptr);
    for (int32_t row = 0; row < size; ++row) {
        EXPECT_TRUE(result->IsNull(row));
    }
    delete result;
}

TEST(CastTimestampToStringTest, FlatValuesRemainSupported)
{
    constexpr int32_t size = 3;
    auto *input = VectorHelper::CreateFlatVector(OMNI_TIMESTAMP, size);
    auto *timestamps = dynamic_cast<Vector<int64_t> *>(input);
    ASSERT_NE(timestamps, nullptr);
    timestamps->SetValue(0, 0);
    timestamps->SetValue(1, 123456);
    input->SetNull(2);

    BaseVector *result = nullptr;
    ASSERT_NO_THROW(result = CastTimestampToString(input));
    auto *strings = dynamic_cast<StringVector *>(result);
    ASSERT_NE(strings, nullptr);
    EXPECT_EQ(strings->GetValue(0), "1970-01-01 00:00:00");
    EXPECT_EQ(strings->GetValue(1), "1970-01-01 00:00:00.123456");
    EXPECT_TRUE(result->IsNull(2));
    delete result;
}

TEST(CastTimestampToStringTest, ConstRespectsSessionTimezone)
{
    constexpr int32_t size = 2;
    auto *input = new ConstVector<int64_t>(0, OMNI_TIMESTAMP, size);
    QueryConfig queryConfig(std::unordered_map<std::string, std::string>{
        {QueryConfig::kSessionTimezone, "Asia/Shanghai"},
        {QueryConfig::kAdjustTimestampToTimezone, "true"}
    });

    BaseVector *result = nullptr;
    ASSERT_NO_THROW(result = CastTimestampToString(input, queryConfig));
    auto *strings = dynamic_cast<StringVector *>(result);
    ASSERT_NE(strings, nullptr);
    for (int32_t row = 0; row < size; ++row) {
        EXPECT_EQ(strings->GetValue(row), "1970-01-01 08:00:00");
    }
    delete result;
}
