/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "gtest/gtest.h"
#include "xsimd/xsimd.hpp"

namespace APLTest {

TEST(APLTest, TestAdd)
{
    constexpr int32_t dataSize = 4;
    int64_t data1[dataSize] = {4, 3, 2, 1};
    int64_t data2[dataSize] = {1, 2, 3, 4};
    int64_t expect[dataSize] = {5, 5, 5, 5};
    xsimd::batch<int64_t, xsimd::default_arch> a = xsimd::load(data1);
    xsimd::batch<int64_t, xsimd::default_arch> b = xsimd::load(data2);
    xsimd::batch<int64_t, xsimd::default_arch> res = a + b;
    for (int32_t i = 0; i < xsimd::batch<int64_t, xsimd::default_arch>::size; i++) {
        EXPECT_EQ(res.get(i), expect[i]);
    }
}
}  // namespace APLTest