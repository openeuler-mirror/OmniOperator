/**
 * Copyright (C) 2023-2026. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifdef OMNI_ENABLE_EXPERIMENTAL_SORT

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "operator/inplace_pdqsort.h"
#include "operator/pdqsort.h"
#include "operator/timsort.h"

namespace {

std::vector<uint64_t> IdentityAddrs(size_t n)
{
    std::vector<uint64_t> a(n);
    std::iota(a.begin(), a.end(), 0);
    return a;
}

// swap_indices moves values and addresses together, so values[i] must always be
// the element that started at original[addrs[i]].
void AssertAddressesConsistent(const std::vector<int64_t> &values, const std::vector<uint64_t> &addrs,
    const std::vector<int64_t> &original)
{
    ASSERT_EQ(values.size(), addrs.size());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(values[i], original[addrs[i]]) << "i=" << i;
    }
}

template <typename RawType, int32_t Asc>
void RunDirectHeapSort(std::vector<int64_t> &values, std::vector<uint64_t> &addrs, int32_t from, int32_t to)
{
    pdqsort::detail::make_heap_indices<RawType, Asc>(values.data(), addrs.data(), from, to);
    pdqsort::detail::sort_heap_indices<RawType, Asc>(values.data(), addrs.data(), from, to);
}

// Baseline that forces pdqsort_loop into a highly unbalanced first partition:
//   n <= 128: sorted input -> median-of-3 pivot is the min/max -> one side is empty.
//   n > 128:  the 9 extremes are placed on the ninther sample positions so the
//             pseudomedian is the 5th extreme -> only 4 elements end up left of it.
// With bad_allowed == 1 this deterministically triggers the heap fallback.
std::vector<int64_t> MakeKiller(int32_t n, int32_t asc)
{
    std::vector<int64_t> values(static_cast<size_t>(n));
    if (asc == 1) {
        std::iota(values.begin(), values.end(), 0);
    } else {
        for (int32_t i = 0; i < n; ++i) {
            values[i] = int64_t(n - 1 - i);
        }
    }
    if (n > 128) {
        const int32_t s2 = n / 2;
        const int32_t pos[9] = {0, 1, 2, s2 - 1, s2, s2 + 1, n - 3, n - 2, n - 1};
        for (int32_t k = 0; k < 9; ++k) {
            values[pos[k]] = (asc == 1) ? int64_t(k) : int64_t(n - 1 - k);
        }
    }
    return values;
}

}  // namespace

// Regression for the heap fallback fix: sift_down used to build a min-heap while
// sort_heap_indices extracts the root to the back, which produced a descending
// result for ascending sorts. Exercise make_heap_indices/sort_heap_indices
// directly so the fixed path is always covered.
TEST(SortRegressionPdqHeapTest, direct_heap_functions_asc_desc)
{
    std::mt19937 rng(2026);
    for (int32_t n : {0, 1, 2, 3, 15, 16, 17, 31, 64, 100, 257}) {
        std::vector<int64_t> values(static_cast<size_t>(n));
        for (auto &v : values) {
            v = int64_t(rng() % 10000) - 5000;
        }
        auto original = values;
        auto addrs = IdentityAddrs(values.size());
        RunDirectHeapSort<int64_t, 1>(values, addrs, 0, n);
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end())) << "asc n=" << n;
        AssertAddressesConsistent(values, addrs, original);

        values = original;
        addrs = IdentityAddrs(values.size());
        RunDirectHeapSort<int64_t, 0>(values, addrs, 0, n);
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end(), std::greater<int64_t>{})) << "desc n=" << n;
        AssertAddressesConsistent(values, addrs, original);
    }
}

TEST(SortRegressionPdqHeapTest, direct_heap_functions_sorted_reverse_duplicates)
{
    constexpr int32_t n = 300;
    {
        std::vector<int64_t> values(n);
        std::iota(values.begin(), values.end(), 0);
        std::reverse(values.begin(), values.end());
        auto original = values;
        auto addrs = IdentityAddrs(n);
        RunDirectHeapSort<int64_t, 1>(values, addrs, 0, n);
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end()));
        AssertAddressesConsistent(values, addrs, original);
    }
    {
        std::vector<int64_t> values(200, 7);
        for (int32_t i = 0; i < 32; ++i) {
            values[i * 6] = int64_t(i);
        }
        auto original = values;
        auto addrs = IdentityAddrs(values.size());
        RunDirectHeapSort<int64_t, 0>(values, addrs, 0, int32_t(values.size()));
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end(), std::greater<int64_t>{}));
        AssertAddressesConsistent(values, addrs, original);
    }
}

TEST(SortRegressionPdqHeapTest, direct_heap_functions_subrange)
{
    // sift_down indexes relative to `from`; a subrange sort must leave the
    // prefix and suffix untouched.
    std::vector<int64_t> values(100);
    for (int32_t i = 0; i < 100; ++i) {
        values[i] = (i * 37 + 11) % 100;
    }
    auto addrs = IdentityAddrs(100);
    auto prefix = std::vector<int64_t>(values.begin(), values.begin() + 20);
    auto suffix = std::vector<int64_t>(values.begin() + 60, values.end());
    RunDirectHeapSort<int64_t, 1>(values, addrs, 20, 60);
    ASSERT_TRUE(std::equal(values.begin(), values.begin() + 20, prefix.begin()));
    ASSERT_TRUE(std::equal(values.begin() + 60, values.end(), suffix.begin()));
    ASSERT_TRUE(std::is_sorted(values.begin() + 20, values.begin() + 60));
}

TEST(SortRegressionPdqHeapTest, direct_heap_functions_double)
{
    constexpr int32_t n = 300;
    std::vector<int64_t> values(n);
    for (int32_t i = 0; i < n; ++i) {
        double d = double((i * 97) % 300) - 150 + 0.5 * double(i % 3);
        std::memcpy(&values[i], &d, sizeof(double));
    }
    auto original = values;
    auto addrs = IdentityAddrs(n);
    RunDirectHeapSort<double, 1>(values, addrs, 0, n);
    for (int32_t i = 1; i < n; ++i) {
        double a = 0, b = 0;
        std::memcpy(&a, &values[i - 1], sizeof(double));
        std::memcpy(&b, &values[i], sizeof(double));
        ASSERT_LE(a, b) << "asc i=" << i;
    }
    AssertAddressesConsistent(values, addrs, original);

    values = original;
    addrs = IdentityAddrs(n);
    RunDirectHeapSort<double, 0>(values, addrs, 0, n);
    for (int32_t i = 1; i < n; ++i) {
        double a = 0, b = 0;
        std::memcpy(&a, &values[i - 1], sizeof(double));
        std::memcpy(&b, &values[i], sizeof(double));
        ASSERT_GE(a, b) << "desc i=" << i;
    }
}

// End-to-end: drive pdqsort_loop into the heap fallback with bad_allowed == 1
// and a killer input, then verify the fallback range comes out in the
// requested order. Before the sift_down fix these produced reversed output.
TEST(SortRegressionPdqHeapTest, end_to_end_bad_allowed_fallback)
{
    for (int32_t n : {16, 64, 128, 129, 257, 1024}) {
        auto values = MakeKiller(n, 1);
        auto addrs = IdentityAddrs(static_cast<size_t>(n));
        auto original = values;
        pdqsort::detail::pdqsort_loop<int64_t, 1, true>(values.data(), addrs.data(), 0, n, 1);
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end())) << "asc n=" << n;
        AssertAddressesConsistent(values, addrs, original);

        values = MakeKiller(n, 0);
        addrs = IdentityAddrs(static_cast<size_t>(n));
        original = values;
        pdqsort::detail::pdqsort_loop<int64_t, 0, true>(values.data(), addrs.data(), 0, n, 1);
        ASSERT_TRUE(std::is_sorted(values.begin(), values.end(), std::greater<int64_t>{})) << "desc n=" << n;
        AssertAddressesConsistent(values, addrs, original);
    }

    // Non-branchless partition feeds the same fallback.
    const int32_t n = 64;
    auto values = MakeKiller(n, 1);
    auto addrs = IdentityAddrs(static_cast<size_t>(n));
    pdqsort::detail::pdqsort_loop<int64_t, 1, false>(values.data(), addrs.data(), 0, n, 1);
    ASSERT_TRUE(std::is_sorted(values.begin(), values.end()));
}

struct Item {
    int32_t key;
    int32_t seq;
    bool operator==(const Item &o) const
    {
        return key == o.key && seq == o.seq;
    }
};

TEST(SortRegressionTimSortTest, projection_stability_and_member_pointer)
{
    std::mt19937 rng(55);
    for (int32_t n : {1, 2, 31, 32, 33, 200, 1000}) {
        std::vector<Item> items;
        items.reserve(static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i) {
            items.push_back({int32_t(rng() % 10), i});
        }
        auto expected = items;
        std::stable_sort(expected.begin(), expected.end(),
            [](const Item &a, const Item &b) { return a.key < b.key; });

        // lambda projection, ascending
        timsort::timsort(items.begin(), items.end(), std::less<int32_t>{},
            [](const Item &it) { return it.key; });
        ASSERT_EQ(items, expected) << "asc lambda n=" << n;

        // member-pointer projection goes through std::invoke
        timsort::timsort(items.begin(), items.end(), std::greater<int32_t>{}, &Item::key);
        auto expectedDesc = expected;
        std::stable_sort(expectedDesc.begin(), expectedDesc.end(),
            [](const Item &a, const Item &b) { return a.key > b.key; });
        ASSERT_EQ(items, expectedDesc) << "desc member n=" << n;

        // sorting an already ascending sequence must stay stable
        timsort::timsort(items.begin(), items.end(), std::less<int32_t>{}, &Item::key);
        ASSERT_EQ(items, expected) << "resort asc n=" << n;
    }
}

TEST(SortRegressionTimSortTest, timmerge_with_projection_and_bool_overload)
{
    // left keys {1,3,5}, right keys {0,2,4}
    std::vector<Item> items = {{1, 0}, {3, 1}, {5, 2}, {0, 3}, {2, 4}, {4, 5}};
    timsort::timmerge(items.begin(), items.begin() + 3, items.end(), std::less<int32_t>{}, &Item::key);
    for (size_t i = 0; i < items.size(); ++i) {
        ASSERT_EQ(items[i].key, int32_t(i)) << "i=" << i;
    }
    ASSERT_EQ(items[0].seq, 3);
    ASSERT_EQ(items[1].seq, 0);

    // bool* overload falls back to std::sort (vector<bool> has no bool&)
    bool data[] = {true, false, false, true, false, true};
    timsort::timsort(data, data + 6);
    ASSERT_TRUE(std::is_sorted(data, data + 6));
    ASSERT_FALSE(data[0]);
    ASSERT_TRUE(data[5]);
}

TEST(SortRegressionTimSortTest, stability_large_random)
{
    std::mt19937 rng(1234);
    std::vector<std::pair<int32_t, int32_t>> data(5000);
    for (int32_t i = 0; i < 5000; ++i) {
        data[i] = {int32_t(rng() % 50), i};
    }
    auto expected = data;
    std::stable_sort(expected.begin(), expected.end(),
        [](const auto &a, const auto &b) { return a.first < b.first; });
    timsort::timsort(data.begin(), data.end(),
        [](const auto &a, const auto &b) { return a.first < b.first; });
    ASSERT_EQ(data, expected);
}

TEST(SortRegressionInplacePdqTest, strings_branchless_and_doubles)
{
    std::mt19937 rng(77);
    // std::string is not arithmetic, so this exercises the non-branchless partition
    for (int32_t n : {8, 64, 300, 1000}) {
        std::vector<std::string> v;
        v.reserve(static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i) {
            v.push_back("key" + std::to_string(int32_t(rng() % 100000)));
        }
        auto expected = v;
        std::sort(expected.begin(), expected.end());
        inplace_pdqsort::pdqsort(v.begin(), v.end());
        ASSERT_EQ(v, expected) << "asc n=" << n;
    }

    {
        std::vector<std::string> v = {"alpha", "zulu", "mike", "bravo", "yankee", "november"};
        inplace_pdqsort::pdqsort(v.begin(), v.end(), std::greater<std::string>{});
        ASSERT_TRUE(std::is_sorted(v.begin(), v.end(), std::greater<std::string>{}));
    }

    {
        std::vector<double> v(800);
        std::mt19937 r2(2);
        for (auto &d : v) {
            d = double(r2() % 100000) / 7.0 - 5000.0;
        }
        inplace_pdqsort::pdqsort(v.begin(), v.end());
        ASSERT_TRUE(std::is_sorted(v.begin(), v.end()));
        inplace_pdqsort::pdqsort(v.begin(), v.end(), std::greater<double>{});
        ASSERT_TRUE(std::is_sorted(v.begin(), v.end(), std::greater<double>{}));
    }
}

TEST(SortRegressionInplacePdqTest, explicit_branchless_and_subrange)
{
    {
        std::vector<int64_t> v(100000);
        std::mt19937 rng(3);
        for (auto &x : v) {
            x = int64_t(rng() % 1000000);
        }
        inplace_pdqsort::pdqsort_branchless(v.begin(), v.end());
        ASSERT_TRUE(std::is_sorted(v.begin(), v.end()));
        for (auto &x : v) {
            x = int64_t(rng() % 1000000);
        }
        inplace_pdqsort::pdqsort_branchless(v.begin(), v.end(), std::greater<int64_t>{});
        ASSERT_TRUE(std::is_sorted(v.begin(), v.end(), std::greater<int64_t>{}));
    }

    {
        // sorting [3, 12) must not touch the prefix or the suffix
        std::vector<int64_t> v = {90, 80, 70, 9, 7, 5, 3, 1, 8, 6, 4, 2, 30, 20, 10};
        inplace_pdqsort::pdqsort(v.begin() + 3, v.begin() + 12);
        ASSERT_EQ(v[0], 90);
        ASSERT_EQ(v[1], 80);
        ASSERT_EQ(v[2], 70);
        ASSERT_TRUE(std::is_sorted(v.begin() + 3, v.begin() + 12));
        ASSERT_EQ(v[12], 30);
        ASSERT_EQ(v[13], 20);
        ASSERT_EQ(v[14], 10);
    }

    {
        std::vector<float> v = {1.5f, 3.25f, -0.5f, 2.0f, 0.0f, 3.25f, -2.0f};
        auto expected = v;
        std::sort(expected.begin(), expected.end());
        inplace_pdqsort::pdqsort(v.begin(), v.end());
        ASSERT_EQ(v, expected);
    }
}

#endif
