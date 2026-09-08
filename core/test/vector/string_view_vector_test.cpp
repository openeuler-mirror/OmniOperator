/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: Integration tests for Vector<StringView>
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "vector/string_view.h"
#include "vector/dictionary_container.h"
#include <string>

namespace omniruntime::vec::test {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const std::string kShortStr = "hello";          // 5 bytes — inline
static const std::string kBoundaryStr = "123456789012"; // 12 bytes — inline (max)
static const std::string kLongStr = "hello_world_123";  // 15 bytes — non-inline

// ---------------------------------------------------------------------------
// Construction & GetTypeId
// ---------------------------------------------------------------------------

TEST(StringViewVector, getTypeId)
{
    auto vec = std::make_unique<Vector<StringView>>(10);
    EXPECT_EQ(vec->GetTypeId(), type::OMNI_STRING_VIEW);
}

TEST(StringViewVector, factoryGetTypeId)
{
    auto *base = VectorHelper::CreateStringViewVector(10);
    EXPECT_EQ(base->GetTypeId(), type::OMNI_STRING_VIEW);
    delete base;
}

// ---------------------------------------------------------------------------
// SetValue / GetValue — inline strings
// ---------------------------------------------------------------------------

TEST(StringViewVector, setGetInlineString)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    for (int i = 0; i < 5; i++) {
        std::string s = "str" + std::to_string(i);
        vec->SetValue(i, StringView(s));
    }
    for (int i = 0; i < 5; i++) {
        std::string expected = "str" + std::to_string(i);
        EXPECT_EQ(std::string(vec->GetValue(i)), expected);
    }
}

TEST(StringViewVector, setGetBoundaryInlineString)
{
    auto vec = std::make_unique<Vector<StringView>>(3);
    vec->SetValue(0, StringView(kBoundaryStr));  // exactly 12 bytes — still inline
    vec->SetValue(1, StringView(kShortStr));
    vec->SetValue(2, StringView(kLongStr));       // 15 bytes — non-inline

    EXPECT_EQ(std::string(vec->GetValue(0)), kBoundaryStr);
    EXPECT_EQ(std::string(vec->GetValue(1)), kShortStr);
    EXPECT_EQ(std::string(vec->GetValue(2)), kLongStr);
}

// ---------------------------------------------------------------------------
// SetValue / GetValue — non-inline strings
// ---------------------------------------------------------------------------

TEST(StringViewVector, setGetNonInlineString)
{
    int vectorSize = 100;
    auto vec = std::make_unique<Vector<StringView>>(vectorSize);
    std::string prefix = "non_inline_value_";
    for (int i = 0; i < vectorSize; i++) {
        std::string s = prefix + std::to_string(i);
        vec->SetValue(i, StringView(s));
    }
    for (int i = 0; i < vectorSize; i++) {
        std::string expected = prefix + std::to_string(i);
        EXPECT_EQ(std::string(vec->GetValue(i)), expected);
    }
}

// ---------------------------------------------------------------------------
// Empty string
// ---------------------------------------------------------------------------

TEST(StringViewVector, setGetEmptyString)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    for (int i = 0; i < 5; i++) {
        vec->SetValue(i, StringView("", 0));
    }
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(vec->GetValue(i).size(), 0u);
        EXPECT_TRUE(vec->GetValue(i).empty());
    }
}

// ---------------------------------------------------------------------------
// Mixed inline and non-inline
// ---------------------------------------------------------------------------

TEST(StringViewVector, mixedInlineAndNonInline)
{
    int vectorSize = 1000;
    auto vec = std::make_unique<Vector<StringView>>(vectorSize);
    std::vector<std::string> strs;
    for (int i = 0; i < vectorSize; i++) {
        // Alternate between short (inline) and long (non-inline)
        std::string s = (i % 2 == 0)
            ? ("ab" + std::to_string(i))            // short
            : ("long_string_val_" + std::to_string(i)); // long
        strs.push_back(s);
        vec->SetValue(i, StringView(s));
    }
    for (int i = 0; i < vectorSize; i++) {
        EXPECT_EQ(std::string(vec->GetValue(i)), strs[i]);
    }
}

// ---------------------------------------------------------------------------
// Null handling
// ---------------------------------------------------------------------------

TEST(StringViewVector, nullHandling)
{
    int vectorSize = 10;
    auto vec = std::make_unique<Vector<StringView>>(vectorSize);

    EXPECT_FALSE(vec->HasNull());

    for (int i = 0; i < vectorSize; i++) {
        if (i % 2 == 0) {
            vec->SetNull(i);
        } else {
            vec->SetValue(i, StringView(kLongStr));
        }
    }

    EXPECT_TRUE(vec->HasNull());
    for (int i = 0; i < vectorSize; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(vec->IsNull(i));
        } else {
            EXPECT_FALSE(vec->IsNull(i));
            EXPECT_EQ(std::string(vec->GetValue(i)), kLongStr);
        }
    }
}

// ---------------------------------------------------------------------------
// String buffer growth (GrowStringBuffer)
// ---------------------------------------------------------------------------

TEST(StringViewVector, stringBufferGrowth)
{
    // Start with tiny capacity to force multiple reallocations
    int vectorSize = 200;
    auto vec = std::make_unique<Vector<StringView>>(vectorSize, /*capacityInBytes=*/16);
    std::vector<std::string> strs;
    for (int i = 0; i < vectorSize; i++) {
        std::string s = "growing_buf_test_" + std::to_string(i);
        strs.push_back(s);
        vec->SetValue(i, StringView(s));
    }
    // After multiple growths, all pointers must still be valid
    for (int i = 0; i < vectorSize; i++) {
        EXPECT_EQ(std::string(vec->GetValue(i)), strs[i]);
    }
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

TEST(StringViewVector, append)
{
    int srcSize = 5;
    std::vector<std::string> strs;
    auto src = std::make_unique<Vector<StringView>>(srcSize);
    for (int i = 0; i < srcSize; i++) {
        std::string s = "append_val_" + std::to_string(i);
        strs.push_back(s);
        src->SetValue(i, StringView(s));
    }

    int dstSize = 10;
    auto dst = std::make_unique<Vector<StringView>>(dstSize);
    dst->Append(src.get(), 0, srcSize);

    for (int i = 0; i < srcSize; i++) {
        EXPECT_FALSE(dst->IsNull(i));
        EXPECT_EQ(std::string(dst->GetValue(i)), strs[i]);
    }
}

TEST(StringViewVector, appendWithNulls)
{
    int srcSize = 4;
    auto src = std::make_unique<Vector<StringView>>(srcSize);
    src->SetNull(0);
    src->SetValue(1, StringView("val_one"));
    src->SetNull(2);
    src->SetValue(3, StringView("a_longer_value_here"));

    int dstSize = 4;
    auto dst = std::make_unique<Vector<StringView>>(dstSize);
    dst->Append(src.get(), 0, srcSize);

    EXPECT_TRUE(dst->IsNull(0));
    EXPECT_FALSE(dst->IsNull(1));
    EXPECT_EQ(std::string(dst->GetValue(1)), "val_one");
    EXPECT_TRUE(dst->IsNull(2));
    EXPECT_FALSE(dst->IsNull(3));
    EXPECT_EQ(std::string(dst->GetValue(3)), "a_longer_value_here");
}

TEST(StringViewVector, appendOutOfRangeThrows)
{
    auto src = std::make_unique<Vector<StringView>>(5);
    auto dst = std::make_unique<Vector<StringView>>(5);
    EXPECT_ANY_THROW(dst->Append(src.get(), 0, 6));
}

// ---------------------------------------------------------------------------
// CopyPositions
// ---------------------------------------------------------------------------

TEST(StringViewVector, copyPositions)
{
    int vecSize = 6;
    auto vec = std::make_unique<Vector<StringView>>(vecSize);
    std::vector<std::string> strs = {"aaa", "bbb_longer_val", "ccc", "ddd_longer_too", "eee", "fff_also_long"};
    for (int i = 0; i < vecSize; i++) {
        vec->SetValue(i, StringView(strs[i]));
    }

    int positions[] = {0, 2, 4, 1, 3, 5};
    int offset = 1;
    int copySize = 3;
    auto *copied = reinterpret_cast<Vector<StringView> *>(vec->CopyPositions(positions, offset, copySize));
    ASSERT_NE(copied, nullptr);

    // positions[1..3] = {2, 4, 1}
    EXPECT_EQ(std::string(copied->GetValue(0)), strs[2]);
    EXPECT_EQ(std::string(copied->GetValue(1)), strs[4]);
    EXPECT_EQ(std::string(copied->GetValue(2)), strs[1]);
    delete copied;
}

TEST(StringViewVector, copyPositionsWithNulls)
{
    int vecSize = 4;
    auto vec = std::make_unique<Vector<StringView>>(vecSize);
    vec->SetNull(0);
    vec->SetValue(1, StringView("some_long_string_here"));
    vec->SetNull(2);
    vec->SetValue(3, StringView("x"));

    int positions[] = {0, 1, 2, 3};
    auto *copied = reinterpret_cast<Vector<StringView> *>(vec->CopyPositions(positions, 0, 4));
    EXPECT_TRUE(copied->IsNull(0));
    EXPECT_FALSE(copied->IsNull(1));
    EXPECT_EQ(std::string(copied->GetValue(1)), "some_long_string_here");
    EXPECT_TRUE(copied->IsNull(2));
    EXPECT_FALSE(copied->IsNull(3));
    EXPECT_EQ(std::string(copied->GetValue(3)), "x");
    delete copied;
}

TEST(StringViewVector, copyPositionsNegativeLengthThrows)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    int positions[] = {0, 1};
    EXPECT_ANY_THROW(vec->CopyPositions(positions, 0, -1));
}

TEST(StringViewVector, copyPositionsEmpty)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    int positions[] = {0, 1};
    auto *copied = reinterpret_cast<Vector<StringView> *>(vec->CopyPositions(positions, 0, 0));
    EXPECT_EQ(copied->GetSize(), 0);
    delete copied;
}

// ---------------------------------------------------------------------------
// Slice
// ---------------------------------------------------------------------------

TEST(StringViewVector, slice)
{
    int vectorSize = 20;
    auto parent = std::make_unique<Vector<StringView>>(vectorSize);
    std::vector<std::string> strs;
    for (int i = 0; i < vectorSize; i++) {
        std::string s = "slice_test_val_" + std::to_string(i);
        strs.push_back(s);
        parent->SetValue(i, StringView(s));
    }

    int sliceOffset = 5;
    int sliceLen = 8;
    auto *sliced = reinterpret_cast<Vector<StringView> *>(parent->Slice(sliceOffset, sliceLen));
    ASSERT_NE(sliced, nullptr);
    EXPECT_EQ(sliced->GetTypeId(), type::OMNI_STRING_VIEW);
    EXPECT_EQ(sliced->GetSize(), sliceLen);

    for (int i = 0; i < sliceLen; i++) {
        EXPECT_EQ(std::string(sliced->GetValue(i)), strs[i + sliceOffset]);
    }
    delete sliced;
}

TEST(StringViewVector, sliceOutOfRangeThrows)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    EXPECT_ANY_THROW(vec->Slice(4, 5));
}

// ---------------------------------------------------------------------------
// Expand
// ---------------------------------------------------------------------------

TEST(StringViewVector, expand)
{
    auto vec = std::make_unique<Vector<StringView>>(5);
    for (int i = 0; i < 5; i++) {
        std::string s = "val_" + std::to_string(i);
        vec->SetValue(i, StringView(s));
    }

    vec->Expand(20);
    EXPECT_EQ(vec->GetSize(), 20);

    // Original values are still valid after expansion
    for (int i = 0; i < 5; i++) {
        std::string expected = "val_" + std::to_string(i);
        EXPECT_EQ(std::string(vec->GetValue(i)), expected);
    }

    // Write into newly expanded slots
    for (int i = 5; i < 20; i++) {
        std::string s = "new_long_value_" + std::to_string(i);
        vec->SetValue(i, StringView(s));
    }
    for (int i = 5; i < 20; i++) {
        std::string expected = "new_long_value_" + std::to_string(i);
        EXPECT_EQ(std::string(vec->GetValue(i)), expected);
    }
}

// ---------------------------------------------------------------------------
// VectorHelper::SetValue (via std::string_view pointer)
// ---------------------------------------------------------------------------

TEST(StringViewVector, vectorHelperSetValue)
{
    auto vec = std::make_unique<Vector<StringView>>(5);

    std::string_view sv1 = "short";
    std::string longStr = "a_longer_string_value";
    std::string_view sv2(longStr);

    VectorHelper::SetValue(vec.get(), 0, &sv1);
    VectorHelper::SetValue(vec.get(), 1, &sv2);

    EXPECT_EQ(std::string(vec->GetValue(0)), "short");
    EXPECT_EQ(std::string(vec->GetValue(1)), longStr);
}

// ---------------------------------------------------------------------------
// Dictionary vector
// ---------------------------------------------------------------------------

TEST(StringViewVector, dictionaryGetValue)
{
    int dictSize = 4;
    int valueSize = 8;

    auto *dict = new Vector<StringView>(dictSize);
    dict->SetValue(0, StringView("apple"));
    dict->SetValue(1, StringView("banana_long_value"));
    dict->SetNull(2);
    dict->SetValue(3, StringView("cherry"));

    int32_t indices[] = {0, 1, 2, 3, 0, 1, 3, 2};
    auto *base = VectorHelper::CreateDictionary(indices, valueSize, dict);
    auto *dictVec = reinterpret_cast<Vector<DictionaryContainer<StringView>> *>(base);

    EXPECT_EQ(std::string(dictVec->GetValue(0)), "apple");
    EXPECT_EQ(std::string(dictVec->GetValue(1)), "banana_long_value");
    EXPECT_TRUE(dictVec->IsNull(2));
    EXPECT_EQ(std::string(dictVec->GetValue(3)), "cherry");
    EXPECT_EQ(std::string(dictVec->GetValue(4)), "apple");
    EXPECT_EQ(std::string(dictVec->GetValue(5)), "banana_long_value");
    EXPECT_EQ(std::string(dictVec->GetValue(6)), "cherry");
    EXPECT_TRUE(dictVec->IsNull(7));

    delete dictVec;
    delete dict;
}

// ---------------------------------------------------------------------------
// Large-scale round-trip (stress)
// ---------------------------------------------------------------------------

TEST(StringViewVector, largeScaleRoundTrip)
{
    int vectorSize = 10000;
    auto vec = std::make_unique<Vector<StringView>>(vectorSize, /*capacityInBytes=*/64);
    std::vector<std::string> strs;
    strs.reserve(vectorSize);
    for (int i = 0; i < vectorSize; i++) {
        // Alternate inline / non-inline
        std::string s = (i % 3 == 0)
            ? ("x" + std::to_string(i))
            : ("long_round_trip_val_" + std::to_string(i));
        strs.push_back(s);
        vec->SetValue(i, StringView(s));
    }
    for (int i = 0; i < vectorSize; i++) {
        EXPECT_EQ(std::string(vec->GetValue(i)), strs[i]);
    }
}

// ---------------------------------------------------------------------------
// SetNoCopy
// ---------------------------------------------------------------------------

TEST(StringViewVector, setNoCopyInlineStrings)
{
    auto vec = std::make_unique<Vector<StringView>>(3);
    vec->SetNoCopy(0, StringView(kShortStr));
    vec->SetNoCopy(1, StringView(kBoundaryStr));
    vec->SetNoCopy(2, StringView("hi"));

    EXPECT_EQ(std::string(vec->GetValue(0)), kShortStr);
    EXPECT_EQ(std::string(vec->GetValue(1)), kBoundaryStr);
    EXPECT_EQ(std::string(vec->GetValue(2)), "hi");
}

TEST(StringViewVector, setNoCopyTrimmedStringsSharedBuffer)
{
    // Source strings where the trimmed form is still > 12 bytes (stays non-inline).
    const std::vector<std::string> padded = {
        "  hello_world_wide  ",    // trimmed: "hello_world_wide" (16 bytes)
        "  longer_string_here  ",  // trimmed: "longer_string_here" (18 bytes)
        "  another_long_one  ",    // trimmed: "another_long_one" (16 bytes)
    };
    const std::vector<std::string> expected = {
        "hello_world_wide",
        "longer_string_here",
        "another_long_one",
    };

    int n = static_cast<int>(padded.size());
    auto srcVec = std::make_unique<Vector<StringView>>(n);
    for (int i = 0; i < n; i++) {
        srcVec->SetValue(i, StringView(padded[i]));
    }

    // dstVec shares srcVec's string buffer via shared_ptr.
    auto dstVec = std::make_unique<Vector<StringView>>(n, *srcVec);
    for (int i = 0; i < n; i++) {
        StringView sv = srcVec->GetValue(i);
        const char *p = sv.data();
        int32_t len = static_cast<int32_t>(sv.size());
        while (len > 0 && *p == ' ') { ++p; --len; }
        while (len > 0 && p[len - 1] == ' ') { --len; }
        StringView trimmed(p, len);
        EXPECT_FALSE(trimmed.isInline());  // confirm we exercise the non-inline path
        dstVec->SetNoCopy(i, trimmed);
    }

    // Destroy srcVec — dstVec still holds the shared_ptr to the string buffer.
    srcVec.reset();

    for (int i = 0; i < n; i++) {
        EXPECT_EQ(std::string(dstVec->GetValue(i)), expected[i]);
    }
}

TEST(StringViewVector, setNoCopyMixedInlineAndNonInline)
{
    const std::vector<std::string> strings = {
        "hi",
        "hello_world_123",
        "123456789012",
        "non_inline_substring_val",
        "abc",
    };

    int n = static_cast<int>(strings.size());
    auto srcVec = std::make_unique<Vector<StringView>>(n);
    for (int i = 0; i < n; i++) {
        srcVec->SetValue(i, StringView(strings[i]));
    }

    auto dstVec = std::make_unique<Vector<StringView>>(n, *srcVec);
    for (int i = 0; i < n; i++) {
        dstVec->SetNoCopy(i, srcVec->GetValue(i));
    }

    for (int i = 0; i < n; i++) {
        EXPECT_EQ(std::string(dstVec->GetValue(i)), strings[i]);
    }

    // Inline: each struct owns its bytes independently — distinct storage addresses.
    StringView dstInline = dstVec->GetValue(0);
    StringView srcInline = srcVec->GetValue(0);
    EXPECT_TRUE(dstInline.isInline());
    EXPECT_NE(dstInline.data(), srcInline.data());

    // Non-inline: same pointer proves SetNoCopy made no copy.
    StringView dstNonInline = dstVec->GetValue(1);
    StringView srcNonInline = srcVec->GetValue(1);
    EXPECT_FALSE(dstNonInline.isInline());
    EXPECT_EQ(dstNonInline.data(), srcNonInline.data());
}

} // namespace omniruntime::vec::test
