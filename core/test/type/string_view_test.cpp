#include "gtest/gtest.h"
#include "vector/string_view.h"

#include <algorithm>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace omniruntime::vec {

// ─── Basic ───────────────────────────────────────────────────────────────────

TEST(StringView, basic)
{
    std::string text = "We are stardust, we are golden...";
    for (int32_t i = 0; i < static_cast<int32_t>(text.size()); ++i) {
        std::string subText(text.data(), i);
        StringView view(subText.data(), i);

        EXPECT_EQ(view.size(), static_cast<size_t>(i));
        EXPECT_EQ(view.isInline(), i <= static_cast<int32_t>(StringView::kInlineSize));

        if (view.isInline()) {
            // Inline: data is stored inside the struct, not at subText.data()
            EXPECT_NE(view.data(), subText.data());
        } else {
            EXPECT_EQ(view.data(), subText.data());
        }

        EXPECT_EQ(view.materialize(), subText);
        EXPECT_EQ(view.getString(), subText);
        EXPECT_EQ(view.str(), subText);
        EXPECT_EQ(view, StringView(subText.data(), i));
    }
}

TEST(StringView, emptyString)
{
    StringView empty;
    EXPECT_EQ(empty.size(), 0u);
    EXPECT_TRUE(empty.empty());
    EXPECT_TRUE(empty.isInline());
    EXPECT_EQ(empty.materialize(), "");

    StringView emptyFromLiteral("", 0);
    EXPECT_EQ(empty, emptyFromLiteral);
}

// ─── Inline / non-inline boundary ────────────────────────────────────────────

TEST(StringView, inlineBoundary)
{
    // Exactly at the boundary
    std::string exactly12(12, 'x');
    StringView sv12(exactly12.data(), 12);
    EXPECT_TRUE(sv12.isInline());
    EXPECT_NE(sv12.data(), exactly12.data()); // copied into struct

    // One byte over
    std::string thirteen(13, 'x');
    StringView sv13(thirteen.data(), 13);
    EXPECT_FALSE(sv13.isInline());
    EXPECT_EQ(sv13.data(), thirteen.data()); // points to original
}

// ─── Equality ────────────────────────────────────────────────────────────────

TEST(StringView, equality)
{
    EXPECT_EQ(StringView("hello"), StringView("hello"));
    EXPECT_NE(StringView("hello"), StringView("world"));
    EXPECT_EQ(StringView(""), StringView(""));
    EXPECT_NE(StringView("a"), StringView(""));

    // Cross inline/non-inline boundary equality
    std::string longStr = "this string is definitely longer than twelve bytes";
    std::string longStr2 = longStr; // same content, different allocation
    EXPECT_EQ(StringView(longStr.data(), static_cast<int32_t>(longStr.size())),
              StringView(longStr2.data(), static_cast<int32_t>(longStr2.size())));
}

// ─── Comparison ──────────────────────────────────────────────────────────────

TEST(StringView, comparison)
{
    // Differ in prefix
    EXPECT_LT(StringView(" ab").compare(StringView("ab")), 0);
    EXPECT_GT(StringView("ab").compare(StringView(" ab")), 0);

    // Differ in inlined part
    EXPECT_GT(StringView("In hoc signo").compare(StringView("In hoc signO")), 0);

    // Inlined vs non-inline
    EXPECT_LT(StringView("In hoc signo").compare(
                  StringView("in hoc signo vinces, Constantinus")),
              0);

    // Equal
    EXPECT_EQ(StringView("equal").compare(StringView("equal")), 0);

    // Prefix match, shorter < longer
    EXPECT_LT(StringView("abc").compare(StringView("abcd")), 0);
    EXPECT_GT(StringView("abcd").compare(StringView("abc")), 0);
}

// ─── Self-comparison ─────────────────────────────────────────────────────────

TEST(StringView, selfComparison)
{
    std::vector<std::string> texts{
        "USA",           // within prefix
        "CUBA",          // exactly prefix
        "ARGENTINA",     // within inline
        "UNITEDSTATES",  // exactly inline
        "UNITED STATES", // barely non-inline
        "UNITED STATES OF AMERICA", // clearly non-inline
    };
    std::vector<std::string> copyTexts(texts);

    for (auto& text : texts) {
        StringView view(text.data(), static_cast<int32_t>(text.size()));
        EXPECT_EQ(view.compare(view), 0);
        EXPECT_EQ(view == view, true);
        EXPECT_EQ(view != view, false);
    }

    for (size_t i = 0; i < texts.size(); ++i) {
        StringView lhs(texts[i].data(), static_cast<int32_t>(texts[i].size()));
        StringView rhs(copyTexts[i].data(), static_cast<int32_t>(copyTexts[i].size()));
        EXPECT_EQ(lhs.compare(rhs), 0);
        EXPECT_EQ(lhs == rhs, true);
        EXPECT_EQ(lhs != rhs, false);
    }
}

// ─── Container usage (hash map + sort) ───────────────────────────────────────

TEST(StringView, container)
{
    std::vector<std::string> strings = {
        "May",
        "I walk",
        "beside you",
        "I've come here to lose ",
        "the smog"
        "feel like a cog in something",
        "turning",
    };

    std::vector<StringView> views;
    std::unordered_map<StringView, int32_t> map;
    for (int32_t i = 0; i < static_cast<int32_t>(strings.size()); ++i) {
        views.push_back(StringView(strings[i]));
        map[views.back()] = i;
    }

    for (int32_t i = 0; i < static_cast<int32_t>(strings.size()); ++i) {
        auto it = map.find(StringView(strings[i].c_str(), static_cast<int32_t>(strings[i].size())));
        ASSERT_NE(it, map.end());
        EXPECT_EQ(it->second, i);
    }

    // Sort using compare()
    std::sort(views.begin(), views.end(), [](const StringView& a, const StringView& b) {
        return a.compare(b) < 0;
    });
    for (size_t i = 0; i + 1 < views.size(); ++i) {
        EXPECT_LE(views[i].compare(views[i + 1]), 0);
    }
}

// ─── UDL ─────────────────────────────────────────────────────────────────────

TEST(StringView, literal)
{
    EXPECT_EQ("ab"_sv, StringView("ab"));
    EXPECT_EQ(""_sv, StringView(""));
    EXPECT_EQ("hello world from a longer string"_sv,
              StringView("hello world from a longer string"));
}

// ─── Constructors and conversions ────────────────────────────────────────────

TEST(StringView, implicitConstructionFromLiteral)
{
    StringView sv1("literal");
    EXPECT_EQ(sv1, StringView("literal"));

    StringView sv2{"literal"};
    EXPECT_EQ(sv2, StringView("literal"));

    StringView sv3 = "literal";
    EXPECT_EQ(sv3, StringView("literal"));

    std::optional<StringView> sv4(StringView("literal"));
    EXPECT_TRUE(sv4.has_value());
    EXPECT_EQ(*sv4, StringView("literal"));
}

TEST(StringView, conversionToStringView)
{
    StringView sv("hello");
    std::string_view stdSv = sv;
    EXPECT_EQ(stdSv, "hello");
    EXPECT_EQ(stdSv.size(), 5u);
}

TEST(StringView, conversionToString)
{
    StringView sv("hello world, this is a long string");
    std::string s = static_cast<std::string>(sv);
    EXPECT_EQ(s, "hello world, this is a long string");
}

TEST(StringView, fromStdString)
{
    std::string src = "from std::string";
    StringView sv(src);
    EXPECT_EQ(sv.materialize(), src);
}

TEST(StringView, fromStringView)
{
    std::string_view stdSv = "from std::string_view";
    StringView sv(stdSv);
    EXPECT_EQ(sv.materialize(), std::string(stdSv));
}

// ─── Hash ────────────────────────────────────────────────────────────────────

TEST(StringView, hash)
{
    std::hash<StringView> hasher;

    // Same content → same hash
    EXPECT_EQ(hasher(StringView("hello")), hasher(StringView("hello")));
    EXPECT_EQ(hasher(StringView("")), hasher(StringView("")));

    // Different content → (almost certainly) different hash
    EXPECT_NE(hasher(StringView("hello")), hasher(StringView("world")));

    // Inline vs non-inline copies of the same string should hash equally
    std::string longStr = "this is a string longer than twelve bytes";
    std::string longStr2 = longStr;
    EXPECT_EQ(hasher(StringView(longStr.data(), static_cast<int32_t>(longStr.size()))),
              hasher(StringView(longStr2.data(), static_cast<int32_t>(longStr2.size()))));

    // Hash works in an unordered_map
    std::unordered_map<StringView, int> m;
    m[StringView("key")] = 42;
    EXPECT_EQ(m.at(StringView("key")), 42);
}

// ─── Prefix caching ──────────────────────────────────────────────────────────

TEST(StringView, prefixCaching)
{
    std::string base = "hello world, non-inline string";
    StringView sv(base.data(), static_cast<int32_t>(base.size()));

    // Prefix must match the first kPrefixSize bytes
    EXPECT_EQ(memcmp(reinterpret_cast<const char*>(&sv) + sizeof(uint32_t),
                     base.data(),
                     StringView::kPrefixSize),
              0);
}

// ─── Size / empty ─────────────────────────────────────────────────────────────

TEST(StringView, sizeAndEmpty)
{
    EXPECT_TRUE(StringView("").empty());
    EXPECT_FALSE(StringView("x").empty());
    EXPECT_EQ(StringView("abc").size(), 3u);
    EXPECT_EQ(StringView("hello world!!").size(), 13u);
}

} // namespace omniruntime::vec
