/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: SIMILAR TO function implementation (SQL regex full-match, vectorized)
 */

#include "Similar.h"
#include "vector/vector.h"
#include "util/utf8_util.h"
#include <limits>
#include <cstring>
#include <optional>
#include <regex>
#include <re2/re2.h>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

namespace {

bool IsSimilarSpecial(char c)
{
    return std::strchr("[]()|^-+*_%?{}", c) != nullptr;
}

bool IsRegexSpecial(char c)
{
    return std::strchr("[]()|^-+*?{}$\\", c) != nullptr;
}

void AppendRegexLiteral(std::string &regex, char c)
{
    if (IsRegexSpecial(c)) {
        regex += '\\';
    }
    regex += c;
}

void CheckSimilarEscapeRules(const std::string &sqlPattern, char escapeChar)
{
    if (escapeChar == 0) {
        return;
    }
    // SQL 2003 Part 2 Section 8.6 General Rule 3.b: a special escape char may only
    // escape another special or itself.
    if (IsSimilarSpecial(escapeChar)) {
        for (size_t i = 0; i < sqlPattern.size(); ++i) {
            if (sqlPattern[i] != escapeChar) {
                continue;
            }
            if (i + 1 >= sqlPattern.size()) {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
            char next = sqlPattern[i + 1];
            if (!IsSimilarSpecial(next) && next != escapeChar) {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
        }
    }
    // General Rule 3.c: escape ':' requires a character-class marker [: ... :].
    if (escapeChar == ':') {
        auto openPos = sqlPattern.find("[:");
        if (openPos == std::string::npos || sqlPattern.find(":]", openPos) == std::string::npos) {
            OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
        }
    }
}

// Returns the index of the character before the closing ']'. Caller then lands on ']'.
size_t RewriteSimilarCharClass(const std::string &sqlPattern, std::string &regex, size_t pos, char escapeChar)
{
    for (size_t i = pos + 1; i < sqlPattern.size(); ++i) {
        char c = sqlPattern[i];
        if (c == ']') {
            return i - 1;
        }
        if (escapeChar != 0 && c == escapeChar) {
            if (i + 1 >= sqlPattern.size()) {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
            char next = sqlPattern[++i];
            if (!IsSimilarSpecial(next) && next != escapeChar) {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
            AppendRegexLiteral(regex, next);
            continue;
        }
        if (c == '%' || c == '_') {
            OMNI_THROW("SIMILAR TO pattern error: ", "% or _ not allowed inside character class");
        }
        regex += c;
    }
    return sqlPattern.empty() ? 0 : sqlPattern.size() - 1;
}

std::string SimilarPatternToRegexWithEscape(const std::string &sqlPattern, char escapeChar)
{
    CheckSimilarEscapeRules(sqlPattern, escapeChar);
    std::string regex;
    regex.reserve(sqlPattern.size() * 2);
    bool insideClass = false;
    for (size_t i = 0; i < sqlPattern.size(); ++i) {
        char c = sqlPattern[i];
        if (c == escapeChar) {
            if (i + 1 >= sqlPattern.size()) {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
            char next = sqlPattern[++i];
            if (IsSimilarSpecial(next) || next == escapeChar) {
                AppendRegexLiteral(regex, next);
            } else {
                OMNI_THROW("SIMILAR TO pattern error: ", "Invalid escape sequence");
            }
            continue;
        }
        switch (c) {
            case '_':
                regex += '.';
                break;
            case '%':
                regex += ".*";
                break;
            case '\\':
                regex += "\\\\";
                break;
            case '$':
                regex += "\\$";
                break;
            case '[':
                regex += '[';
                insideClass = true;
                i = RewriteSimilarCharClass(sqlPattern, regex, i, escapeChar);
                break;
            case ']':
                if (!insideClass) {
                    OMNI_THROW("SIMILAR TO pattern error: ", "Invalid regular expression");
                }
                insideClass = false;
                regex += ']';
                break;
            default:
                regex += c;
                break;
        }
    }
    if (insideClass) {
        OMNI_THROW("SIMILAR TO pattern error: ", "Invalid regular expression");
    }
    return regex;
}

bool EscapeToChar(std::string_view esc, char &out)
{
    while (!esc.empty() && esc.back() == ' ') {
        esc.remove_suffix(1);
    }
    if (esc.empty()) {
        return false;
    }
    int32_t cps = omniruntime::Utf8Util::CountCodePoints(esc.data(), static_cast<int32_t>(esc.size()));
    if (cps != 1 || esc.size() != 1) {
        return false;
    }
    out = esc[0];
    return true;
}

}  // namespace

void SimilarFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
                            BaseVector *&result, ExecutionContext *context) const {
    if (argumentCount_ != 2 && argumentCount_ != 3) {
        OMNI_THROW("SIMILAR TO function Error", "Invalid SIMILAR TO registration arity");
    }
    if (args.size() < static_cast<size_t>(argumentCount_)) {
        OMNI_THROW("SIMILAR TO function Error", "SIMILAR TO argument count mismatch");
    }
    if (argumentCount_ == 3) {
        // ExprEval pushes value, pattern, then escape; stack is LIFO.
        auto escapeVec = args.top();
        args.pop();
        auto patternVec = args.top();
        args.pop();
        auto strVec = args.top();
        args.pop();
        ApplySimilarWithEscape(strVec, patternVec, escapeVec, result, outputType);
        delete escapeVec;
        delete patternVec;
        delete strVec;
        return;
    }
    // ExprEval pushes value first, then pattern; stack is LIFO, so pattern is on top.
    auto patternVec = args.top();
    args.pop();
    auto strVec = args.top();
    args.pop();

    ApplySimilar(strVec, patternVec, result, outputType);

    delete patternVec;
    delete strVec;
}

void SimilarFunction::ApplySimilar(BaseVector *strVec, BaseVector *patternVec, BaseVector *&result,
                                   const DataTypePtr &outputType) const {
    auto size = strVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    if (patternVec->GetEncoding() == OMNI_ENCODING_CONST) {
        std::string_view pattern = reinterpret_cast<ConstVector<std::string_view> *>(patternVec)->GetConstValue();
        for (int32_t row = 0; row < size; ++row) {
            if (strVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            std::string_view str = GetStringValueFromVector(strVec, row);
            bool matches = MatchSimilar(str, pattern, 0);
            SetBooleanValueToVector(result, row, matches);
        }
    } else {
        for (int32_t row = 0; row < size; ++row) {
            if (strVec->IsNull(row) || patternVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            std::string_view str = GetStringValueFromVector(strVec, row);
            std::string_view pattern = GetStringValueFromVector(patternVec, row);
            bool matches = MatchSimilar(str, pattern, 0);
            SetBooleanValueToVector(result, row, matches);
        }
    }
}

void SimilarFunction::ApplySimilarWithEscape(BaseVector *strVec, BaseVector *patternVec, BaseVector *escapeVec,
                                              BaseVector *&result, const DataTypePtr &outputType) const {
    auto size = strVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    std::optional<char> constEscape;
    if (escapeVec->GetEncoding() == OMNI_ENCODING_CONST) {
        if (escapeVec->IsNull(0)) {
            for (int32_t row = 0; row < size; ++row) {
                result->SetNull(row);
            }
            return;
        }
        char esc = 0;
        std::string_view raw = reinterpret_cast<ConstVector<std::string_view> *>(escapeVec)->GetConstValue();
        if (!EscapeToChar(raw, esc)) {
            OMNI_THROW("SIMILAR TO function Error", "Escape string must be a single character");
        }
        constEscape = esc;
    }

    for (int32_t row = 0; row < size; ++row) {
        if (strVec->IsNull(row) || patternVec->IsNull(row)) {
            result->SetNull(row);
            continue;
        }
        char esc = 0;
        if (constEscape.has_value()) {
            esc = *constEscape;
        } else {
            if (escapeVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            std::string_view raw = GetStringValueFromVector(escapeVec, row);
            if (!EscapeToChar(raw, esc)) {
                OMNI_THROW("SIMILAR TO function Error", "Escape string must be a single character");
            }
        }
        std::string_view str = GetStringValueFromVector(strVec, row);
        std::string_view pattern = GetStringValueFromVector(patternVec, row);
        SetBooleanValueToVector(result, row, MatchSimilar(str, pattern, esc));
    }
}

std::string SimilarFunction::SimilarPatternToRegex(const std::string &sqlPattern, char escapeChar) const {
    if (escapeChar != 0) {
        return SimilarPatternToRegexWithEscape(sqlPattern, escapeChar);
    }
    // Port of Flink SqlLikeUtils.sqlToRegexSimilar (escapeChar=0). % -> .* , _ -> . , \ -> \\ , $ -> \$.
    // . and ^ pass through intentionally (Flink default branch); inside [...] %/_ are invalid -> throw.
    std::string regex;
    regex.reserve(sqlPattern.size() * 2);
    bool insideClass = false;
    for (size_t i = 0; i < sqlPattern.size(); ++i) {
        char c = sqlPattern[i];
        if (insideClass) {
            if (c == ']') {
                insideClass = false;
                regex += c;
            } else if (c == '%' || c == '_') {
                // Flink throws invalidRegularExpression on SQL specials inside [...]; match it.
                std::string detail = "% or _ not allowed inside character class";
                OMNI_THROW("SIMILAR TO pattern error: ", detail);
            } else {
                regex += c;  // - ^ and POSIX [:class:] pass through; re2 understands POSIX classes
            }
            continue;
        }
        switch (c) {
            case '_':
                regex += '.';
                break;
            case '%':
                regex += ".*";
                break;
            case '\\':
                regex += "\\\\";
                break;
            case '$':
                regex += "\\$";
                break;
            case '[':
                insideClass = true;
                regex += c;
                break;
            default:
                regex += c;
                break;
        }
    }
    return regex;
}

bool SimilarFunction::MatchSimilar(const std::string_view &sv, const std::string_view &pattern,
                                    char escapeChar) const {
    std::string r(pattern);
    if (r.empty() && escapeChar == 0) {
        return sv.empty();  // empty pattern full-matches only empty string (Flink Pattern.matches("", s))
    }
    thread_local std::string cachedPattern;
    thread_local char cachedEscape = 0;
    thread_local std::unique_ptr<RE2> cachedRegex;
    if (cachedPattern != r || cachedEscape != escapeChar) {
        std::string regexStr = SimilarPatternToRegex(r, escapeChar);  // may throw on invalid char class
        RE2::Options opt(RE2::Quiet);
        opt.set_dot_nl(true);  // SQL % matches any chars including newline
        auto newRegex = std::make_unique<RE2>(regexStr, opt);
        if (!newRegex->ok()) {
            OMNI_THROW("SIMILAR TO regex error: ", newRegex->error());
        }
        cachedPattern = r;  // update cache only after successful compile (avoid stale cache on failure)
        cachedEscape = escapeChar;
        cachedRegex = std::move(newRegex);
    }
    std::string s(sv);
    // SIMILAR TO is a full match (Flink uses Pattern.matches), so use FullMatch.
    return RE2::FullMatch(re2::StringPiece(s.data(), s.length()), *cachedRegex);
}

std::string_view SimilarFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
    Encoding encoding = vec->GetEncoding();
    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<std::string_view> *>(vec);
        return constVec->GetConstValue();
    } else if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
        return flatVec->GetValue(row);
    } else if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vec);
        return dictVec->GetValue(row);
    } else {
        OMNI_THROW("SIMILAR TO function Error", "Unsupported encoding type for string");
    }
}

void SimilarFunction::SetBooleanValueToVector(BaseVector *vec, int32_t row, bool value) const {
    auto *resultVec = static_cast<Vector<bool> *>(vec);
    resultVec->SetValue(row, value);
}
}
