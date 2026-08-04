/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: SIMILAR TO function implementation (SQL regex full-match, vectorized)
 */

#include "Similar.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <regex>
#include <re2/re2.h>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

void SimilarFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
                            BaseVector *&result, ExecutionContext *context) const {
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
            bool matches = MatchSimilar(str, pattern);
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
            bool matches = MatchSimilar(str, pattern);
            SetBooleanValueToVector(result, row, matches);
        }
    }
}

std::string SimilarFunction::SimilarPatternToRegex(const std::string &sqlPattern) const {
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

bool SimilarFunction::MatchSimilar(const std::string_view &sv, const std::string_view &pattern) const {
    std::string r(pattern);
    if (r.empty()) {
        return sv.empty();  // empty pattern full-matches only empty string (Flink Pattern.matches("", s))
    }
    thread_local std::string cachedPattern;
    thread_local std::unique_ptr<RE2> cachedRegex;
    if (cachedPattern != r) {
        std::string regexStr = SimilarPatternToRegex(r);  // may throw on invalid char class
        RE2::Options opt(RE2::Quiet);
        opt.set_dot_nl(true);  // SQL % matches any chars including newline
        auto newRegex = std::make_unique<RE2>(regexStr, opt);
        if (!newRegex->ok()) {
            OMNI_THROW("SIMILAR TO regex error: ", newRegex->error());
        }
        cachedPattern = r;  // update cache only after successful compile (avoid stale cache on failure)
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
