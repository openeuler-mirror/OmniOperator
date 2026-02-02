/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: LIKE function implementation.
 */

#include "Like.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <regex>
#include <codecvt>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;

namespace {
constexpr char kLikeWildcardMany = '%';
constexpr char kLikeWildcardOne = '_';
constexpr char kLikeEscape = '\\';

static std::wstring ToWideString(const std::string& s) {
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
    return convert.from_bytes(s);
}

/// Escape a single char for regex (if special). Append to out.
static void AppendRegexEscaped(char c, std::string& out) {
    if (c == '\\' || c == '.' || c == '^' || c == '$' || c == '[' || c == ']' ||
        c == '(' || c == ')' || c == '*' || c == '+' || c == '?' || c == '{' || c == '}' || c == '|') {
        out += '\\';
    }
    out += c;
}

/// Convert SQL LIKE pattern to regex string. % -> .*, _ -> ., \ + char -> literal.
static std::string LikePatternToRegex(const std::string_view& pattern) {
    std::string regexStr;
    regexStr.reserve(pattern.size() * 2 + 4);
    regexStr += '^';  // full match from start
    size_t i = 0;
    while (i < pattern.size()) {
        char c = pattern[i];
        if (c == kLikeEscape && i + 1 < pattern.size()) {
            AppendRegexEscaped(pattern[i + 1], regexStr);
            i += 2;
            continue;
        }
        if (c == kLikeWildcardMany) {
            regexStr += ".*";
        } else if (c == kLikeWildcardOne) {
            regexStr += '.';
        } else {
            AppendRegexEscaped(c, regexStr);
        }
        ++i;
    }
    regexStr += '$';  // full match to end
    return regexStr;
}
}  // namespace

void LikeFunction::Apply(std::stack<BaseVector*>& args, const DataTypePtr& outputType,
                         BaseVector*& result, ExecutionContext* context) const {
    if (args.size() < 2) {
        OMNI_THROW("Like function Error", "LIKE requires 2 arguments: string, pattern");
    }
    BaseVector* patternVec = args.top();
    args.pop();
    BaseVector* strVec = args.top();
    args.pop();
    ApplyLike(strVec, patternVec, result, outputType);
}

void LikeFunction::ApplyLike(BaseVector* strVec, BaseVector* patternVec, BaseVector*& result,
                             const DataTypePtr& outputType) const {
    int32_t size = strVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    if (patternVec->GetEncoding() == OMNI_ENCODING_CONST) {
        std::string_view patternSv = reinterpret_cast<ConstVector<std::string_view>*>(patternVec)->GetConstValue();
        for (int32_t row = 0; row < size; ++row) {
            if (strVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            std::string_view str = GetStringValueFromVector(strVec, row);
            bool matches = MatchLike(str, patternSv);
            SetBooleanValueToVector(result, row, matches);
        }
    } else {
        for (int32_t row = 0; row < size; ++row) {
            if (strVec->IsNull(row) || patternVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }
            std::string_view str = GetStringValueFromVector(strVec, row);
            std::string_view patternSv = GetStringValueFromVector(patternVec, row);
            bool matches = MatchLike(str, patternSv);
            SetBooleanValueToVector(result, row, matches);
        }
    }
}

bool LikeFunction::MatchLike(const std::string_view& str, const std::string_view& pattern) const {
    std::string strCopy(str);
    std::string regexStr = LikePatternToRegex(pattern);
    try {
        std::wstring wRegex = ToWideString(regexStr);
        std::wstring wStr = ToWideString(strCopy);
        std::wregex re(wRegex);
        return std::regex_match(wStr, re);
    } catch (const std::regex_error& e) {
        OMNI_THROW("Like regex Error: ", e.what());
    }
}

std::string_view LikeFunction::GetStringValueFromVector(BaseVector* vec, int32_t row) {
    Encoding encoding = vec->GetEncoding();
    if (encoding == OMNI_ENCODING_CONST) {
        auto* constVec = static_cast<ConstVector<std::string_view>*>(vec);
        return constVec->GetConstValue();
    }
    if (encoding == OMNI_FLAT) {
        auto* flatVec = static_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        return flatVec->GetValue(row);
    }
    if (encoding == OMNI_DICTIONARY) {
        auto* dictVec = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>*>(vec);
        return dictVec->GetValue(row);
    }
    OMNI_THROW("Like function Error", "Unsupported encoding type for string");
}

void LikeFunction::SetBooleanValueToVector(BaseVector* vec, int32_t row, bool value) {
    auto* resultVec = static_cast<Vector<bool>*>(vec);
    resultVec->SetValue(row, value);
}
}  // namespace omniruntime::vectorization
