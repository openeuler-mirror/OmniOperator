/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RLike function implementation
 */

#include "RLike.h"
#include "vector/vector.h"
#include <limits>
#include <cstring>
#include <regex>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

static std::wstring ToWideString(std::string &s)
{
   std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
   return convert.from_bytes(s);
}

thread_local std::unique_ptr<RLikeFunction::RegexTLSCache> RLikeFunction::tlsCache_ = nullptr;

void RLikeFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
                          BaseVector *&result, ExecutionContext *context) const {
    if (args.size() < 2) {
        OMNI_THROW("RLike function Error", "RLike requires 2 arguments: string, pattern");
    }

    auto patternVec = args.top();
    args.pop();
    auto strVec = args.top();
    args.pop();

    ApplyRLike(strVec, patternVec, result, outputType);
}

void RLikeFunction::ApplyRLike(BaseVector *strVec, BaseVector *patternVec, BaseVector *&result,
                               const DataTypePtr &outputType) const {
    auto size = strVec->GetSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    if(patternVec->GetEncoding() == OMNI_ENCODING_CONST) {
       std::string_view pattern = reinterpret_cast<ConstVector<std::string_view> *>(patternVec)->GetConstValue();
        for (int32_t row = 0; row < size; ++row) {
            // If either string or pattern is NULL, result is NULL
            if (strVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }

            std::string_view str = GetStringValueFromVector(strVec, row);

            bool matches = MatchRegex(str, pattern);
            SetBooleanValueToVector(result, row, matches);
        }
    } else {
        for (int32_t row = 0; row < size; ++row) {
            // If either string or pattern is NULL, result is NULL
            if (strVec->IsNull(row) || patternVec->IsNull(row)) {
                result->SetNull(row);
                continue;
            }

            std::string_view str = GetStringValueFromVector(strVec, row);
            std::string_view pattern = GetStringValueFromVector(patternVec, row);

            bool matches = MatchRegex(str, pattern);
            SetBooleanValueToVector(result, row, matches);
        }
    }
}

bool RLikeFunction::MatchRegex(const std::string_view &sv, const std::string_view &pattern) const {
    std::string str(sv);
    std::string patternStr(pattern);

    if (!tlsCache_) {
        tlsCache_ = std::make_unique<RegexTLSCache>();
    }
    try {
        if (tlsCache_->lastPattern != patternStr) {
            tlsCache_->regex = std::wregex(ToWideString(patternStr));
            tlsCache_->lastPattern = patternStr;
        }
        return std::regex_search(ToWideString(str), tlsCache_->regex);
    } catch (const std::regex_error &e) {
         OMNI_THROW("RLike regex search Error: ", e.what());
    }
}

std::string_view RLikeFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
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
        OMNI_THROW("RLike function Error", "Unsupported encoding type for string");
    }
}

void RLikeFunction::SetBooleanValueToVector(BaseVector *vec, int32_t row, bool value) const {
    auto *resultVec = static_cast<Vector<bool> *>(vec);
    resultVec->SetValue(row, value);
}
}