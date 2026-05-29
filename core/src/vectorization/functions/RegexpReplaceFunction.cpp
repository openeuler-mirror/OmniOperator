/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RegexpReplace function implementation
 */

#include "RegexpReplaceFunction.h"
#include "vector/vector.h"
#include "type/string_Impl.h"
#include "vector/vector_helper.h"
#include <limits>
#include <cstring>
#include <shared_mutex>
#include <re2/re2.h>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;

    static const re2::RE2 kRegex_FixNameGroup("[(][?]<([^>]*)>", re2::RE2::Quiet);
    static std::unordered_map<std::string, std::shared_ptr<re2::RE2>> re2_cache;
    static std::shared_mutex re2_mutex;
    static constexpr size_t MAX_CACHE_SIZE = 128;

    static constexpr std::array<bool, 128> GetMetaCharTable() {
        std::array<bool, 128> arr{};
        const char meta[] = R"(\.+*?^$()[]{}|)";
        for (char c : meta) {
            arr[static_cast<uint8_t>(c)] = true;
        }
        return arr;
    }
    static constexpr std::array<bool, 128> g_re_meta_chars = GetMetaCharTable();

void RegexpReplaceFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const {
    BaseVector *positionVec = args.top();
    args.pop();
    BaseVector *replacementVec = args.top();
    args.pop();
    BaseVector *patternVec = args.top();
    args.pop();
    BaseVector *strVec = args.top();
    args.pop();

    ApplyRegexpReplace(strVec, patternVec, replacementVec, positionVec, result, outputType, context);

    delete positionVec;
    delete replacementVec;
    delete patternVec;
    delete strVec;
}

void RegexpReplaceFunction::ApplyRegexpReplace(BaseVector *strVec, BaseVector *patternVec,
    BaseVector *replacementVec, BaseVector *positionVec, BaseVector *&result, const DataTypePtr &outputType,
    ExecutionContext *context) const {
    auto size = context->GetResultRowSize();
    result = VectorHelper::CreateFlatVector(outputType->GetId(), size);

    for (int32_t row = 0; row < size; ++row) {
        if (strVec->IsNull(row) || patternVec->IsNull(row) || replacementVec->IsNull(row) || positionVec->IsNull(row)) {
            result->SetNull(row);
            continue;
        }

        std::string_view str = VectorHelper::GetStringValueFromVector(strVec, row);
        std::string_view pattern = VectorHelper::GetStringValueFromVector(patternVec, row);
        std::string_view replacement = VectorHelper::GetStringValueFromVector(replacementVec, row);
        int32_t position = VectorHelper::GetValueFromVector<int32_t>(positionVec, row);

        std::string replaced = RegexpReplace(str, pattern, replacement, position);
        std::string_view replacedView(replaced);
        auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(result);
        resultVec->SetValue(row, replacedView);
    }
}

std::string RegexpReplaceFunction::RegexpReplace(const std::string_view &str, const std::string_view &pattern,
                                                 const std::string_view &replacement, int32_t position) const {
    if (str.empty()) {
        return "";
    }

    const int32_t start_pos = std::max(position - 1, 0);
    if (start_pos >= str.size()) {
        return std::string(str);
    }

    if (pattern.empty()) {
        const std::string_view prefix = str.substr(0, start_pos);
        const std::string_view suffix = str.substr(start_pos);
        std::string result;
        result.reserve(prefix.size() + suffix.size() + (suffix.size() + 1) * replacement.size());
        result.append(prefix);

        if (!replacement.empty()) {
            result.append(replacement);
        }

        for (char c : suffix) {
            result.push_back(c);
            if (!replacement.empty()) {
                result.append(replacement);
            }
        }
        return result;
    }

    const int32_t suffix_len = str.size() - start_pos;
    std::string newPattern(pattern.data(), pattern.size());
    std::string replacementStr(replacement.data(), replacement.size());
    std::string buf;
    buf.reserve(str.size());

    bool is_plain_text = true;
    for (char c : newPattern) {
        auto uc = static_cast<uint8_t>(c);
        if (uc < 128 && g_re_meta_chars[uc]) {
            is_plain_text = false;
            break;
        }
    }

    if (!is_plain_text) {
        re2::RE2::GlobalReplace(&newPattern, kRegex_FixNameGroup, R"((?P<\1>))");
    }

    if (is_plain_text) {
        const std::string_view src_sv(str);
        const std::string_view pattern_sv(newPattern);
        const size_t pattern_len = pattern_sv.size();
        const size_t repl_len = replacementStr.size();
        const std::string_view suffix_sv = src_sv.substr(start_pos);

        const size_t max_possible_len = start_pos + suffix_sv.size() +
            (suffix_sv.size() / std::max(static_cast<size_t>(1), pattern_len)) * std::abs(static_cast<int>(repl_len - pattern_len));
        buf.reserve(max_possible_len);

        if (start_pos > 0) {
            buf.append(str.data(), start_pos);
        }

        size_t last_pos = 0;
        size_t curr_pos = suffix_sv.find(pattern_sv, last_pos);
        while (curr_pos != std::string_view::npos) {
            buf.append(suffix_sv.data() + last_pos, curr_pos - last_pos);
            if (!replacementStr.empty()) {
                buf.append(replacementStr);
            }
            last_pos = curr_pos + pattern_len;
            curr_pos = suffix_sv.find(pattern_sv, last_pos);
        }

        buf.append(suffix_sv.data() + last_pos, suffix_sv.size() - last_pos);
    } else {
        std::shared_ptr<const re2::RE2> re_sp;
        {
            std::shared_lock<std::shared_mutex> read_lock(re2_mutex);
            auto cache_iter = re2_cache.find(newPattern);
            if (cache_iter != re2_cache.end()) {
                re_sp = cache_iter->second;
            }
        }

        if (!re_sp) {
            std::unique_lock<std::shared_mutex> write_lock(re2_mutex);
            auto cache_iter = re2_cache.find(newPattern);
            if (cache_iter != re2_cache.end()) {
                re_sp = cache_iter->second;
            } else {
                if (re2_cache.size() >= MAX_CACHE_SIZE) {
                    re2_cache.clear();
                }
                auto re_uptr = std::make_shared<re2::RE2>(stringImpl::toStringPiece(newPattern), re2::RE2::Quiet);
                if (!re_uptr->ok()) {
                    return std::string(str);
                }
                re2_cache.emplace(newPattern, re_uptr);
                re_sp = std::move(re_uptr);
            }
        }

        const re2::RE2 &re = *re_sp;
        const auto &processedReplacement = stringImpl::PrepareRegexpReplaceReplacement(re, replacementStr);
        re2::StringPiece suffix_sp(str.data() + start_pos, suffix_len);
        std::string suffix(suffix_sp);
        re2::RE2::GlobalReplace(&suffix, re, processedReplacement);

        buf.append(str.data(), start_pos);
        buf.append(std::move(suffix));
    }

    return buf;
}
}
