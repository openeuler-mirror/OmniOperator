/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: String Function Registry
 */
#ifndef JSON_UTIL_H
#define JSON_UTIL_H

#include <iostream>
#include <string>
#include <optional>
#include "codegen/context_helper.h"
#include "rapidjson/document.h"
#include "rapidjson/reader.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace omniruntime::codegen::function {

const char ROOT = '$';
const char DOT = '.';
const char QUOTE = '\'';
const char UNDER_SCORE = '_';
const char OPEN_BRACKET = '[';
const char CLOSE_BRACKET = ']';

class JsonPathTokenizer {
public:
    JsonPathTokenizer() : index_(0) {}

    bool reset(const std::string_view & path)
    {
        path_ = path;
        index_ = 0;
        if (path.empty() || path_[0] != ROOT) return false;
        index_ = 1;
        return true;
    }

    bool hasNext() const
    {
        return index_ < path_.size();
    }

    std::optional<std::string> next()
    {
        if (match(DOT)) {
            return matchDotKey();
        }
        if (match(OPEN_BRACKET)) {
            if (peek() == QUOTE) {
                return matchQuotedSubscriptKey();
            } else {
                return matchUnquotedSubscriptKey();
            }
        }
        return std::nullopt;
    }
private:
    std::string_view path_;
    int32_t index_;

    bool match(char c)
    {
        if (index_ < path_.size() && path_[index_] == c) {
            index_++;
            return true;
        }
        return false;
    }

    char peek() const
    {
        return index_ < path_.size() ? path_[index_] : '\0';
    }

    std::optional<std::string> matchDotKey()
    {
        size_t start = index_;
        while (index_ < path_.size() && (std::isalnum(path_[index_]) || path_[index_] == UNDER_SCORE)) {
            index_++;
        }
        if (start == index_) return std::nullopt;
        return std::string(path_.substr(start, index_ - start));
    }

    std::optional<std::string> matchUnquotedSubscriptKey()
    {
        size_t start = index_;
        while (index_ < path_.size() && (std::isalnum(path_[index_]) || path_[index_] == UNDER_SCORE)) {
            index_++;
        }
        if (!match(CLOSE_BRACKET)) return std::nullopt;
        return std::string(path_.substr(start, index_ - start));
    }

    std::optional<std::string> matchQuotedSubscriptKey()
    {
        if (!match(QUOTE)) return std::nullopt;    // match the first QUOTE after OPEN_BRACKET
        size_t start = index_;
        while (index_ < path_.size() &&  path_[index_] != QUOTE) {
            index_++;
        }
        if (!match(QUOTE)) return std::nullopt;    // match the second QUOTE after json path
        if (!match(CLOSE_BRACKET)) return std::nullopt;
        return std::string(path_.substr(start, index_ - start - 2));    // remove two QUOTE characters
    }
};

inline std::optional<std::string> traverse_and_extract(const std::string_view json_sv, JsonPathTokenizer& tok)
{
    rapidjson::Document doc;
    doc.Parse<rapidjson::kParseNoFlags>(json_sv.data(), json_sv.size());
    if (doc.HasParseError()) {
        return std::nullopt;
    }

    rapidjson::Value* current = &doc;
    while (tok.hasNext()) {
        auto tokenOpt = tok.next();
        if (!tokenOpt || tokenOpt->empty()) {
            return std::nullopt;
        }
        const std::string_view& token = *tokenOpt;

        const char* token_cstr = token.data();
        if (current->IsObject()) {
            if (!current->HasMember(token_cstr)) {
                return std::nullopt;
            }
            current = &(*current)[token_cstr];
        } else if (current->IsArray()) {
            char* end = nullptr;
            size_t idx = strtoul(token_cstr, &end, 10);
            if (end == token_cstr || idx >= current->Size()) {
                return std::nullopt;
            }
            current = &(*current)[static_cast<rapidjson::SizeType>(idx)];
        } else {
            return std::nullopt;
        }
    }
    if (current->IsNull()) {
        return std::nullopt;
    } else if (current->IsString()) {
        return std::string(current->GetString(), current->GetStringLength());
    } else {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        current->Accept(writer);
        return std::string(buffer.GetString(), buffer.GetSize());
    }
}
}
#endif
