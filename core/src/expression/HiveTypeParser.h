/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <folly/Range.h>
#include "type/data_type.h"

namespace omniruntime::type::fbhive {
// TODO : Find out what to do with these types
// NUMERIC, INTERVAL, VARCHAR, VOID
enum class TokenType {
    Boolean,
    Byte,
    Short,
    Integer,
    Date,
    Long,
    Float,
    Double,
    String,
    Binary,
    Timestamp,
    Opaque,
    List,
    Map,
    Struct,
    StartSubType,
    EndSubType,
    Colon,
    Comma,
    Number,
    Identifier,
    EndOfStream,
    Decimal,
    LeftRoundBracket,
    RightRoundBracket,
    MaxTokenType
};

struct TokenMetadata {
    TokenType tokenType;
    DataTypeId typeKind;
    std::vector<std::string> tokenString;
    bool isPrimitiveType;

    TokenMetadata(TokenType typ, DataTypeId kind, std::vector<std::string> &&ts, bool ip)
        : tokenType(typ), typeKind(kind), tokenString(std::move(ts)), isPrimitiveType(ip) {}
};

struct Token {
    TokenMetadata *metadata;
    folly::StringPiece value;

    TokenType tokenType() const;

    DataTypeId typeKind() const;

    bool isPrimitiveType() const;

    bool isValidType() const;

    bool isEOS() const;

    bool isOpaqueType() const;
};

struct TokenAndRemaining : public Token {
    folly::StringPiece remaining;
};

struct Result {
    DataTypePtr type;
};

struct ResultList {
    std::vector<DataTypePtr> typelist;
    std::vector<std::string> names;
};

class HiveTypeParser {
public:
    HiveTypeParser();

    ~HiveTypeParser() = default;

    DataTypePtr parse(const std::string &ser);

private:
    static int8_t makeTokenId(TokenType tokenType);

    Result parseType();

    ResultList parseTypeList(bool hasFieldNames);

    TokenType lookAhead() const;

    Token eatToken(TokenType tokenType, bool ignorePredefined = false);

    Token nextToken(bool ignorePredefined = false);

    TokenAndRemaining nextToken(folly::StringPiece sp, bool ignorePredefined = false) const;

    static TokenAndRemaining makeExtendedToken(TokenMetadata *tokenMetadata, folly::StringPiece sp, size_t len);

    template <TokenType KIND, DataTypeId TYPEKIND>
    void setupMetadata(const char *tok = "")
    {
        setupMetadata<KIND, TYPEKIND>(std::vector<std::string>{std::string{tok}});
    }

    template <TokenType KIND, DataTypeId TYPEKIND>
    void setupMetadata(std::vector<std::string> &&tokens)
    {
        bool isPrimitive = (TYPEKIND != OMNI_ARRAY) && (TYPEKIND != OMNI_MAP) && (TYPEKIND != OMNI_ROW);
        metadata_[makeTokenId(KIND)] = std::make_unique<TokenMetadata>(KIND, TYPEKIND, std::move(tokens), isPrimitive);
    }

    TokenMetadata *getMetadata(TokenType type) const;

private:
    std::vector<std::unique_ptr<TokenMetadata>> metadata_;
    folly::StringPiece remaining_;
};
}
