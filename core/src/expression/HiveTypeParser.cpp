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

#include "HiveTypeParser.h"

#include <cctype>
#include <stdexcept>
#include <string>
#include <utility>
#include <unordered_set>

#include "util/omni_exception.h"

namespace {
using namespace omniruntime::type;
/// Returns true only if 'str' contains digits.
bool isPositiveInteger(const std::string &str)
{
    return !str.empty() && std::find_if(str.begin(), str.end(), [](unsigned char c) {
        return !std::isdigit(c);
    }) == str.end();
}

bool isSupportedSpecialChar(char c)
{
    static std::unordered_set<char> supported{'_', '$', '#'};
    return supported.count(c) == 1;
}

DataTypePtr DECIMAL(const uint8_t precision, const uint8_t scale)
{
    if (precision <= 18) {
        return std::make_shared<Decimal64DataType>(precision, scale);
    }
    return std::make_shared<Decimal128DataType>(precision, scale);
}
} // namespace

namespace omniruntime::type::fbhive {
HiveTypeParser::HiveTypeParser()
{
    metadata_.resize(static_cast<size_t>(TokenType::MaxTokenType));
    setupMetadata<TokenType::Boolean, OMNI_BOOLEAN>("boolean");
    setupMetadata<TokenType::Byte, OMNI_BYTE>("tinyint");
    setupMetadata<TokenType::Short, OMNI_SHORT>("smallint");
    setupMetadata<TokenType::Integer, OMNI_INT>({"integer", "int"});
    setupMetadata<TokenType::Long, OMNI_LONG>("bigint");
    setupMetadata<TokenType::Date, OMNI_INT>("date");
    setupMetadata<TokenType::Float, OMNI_FLOAT>({"float", "real"});
    setupMetadata<TokenType::Double, OMNI_DOUBLE>("double");
    setupMetadata<TokenType::Decimal, OMNI_LONG>("decimal");
    setupMetadata<TokenType::String, OMNI_VARCHAR>({"string", "varchar"});
    setupMetadata<TokenType::Binary, OMNI_VARBINARY>({"binary", "varbinary"});
    setupMetadata<TokenType::Timestamp, OMNI_TIMESTAMP>("timestamp");
    setupMetadata<TokenType::Opaque, OMNI_OPAQUE>("opaque");
    setupMetadata<TokenType::List, OMNI_ARRAY>("array");
    setupMetadata<TokenType::Map, OMNI_MAP>("map");
    setupMetadata<TokenType::Struct, OMNI_ROW>({"struct", "row"});
    setupMetadata<TokenType::StartSubType, OMNI_INVALID>("<");
    setupMetadata<TokenType::EndSubType, OMNI_INVALID>(">");
    setupMetadata<TokenType::Colon, OMNI_INVALID>(":");
    setupMetadata<TokenType::Comma, OMNI_INVALID>(",");
    setupMetadata<TokenType::LeftRoundBracket, OMNI_INVALID>("(");
    setupMetadata<TokenType::RightRoundBracket, OMNI_INVALID>(")");
    setupMetadata<TokenType::Number, OMNI_INVALID>();
    setupMetadata<TokenType::Identifier, OMNI_INVALID>();
    setupMetadata<TokenType::EndOfStream, OMNI_INVALID>();
}

DataTypePtr HiveTypeParser::parse(const std::string &ser)
{
    remaining_ = folly::StringPiece(ser);
    Result result = parseType();
    OMNI_CHECK(remaining_.empty() || TokenType::EndOfStream == lookAhead(),
        "Input remaining after parsing the Hive type \"{}\"\n" "Remaining: \"{}\"", ser, remaining_.toString());
    return result.type;
}

Result HiveTypeParser::parseType()
{
    Token nt = nextToken();
    OMNI_CHECK(!nt.isEOS(), "Unexpected end of stream parsing type!!!");

    if (!nt.isValidType()) {
        OMNI_THROW("Unexpected token {} at {}. typeKind = {}", nt.value.toString(), remaining_.toString(),
            nt.typeKind());
    }

    if (nt.isPrimitiveType()) {
        if (nt.metadata->tokenString[0] == "decimal") {
            eatToken(TokenType::LeftRoundBracket);
            Token precision = nextToken();
            OMNI_CHECK(isPositiveInteger(precision.value.toString()), "Decimal precision must be a positive integer");
            eatToken(TokenType::Comma);
            Token scale = nextToken();
            OMNI_CHECK(isPositiveInteger(scale.value.toString()), "Decimal scale must be a positive integer");
            eatToken(TokenType::RightRoundBracket);
            return Result{DECIMAL(std::atoi(precision.value.data()), std::atoi(scale.value.data()))};
        } else if (nt.metadata->tokenString[0] == "date") {
            return Result{std::make_shared<Date64DataType>()};
        }
        auto scalarType = std::make_shared<DataType>(nt.typeKind());
        OMNI_CHECK(scalarType!=nullptr, "Returned a null scalar type for ", nt.typeKind());
        if (nt.metadata->tokenType == TokenType::String && lookAhead() == TokenType::LeftRoundBracket) {
            eatToken(TokenType::LeftRoundBracket);
            Token length = nextToken();
            OMNI_CHECK(isPositiveInteger(length.value.toString()), "Varchar length must be a positive integer");
            eatToken(TokenType::RightRoundBracket);
        }
        return Result{scalarType};
    } else {
        ResultList resultList = parseTypeList(OMNI_ROW == nt.typeKind());
        switch (nt.typeKind()) {
            case OMNI_ROW:
                return Result{std::make_shared<RowType>(resultList.typelist, resultList.names)};
            case OMNI_MAP: {
                OMNI_CHECK(resultList.typelist.size() == 2, "wrong param count for map type def");
                return Result{std::make_shared<MapType>(resultList.typelist.at(0), resultList.typelist.at(1))};
            }
            case OMNI_ARRAY: {
                OMNI_CHECK(resultList.typelist.size() == 1, "wrong param count for array type def");
                return Result{std::make_shared<ArrayType>(resultList.typelist.at(0))};
            }
            default: OMNI_FAIL("unsupported kind: ");
        }
    }
}

ResultList HiveTypeParser::parseTypeList(bool hasFieldNames)
{
    std::vector<DataTypePtr> subTypeList{};
    std::vector<std::string> names{};
    eatToken(TokenType::StartSubType);
    while (true) {
        if (TokenType::EndSubType == lookAhead()) {
            eatToken(TokenType::EndSubType);
            return ResultList{std::move(subTypeList), std::move(names)};
        }

        if (hasFieldNames) {
            folly::StringPiece fieldName;
            fieldName = eatToken(TokenType::Identifier, true).value;
            eatToken(TokenType::Colon);
            names.push_back(fieldName.toString());
        }

        Result result = parseType();
        subTypeList.push_back(result.type);
        if (TokenType::Comma == lookAhead()) {
            eatToken(TokenType::Comma);
        }
    }
}

TokenType HiveTypeParser::lookAhead() const
{
    return nextToken(remaining_).tokenType();
}

Token HiveTypeParser::eatToken(TokenType tokenType, bool ignorePredefined)
{
    TokenAndRemaining token = nextToken(remaining_, ignorePredefined);
    if (token.tokenType() == tokenType) {
        remaining_ = token.remaining;
        return static_cast<Token>(token);
    }

    OMNI_FAIL("Unexpected token " + token.remaining.toString());
}

Token HiveTypeParser::nextToken(bool ignorePredefined)
{
    TokenAndRemaining token = nextToken(remaining_, ignorePredefined);
    remaining_ = token.remaining;
    return static_cast<Token>(token);
}

TokenAndRemaining HiveTypeParser::nextToken(folly::StringPiece sp, bool ignorePredefined) const
{
    while (!sp.empty() && isspace(sp.front())) {
        sp.advance(1);
    }

    if (sp.empty()) {
        return makeExtendedToken(getMetadata(TokenType::EndOfStream), sp, 0);
    }

    if (!ignorePredefined) {
        for (auto &metadata : metadata_) {
            for (auto &token : metadata->tokenString) {
                folly::StringPiece match(token);
                if (!match.empty() && sp.startsWith(match, folly::AsciiCaseInsensitive{})) {
                    return makeExtendedToken(metadata.get(), sp, match.size());
                }
            }
        }
    }

    auto iter = sp.cbegin();
    size_t len = 0;
    while (isalnum(*iter) || isSupportedSpecialChar(*iter)) {
        ++len;
        ++iter;
    }

    if (len > 0) {
        return makeExtendedToken(getMetadata(TokenType::Identifier), sp, len);
    }

    OMNI_FAIL("Bad Token at " + sp.toString());
}

TokenType Token::tokenType() const
{
    return metadata->tokenType;
}

DataTypeId Token::typeKind() const
{
    return metadata->typeKind;
}

bool Token::isPrimitiveType() const
{
    return metadata->isPrimitiveType;
}

bool Token::isValidType() const
{
    return metadata->typeKind != OMNI_INVALID;
}

bool Token::isEOS() const
{
    return metadata->tokenType == TokenType::EndOfStream;
}

bool Token::isOpaqueType() const
{
    return metadata->tokenType == TokenType::Opaque;
}

int8_t HiveTypeParser::makeTokenId(TokenType tokenType)
{
    return static_cast<int8_t>(tokenType);
}

TokenAndRemaining HiveTypeParser::makeExtendedToken(TokenMetadata *tokenMetadata, folly::StringPiece sp,
    size_t len)
{
    folly::StringPiece spmatch(sp.cbegin(), sp.cbegin() + len);
    sp.advance(len);

    TokenAndRemaining result;
    result.metadata = tokenMetadata;
    result.value = spmatch;
    result.remaining = sp;
    return result;
}

TokenMetadata *HiveTypeParser::getMetadata(TokenType type) const
{
    auto &value = metadata_[makeTokenId(type)];
    return value.get();
}
}
