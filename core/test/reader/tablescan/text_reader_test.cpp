/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

#include "codegen/Options.h"
#include "reader/ReaderFactory.h"
#include "reader/ReaderOptions.h"
#include "reader/common/UriInfo.h"
#include "reader/text/TextFormatOptions.h"
#include "reader/text/TextLineScanner.h"
#include "reader/text/TextWriter.h"
#include "type/data_type.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"

namespace omniruntime::reader::text {
namespace {

std::string MakeTempPath(const std::string& suffix)
{
    static int sequence = 0;
    return "/tmp/omni_text_" + std::to_string(getpid()) + "_" +
        std::to_string(sequence++) + suffix;
}

void WriteBytes(const std::string& path, const std::string& content)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(content.data(), static_cast<std::streamsize>(content.size()));
    output.close();
}

std::string ReadBytes(const std::string& path)
{
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

std::shared_ptr<ReaderOptions> MakeReaderOptions(
    const std::string& path, int64_t splitStart, int64_t splitEnd, bool projectValue)
{
    auto options = std::make_shared<ReaderOptions>();
    options->ParseEnhanceJson(
        R"({"text.source_kind":"SPARK_TEXT","text.codec_kind":"RAW_LINE",)"
        R"("text.charset":"UTF-8","text.line_separator":"",)"
        R"("text.compression_codec":"NONE","text.whole_text":"false"})",
        codegen::FileFormat::TEXT);
    options->SetUri(std::make_shared<UriInfo>("file", path, "", "-1"));
    options->SetSplitStart(splitStart);
    options->SetSplitEnd(splitEnd);
    auto rowType = projectValue
        ? type::ROW(std::vector<std::string>{"value"},
              std::vector<type::DataTypePtr>{type::VarcharType()})
        : type::ROW({}, {});
    options->SetRowType(rowType);
    options->SetFileRowType(rowType);
    return options;
}

struct ReadResult {
    std::vector<std::string> values;
    uint64_t rows = 0;
};

TEST(TextFormatOptionsTest, AcceptsSupportedCodecCombinations)
{
    TextFormatOptions valid;
    valid.sourceKind = TextSourceKind::SPARK_TEXT;
    valid.codecKind = TextCodecKind::RAW_LINE;
    valid.common.charset = "UTF-8";
    valid.common.compressionCodec = "NONE";
    valid.dialect = RawLineOptions{};
    EXPECT_NO_THROW(valid.Validate());

    auto invalid = valid;
    invalid.codecKind = TextCodecKind::LAZY_SIMPLE;
    EXPECT_THROW(invalid.Validate(), std::runtime_error);
    invalid = valid;
    invalid.common.compressionCodec = "gzip";
    EXPECT_THROW(invalid.Validate(), std::runtime_error);

    TextFormatOptions lazy;
    lazy.sourceKind = TextSourceKind::HIVE_TEXT;
    lazy.codecKind = TextCodecKind::LAZY_SIMPLE;
    lazy.common.charset = "UTF-8";
    lazy.common.compressionCodec = "NONE";
    LazySimpleOptions lazyDialect;
    lazyDialect.delimited.fieldDelimiter = '|';
    lazy.dialect = lazyDialect;
    EXPECT_NO_THROW(lazy.Validate());
}

TEST(LazySimpleSerdeCodecTest, HandlesDelimiterEscapeAndNull)
{
    TextFormatOptions options;
    options.sourceKind = TextSourceKind::HIVE_TEXT;
    options.codecKind = TextCodecKind::LAZY_SIMPLE;
    options.common.charset = "UTF-8";
    options.common.compressionCodec = "NONE";
    LazySimpleOptions lazy;
    lazy.delimited.fieldDelimiter = '|';
    lazy.delimited.nullLiteral = "NULL";
    lazy.delimited.escapeEnabled = true;
    lazy.delimited.escapeChar = '\\';
    options.dialect = lazy;

    auto codec = CreateTextCodec(options);
    DecodedTextRecord decoded;
    codec->DecodeRecord(R"(left|escaped\|value|NULL)", decoded);
    ASSERT_EQ(decoded.fields.size(), 3);
    EXPECT_EQ(decoded.fields[0].value, "left");
    EXPECT_EQ(decoded.fields[1].value, "escaped|value");
    EXPECT_TRUE(decoded.fields[2].isNull);

    std::string encoded;
    codec->EncodeRecord(
        {{false, "left"}, {false, "escaped|value"}, {true, {}}}, encoded);
    EXPECT_EQ(encoded, R"(left|escaped\|value|NULL)");
}

TEST(LazySimpleSerdeCodecTest, DecodesOnlyProjectedFieldsInOutputOrder)
{
    TextFormatOptions options;
    options.sourceKind = TextSourceKind::HIVE_TEXT;
    options.codecKind = TextCodecKind::LAZY_SIMPLE;
    options.common.charset = "UTF-8";
    options.common.compressionCodec = "NONE";
    LazySimpleOptions lazy;
    lazy.delimited.fieldDelimiter = '|';
    lazy.delimited.nullLiteral = "NULL";
    lazy.delimited.escapeEnabled = true;
    lazy.delimited.escapeChar = '\\';
    options.dialect = lazy;

    auto codec = CreateTextCodec(options, {2, 0});
    DecodedTextRecord decoded;
    codec->DecodeRecord(R"(left|escaped\|value|NULL|ignored\|tail)", decoded);
    ASSERT_EQ(decoded.fields.size(), 2);
    EXPECT_TRUE(decoded.fields[0].isNull);
    EXPECT_FALSE(decoded.fields[1].isNull);
    EXPECT_EQ(decoded.fields[1].value, "left");
    EXPECT_TRUE(decoded.storage.empty());

    codec = CreateTextCodec(options, {1});
    codec->DecodeRecord(R"(ignored\|prefix|selected\|value|tail)", decoded);
    ASSERT_EQ(decoded.fields.size(), 1);
    EXPECT_EQ(decoded.fields[0].value, "selected|value");

    codec = CreateTextCodec(options, {3, 1});
    codec->DecodeRecord("left|", decoded);
    ASSERT_EQ(decoded.fields.size(), 2);
    EXPECT_TRUE(decoded.fields[0].isNull);
    EXPECT_FALSE(decoded.fields[1].isNull);
    EXPECT_TRUE(decoded.fields[1].value.empty());
}

ReadResult ReadText(
    const std::string& content,
    int64_t splitStart = 0,
    int64_t splitEnd = std::numeric_limits<int64_t>::max(),
    bool projectValue = true,
    uint64_t batchSize = 3)
{
    const auto path = MakeTempPath(".txt");
    WriteBytes(path, content);
    auto options = MakeReaderOptions(path, splitStart, splitEnd, projectValue);
    auto reader = GetReaderFactory(codegen::FileFormat::TEXT)->CreateReader(options);
    auto rowReader = reader->CreateRowReader();

    ReadResult result;
    std::vector<vec::BaseVector*>* batch = nullptr;
    auto rows = rowReader->Next(&batch, nullptr, batchSize);
    while (rows > 0) {
        result.rows += rows;
        if (projectValue) {
            EXPECT_NE(batch, nullptr);
            EXPECT_EQ(batch == nullptr ? 0 : batch->size(), 1);
            if (batch != nullptr && batch->size() == 1) {
                for (uint64_t row = 0; row < rows; ++row) {
                    result.values.emplace_back(
                        vec::VectorHelper::GetStringValueFromVector(batch->at(0), row));
                }
            }
        }
        if (batch != nullptr) {
            for (auto* vector : *batch) {
                delete vector;
            }
            delete batch;
        }
        batch = nullptr;
        rows = rowReader->Next(&batch, nullptr, batchSize);
    }
    delete batch;
    std::remove(path.c_str());
    return result;
}

struct LazyReadResult {
    std::vector<std::vector<std::string>> values;
    std::vector<std::vector<bool>> nulls;
    std::vector<uint64_t> batchSizes;
    uint64_t rows = 0;
};

LazyReadResult ReadLazySimple(
    const std::string& content,
    const std::vector<std::string>& projectedNames,
    uint64_t batchSize)
{
    const auto path = MakeTempPath(".txt");
    WriteBytes(path, content);
    auto options = std::make_shared<ReaderOptions>();
    options->ParseEnhanceJson(
        R"({"text.source_kind":"HIVE_TEXT","text.codec_kind":"LAZY_SIMPLE",)"
        R"("text.charset":"UTF-8","text.line_separator":"",)"
        R"("text.compression_codec":"NONE","text.field_delimiter":"|",)"
        R"("text.null_literal":"NULL","text.escape_enabled":"true",)"
        R"("text.escape_char":"\\"})",
        codegen::FileFormat::TEXT);
    options->SetUri(std::make_shared<UriInfo>("file", path, "", "-1"));
    options->SetSplitStart(0);
    options->SetSplitEnd(std::numeric_limits<int64_t>::max());

    std::vector<type::DataTypePtr> projectedTypes(
        projectedNames.size(), type::VarcharType());
    auto projectedNamesCopy = projectedNames;
    options->SetRowType(type::ROW(
        std::move(projectedNamesCopy), std::move(projectedTypes)));
    options->SetFileRowType(type::ROW(
        std::vector<std::string>{"a", "b", "c", "d"},
        std::vector<type::DataTypePtr>{
            type::VarcharType(), type::VarcharType(),
            type::VarcharType(), type::VarcharType()}));

    auto reader = GetReaderFactory(codegen::FileFormat::TEXT)->CreateReader(options);
    auto rowReader = reader->CreateRowReader();
    LazyReadResult result;
    result.values.resize(projectedNames.size());
    result.nulls.resize(projectedNames.size());
    std::vector<vec::BaseVector*>* batch = nullptr;
    auto rows = rowReader->Next(&batch, nullptr, batchSize);
    while (rows > 0) {
        result.rows += rows;
        result.batchSizes.push_back(rows);
        EXPECT_NE(batch, nullptr);
        EXPECT_EQ(batch == nullptr ? 0 : batch->size(), projectedNames.size());
        if (batch != nullptr && batch->size() == projectedNames.size()) {
            for (size_t column = 0; column < batch->size(); ++column) {
                EXPECT_EQ(batch->at(column)->GetSize(), rows);
                for (uint64_t row = 0; row < rows; ++row) {
                    const auto isNull = batch->at(column)->IsNull(static_cast<int32_t>(row));
                    result.nulls[column].push_back(isNull);
                    result.values[column].push_back(isNull
                        ? std::string{}
                        : std::string(vec::VectorHelper::GetStringValueFromVector(
                              batch->at(column), row)));
                }
            }
        }
        if (batch != nullptr) {
            for (auto* vector : *batch) {
                delete vector;
            }
            delete batch;
        }
        batch = nullptr;
        rows = rowReader->Next(&batch, nullptr, batchSize);
    }
    delete batch;
    std::remove(path.c_str());
    return result;
}

TEST(TextReaderTest, ReadsDefaultLineBoundaries)
{
    auto result = ReadText("alpha\nbeta\r\ngamma\r\n\rdelta");
    EXPECT_EQ(result.values,
        (std::vector<std::string>{"alpha", "beta", "gamma", "", "delta"}));
}

TEST(TextReaderTest, HandlesEmptyAndTrailingRecords)
{
    EXPECT_TRUE(ReadText("").values.empty());
    EXPECT_EQ(ReadText("\n").values, (std::vector<std::string>{""}));
    EXPECT_EQ(ReadText("a\n").values, (std::vector<std::string>{"a"}));
    EXPECT_EQ(ReadText("a").values, (std::vector<std::string>{"a"}));
}

TEST(TextReaderTest, AlignsSplitInsideRecord)
{
    auto first = ReadText("aa\nbbb\nc", 0, 4);
    auto second = ReadText("aa\nbbb\nc", 4, 8);
    first.values.insert(first.values.end(), second.values.begin(), second.values.end());
    EXPECT_EQ(first.values, (std::vector<std::string>{"aa", "bbb", "c"}));
}

TEST(TextReaderTest, AlignsSplitInsideCrLf)
{
    auto first = ReadText("a\r\nb", 0, 2);
    auto second = ReadText("a\r\nb", 2, 4);
    first.values.insert(first.values.end(), second.values.begin(), second.values.end());
    EXPECT_EQ(first.values, (std::vector<std::string>{"a", "b"}));
}

TEST(TextReaderTest, ReadsLineLargerThanInputBuffer)
{
    const std::string longLine(128 * 1024, 'x');
    auto result = ReadText(longLine + "\ny");
    ASSERT_EQ(result.values.size(), 2);
    EXPECT_EQ(result.values[0], longLine);
    EXPECT_EQ(result.values[1], "y");

    const std::string crlfBoundaryLine(
        static_cast<size_t>(DEFAULT_TEXT_READ_BUFFER_SIZE - 1), 'z');
    result = ReadText(crlfBoundaryLine + "\r\ny");
    ASSERT_EQ(result.values.size(), 2);
    EXPECT_EQ(result.values[0], crlfBoundaryLine);
    EXPECT_EQ(result.values[1], "y");
}

TEST(TextReaderTest, SupportsEmptyProjection)
{
    EXPECT_EQ(ReadText("", 0, std::numeric_limits<int64_t>::max(), false).rows, 0);
    EXPECT_EQ(ReadText("\n", 0, std::numeric_limits<int64_t>::max(), false).rows, 1);
    auto result = ReadText("a\n\nb", 0, std::numeric_limits<int64_t>::max(), false, 2);
    EXPECT_EQ(result.rows, 3);
    EXPECT_TRUE(result.values.empty());

    const std::string splitContent = "aa\r\nbbb\rc\n\nlast";
    auto first = ReadText(splitContent, 0, 6, false, 2);
    auto second = ReadText(
        splitContent, 6, std::numeric_limits<int64_t>::max(), false, 2);
    EXPECT_EQ(first.rows + second.rows, 5);

    const std::string longLine(128 * 1024, 'x');
    EXPECT_EQ(ReadText(longLine + "\ny", 0,
        std::numeric_limits<int64_t>::max(), false, 1).rows, 2);

    const std::string crlfBoundaryLine(
        static_cast<size_t>(DEFAULT_TEXT_READ_BUFFER_SIZE - 1), 'z');
    EXPECT_EQ(ReadText(crlfBoundaryLine + "\r\ny", 0,
        std::numeric_limits<int64_t>::max(), false, 1).rows, 2);
}

TEST(TextReaderTest, ReadsLazySimpleProjectedColumnsWithoutFullRecordBatch)
{
    auto result = ReadLazySimple(
        "a\\|0|b0|c0|tail0\na1||NULL|tail1\na2|b2", {"c", "a"}, 2);
    EXPECT_EQ(result.rows, 3);
    EXPECT_EQ(result.batchSizes, (std::vector<uint64_t>{2, 1}));
    ASSERT_EQ(result.values.size(), 2);
    EXPECT_EQ(result.values[0], (std::vector<std::string>{"c0", "", ""}));
    EXPECT_EQ(result.nulls[0], (std::vector<bool>{false, true, true}));
    EXPECT_EQ(result.values[1], (std::vector<std::string>{"a|0", "a1", "a2"}));
    EXPECT_EQ(result.nulls[1], (std::vector<bool>{false, false, false}));
}

TEST(TextWriterTest, WritesFlatValuesEmptyAndNull)
{
    const auto path = MakeTempPath(".txt");
    UriInfo uri("file", path, "", "-1");
    TextWriter writer;
    writer.Init(uri);
    auto* vector = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(
        vec::VectorHelper::CreateStringVector(3));
    vector->SetValue(0, "value");
    vector->SetValue(1, "");
    vector->SetNull(2);
    writer.Write(vector, 0, 3);
    writer.Close();
    delete vector;
    EXPECT_EQ(ReadBytes(path), "value\n\n\n");
    std::remove(path.c_str());
}

TEST(TextWriterTest, CreatesMissingLocalParentDirectories)
{
    const auto basePath = MakeTempPath("");
    const auto temporaryPath = basePath + "/_temporary/attempt_0";
    const auto path = temporaryPath + "/part-00000.txt";
    UriInfo uri("file", path, "", "-1");
    TextWriter writer;

    ASSERT_NO_THROW(writer.Init(uri));
    writer.Close();

    std::ifstream output(path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.close();

    std::remove(path.c_str());
    rmdir(temporaryPath.c_str());
    rmdir((basePath + "/_temporary").c_str());
    rmdir(basePath.c_str());
}

TEST(TextWriterTest, WritesDictionaryAndConstVectors)
{
    const auto path = MakeTempPath(".txt");
    UriInfo uri("file", path, "", "-1");
    TextWriter writer;
    writer.Init(uri);

    auto* base = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(
        vec::VectorHelper::CreateStringVector(2));
    base->SetValue(0, "left");
    base->SetValue(1, "right");
    int32_t ids[] = {1, 0, 1};
    auto* dictionary = vec::VectorHelper::CreateStringDictionary(ids, 3, base);
    writer.Write(dictionary, 0, 3);

    auto* constant = new vec::ConstVector<std::string_view>("constant", type::OMNI_VARCHAR, 2);
    writer.Write(constant, 0, 2);
    writer.Close();

    delete dictionary;
    delete base;
    delete constant;
    EXPECT_EQ(ReadBytes(path), "right\nleft\nright\nconstant\nconstant\n");
    std::remove(path.c_str());
}

TEST(TextWriterTest, WritesLazySimpleMultipleColumns)
{
    TextFormatOptions options;
    options.sourceKind = TextSourceKind::HIVE_TEXT;
    options.codecKind = TextCodecKind::LAZY_SIMPLE;
    options.common.charset = "UTF-8";
    options.common.compressionCodec = "NONE";
    LazySimpleOptions lazy;
    lazy.delimited.fieldDelimiter = '|';
    lazy.delimited.nullLiteral = "NULL";
    options.dialect = lazy;
    auto schema = type::ROW(
        std::vector<std::string>{"name", "age"},
        std::vector<type::DataTypePtr>{type::VarcharType(), type::IntType()});

    const auto path = MakeTempPath(".txt");
    TextWriter writer(options, schema);
    writer.Init(UriInfo("file", path, "", "-1"));
    auto* names = reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>>*>(
        vec::VectorHelper::CreateStringVector(2));
    names->SetValue(0, "alice");
    names->SetNull(1);
    auto* ages = new vec::Vector<int32_t>(2);
    ages->SetValue(0, 10);
    ages->SetValue(1, 20);

    writer.Write({names, ages}, 0, 2);
    writer.Close();

    delete names;
    delete ages;
    EXPECT_EQ(ReadBytes(path), "alice|10\nNULL|20\n");
    std::remove(path.c_str());
}

} // namespace
} // namespace omniruntime::reader::text
