/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */

#include <arrow/io/memory.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

#include "codegen/Options.h"
#include "reader/ReaderFactory.h"
#include "reader/ReaderOptions.h"
#include "reader/common/UriInfo.h"
#include "reader/text/TextCompressionStream.h"
#include "reader/text/TextFormatOptions.h"
#include "reader/text/TextLineScanner.h"
#include "reader/text/TextValueConverter.h"
#include "reader/text/TextWriter.h"
#include "reader/text/TextReader.h"
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

TextFormatOptions CsvTestOptions(bool hive = false, bool writing = false)
{
    TextFormatOptions options;
    options.sourceKind = hive ? TextSourceKind::HIVE_TEXT : TextSourceKind::SPARK_CSV;
    options.codecKind = TextCodecKind::CSV;
    options.common.charset = "UTF-8";
    options.common.compressionCodec = "NONE";
    CsvOptions csv;
    csv.delimited.fieldDelimiter = ',';
    csv.delimited.escapeEnabled = true;
    csv.delimited.escapeChar = hive ? '"' : '\\';
    csv.delimited.nullLiteral = "NULL";
    csv.parseMode = "PERMISSIVE";
    csv.emptyValue = writing && !hive ? "\"\"" : "";
    csv.ignoreLeadingWhitespace = writing && !hive;
    csv.ignoreTrailingWhitespace = writing && !hive;
    options.dialect = csv;
    return options;
}

TEST(CsvCodecTest, ProjectionSkipsUnneededEscapesAndPreservesOutputOrder)
{
    const auto options = CsvTestOptions();
    auto codec = CreateTextCodec(options, {2, 0, 4});
    DecodedTextRecord decoded;
    codec->DecodeRecord(R"(left,"ignored\"field",NULL,tail)", decoded);
    ASSERT_EQ(decoded.fields.size(), 3);
    EXPECT_TRUE(decoded.fields[0].isNull);
    EXPECT_EQ(decoded.fields[1].value, "left");
    EXPECT_TRUE(decoded.fields[2].isNull);
    EXPECT_TRUE(decoded.storage.empty());
}

TEST(CsvCodecTest, QuotingEscapesMissingAndEmptyFields)
{
    auto codec = CreateTextCodec(CsvTestOptions());
    DecodedTextRecord decoded;
    codec->DecodeRecord(R"("a,b","a\"b","a""b","",,NULL,)", decoded);
    ASSERT_EQ(decoded.fields.size(), 7);
    EXPECT_EQ(decoded.fields[0].value, "a,b");
    EXPECT_EQ(decoded.fields[1].value, "a\"b");
    EXPECT_EQ(decoded.fields[2].value, "\"a\"\"b\"");
    EXPECT_FALSE(decoded.fields[3].isNull);
    EXPECT_TRUE(decoded.fields[3].value.empty());
    EXPECT_TRUE(decoded.fields[4].isNull);
    EXPECT_TRUE(decoded.fields[5].isNull);
    EXPECT_TRUE(decoded.fields[6].isNull);
    codec->DecodeRecord("\"abc\"   ,tail", decoded);
    ASSERT_EQ(decoded.fields.size(), 2);
    EXPECT_EQ(decoded.fields[0].value, "abc");
    codec->DecodeRecord("\"abc\"x,tail", decoded);
    EXPECT_EQ(decoded.fields[0].value, "\"abc\"x");
}

TEST(CsvCodecTest, HiveDefaultsAndSparkWriterHaveDistinctSemantics)
{
    auto hive = CreateTextCodec(CsvTestOptions(true), {0, 1, 2});
    DecodedTextRecord decoded;
    hive->DecodeRecord(R"("a""b",,NULL)", decoded);
    ASSERT_EQ(decoded.fields.size(), 3);
    EXPECT_EQ(decoded.fields[0].value, "a\"b");
    EXPECT_FALSE(decoded.fields[1].isNull);
    EXPECT_EQ(decoded.fields[2].value, "NULL");
    hive->DecodeRecord("", decoded);
    EXPECT_TRUE(decoded.fields[0].isNull);
    hive->DecodeRecord("a,\"unfinished", decoded);
    EXPECT_EQ(decoded.fields[0].value, "a");
    EXPECT_TRUE(decoded.fields[1].isNull);
    hive->DecodeRecord(R"("plain","a,b",tail)", decoded);
    EXPECT_EQ(decoded.fields[0].value, "plain");
    EXPECT_EQ(decoded.fields[1].value, "a,b");
    EXPECT_EQ(decoded.fields[2].value, "tail");
    EXPECT_TRUE(decoded.storage.empty());

    std::string encoded;
    auto sparkWriter = CreateTextCodec(CsvTestOptions(false, true));
    sparkWriter->EncodeRecord({{false, " a "}, {false, ""}, {true, {}}, {false, "a\"b"}}, encoded);
    EXPECT_EQ(encoded, R"(a,"",NULL,"a\"b")");
    hive->EncodeRecord({{false, " a "}, {false, ""}, {true, {}}, {false, "a\"b"}}, encoded);
    EXPECT_EQ(encoded, R"(" a ","",,"a""b")");

    auto sparkOptions = CsvTestOptions(false, true);
    auto csv = sparkOptions.Csv();
    csv.quoteAll = true;
    sparkOptions.dialect = csv;
    auto configuredWriter = CreateTextCodec(sparkOptions);
    configuredWriter->EncodeRecord({{false, "plain"}, {false, "a\"b"}}, encoded);
    EXPECT_EQ(encoded, R"("plain","a\"b")");

    csv.quoteAll = false;
    csv.escapeQuotes = false;
    sparkOptions.dialect = csv;
    configuredWriter = CreateTextCodec(sparkOptions);
    configuredWriter->EncodeRecord(
        {{false, "test \"quote\""}, {false, "a,b"}, {false, "\"very\" well"}}, encoded);
    EXPECT_EQ(encoded, R"(test "quote","a,b","\"very\" well")");
}

TEST(CsvCodecTest, SparkDefaultEmptyValueIsIndependentOfQuoteCharacter)
{
    auto options = CsvTestOptions(false, true);
    auto csv = options.Csv();
    csv.quote = '\'';
    options.dialect = csv;
    auto codec = CreateTextCodec(options);
    std::string encoded;
    codec->EncodeRecord({{false, ""}, {false, " a,b "}}, encoded);
    EXPECT_EQ(encoded, "\"\",'a,b'");
}

TEST(CsvCodecTest, SparkWhitespaceAndCustomEmptyValueFollowOptions)
{
    auto options = CsvTestOptions();
    auto csv = options.Csv();
    csv.ignoreLeadingWhitespace = true;
    csv.ignoreTrailingWhitespace = true;
    csv.emptyValue = "EMPTY";
    options.dialect = csv;
    auto codec = CreateTextCodec(options);
    DecodedTextRecord decoded;
    codec->DecodeRecord(R"(  value  ,"  quoted  ","",)", decoded);
    ASSERT_EQ(decoded.fields.size(), 4);
    EXPECT_EQ(decoded.fields[0].value, "value");
    EXPECT_EQ(decoded.fields[1].value, "  quoted  ");
    EXPECT_EQ(decoded.fields[2].value, "EMPTY");
    EXPECT_TRUE(decoded.fields[3].isNull);

    std::string encoded;
    codec->EncodeRecord({{false, " value "}, {false, ""}}, encoded);
    EXPECT_EQ(encoded, "value,EMPTY");
}

TEST(CsvCodecTest, SparkWriterDistinguishesLeadingAndTrailingWhitespaceTrimming)
{
    auto options = CsvTestOptions(false, true);
    auto csv = options.Csv();
    csv.ignoreTrailingWhitespace = true;
    options.dialect = csv;
    auto codec = CreateTextCodec(options);
    std::string encoded;
    codec->EncodeRecord({{false, ""}, {false, " "}, {false, "x"}}, encoded);
    EXPECT_EQ(encoded, R"("","",x)");

    csv.ignoreLeadingWhitespace = false;
    options.dialect = csv;
    codec = CreateTextCodec(options);
    codec->EncodeRecord({{false, ""}, {false, " "}, {false, "x"}}, encoded);
    EXPECT_EQ(encoded, R"("", \"\,x)");

    csv.emptyValue.clear();
    options.dialect = csv;
    codec = CreateTextCodec(options);
    codec->EncodeRecord({{false, ""}, {false, " "}, {false, "x"}}, encoded);
    EXPECT_EQ(encoded, ",,x");
}

TEST(TextLineScannerTest, CsvCountSkipsBlankLinesAcrossSmallBuffers)
{
    const std::string contents = " \r\n\t\nfirst\r" + std::string(70000, 'x') + "\n\nlast";
    auto file = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(contents));
    TextLineScanner scanner(file, contents.size(), 0, contents.size(), 7);
    EXPECT_EQ(scanner.CountRows(2, true), 2);
    EXPECT_EQ(scanner.CountRows(2, true), 1);
    EXPECT_EQ(scanner.CountRows(2, true), 0);
    TextLineScanner raw(file, contents.size(), 0, contents.size(), 7);
    EXPECT_EQ(raw.CountRows(10), 6);
}

TEST(TextReaderTest, CsvHeaderProjectionAndWriterUseCommonTextPath)
{
    auto format = CsvTestOptions();
    std::get<CsvOptions>(format.dialect).delimited.emitHeader = true;
    const auto path = MakeTempPath(".csv");
    auto schema = type::ROW(std::vector<std::string>{"name", "age"},
        std::vector<type::DataTypePtr>{type::VarcharType(), type::VarcharType()});
    {
        TextWriter writer(format, schema);
        writer.Init(UriInfo("file", path, "", "-1"));
        writer.Close();
    }
    EXPECT_EQ(ReadBytes(path), "name,age\n");
    WriteBytes(path, "name,age\nfirst,10\n\nsecond,20\n");
    auto options = MakeReaderOptions(path, 0, std::numeric_limits<int64_t>::max(), false);
    options->SetFileRowType(schema);
    options->SetRowType(type::ROW(std::vector<std::string>{"age"},
        std::vector<type::DataTypePtr>{type::VarcharType()}));
    auto& csv = std::get<CsvOptions>(format.dialect);
    csv.delimited.emitHeader = false;
    csv.delimited.skipInputLines = 1;
    TextReader reader(options, format);
    reader.InitReader();
    auto rows = reader.CreateRowReader();
    std::vector<vec::BaseVector*>* batch = nullptr;
    ASSERT_EQ(rows->Next(&batch, nullptr, 17), 2);
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->size(), 1);
    EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(batch->front(), 0), "10");
    EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(batch->front(), 1), "20");
    delete batch->front();
    delete batch;
    std::remove(path.c_str());
}

TEST(TextReaderTest, CsvZeroProjectionUsesNoVectorsAndSplitsDoNotDuplicateRows)
{
    const auto path = MakeTempPath(".csv");
    const std::string contents = " \nname,age\r\nfirst,10\rsecond,20\n\nlast,30";
    WriteBytes(path, contents);
    auto format = CsvTestOptions();
    std::get<CsvOptions>(format.dialect).delimited.skipInputLines = 1;
    uint64_t total = 0;
    for (int64_t start = 0; start < static_cast<int64_t>(contents.size()); start += 16) {
        auto options = MakeReaderOptions(path, start, start + 16, false);
        TextReader reader(options, format);
        reader.InitReader();
        auto rowReader = reader.CreateRowReader();
        uint64_t count = 1;
        while (count != 0) {
            std::vector<vec::BaseVector*>* batch = nullptr;
            count = rowReader->Next(&batch, nullptr, 2);
            total += count;
            if (batch != nullptr) {
                EXPECT_TRUE(batch->empty());
            }
            delete batch;
        }
    }
    EXPECT_EQ(total, 3);
    std::remove(path.c_str());
}

TEST(TextReaderTest, CsvCommentIsIgnoredByHeaderAndZeroProjection)
{
    const auto path = MakeTempPath(".csv");
    WriteBytes(path, "# before header\n\nname,age\n# data comment\nfirst,10\nsecond,20\n");
    auto format = CsvTestOptions();
    auto& csv = std::get<CsvOptions>(format.dialect);
    csv.comment = '#';
    csv.delimited.skipInputLines = 1;
    auto options = MakeReaderOptions(
        path, 0, std::numeric_limits<int64_t>::max(), false);
    TextReader reader(options, format);
    reader.InitReader();
    auto rowReader = reader.CreateRowReader();
    std::vector<vec::BaseVector*>* batch = nullptr;
    EXPECT_EQ(rowReader->Next(&batch, nullptr, 17), 2);
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(batch->empty());
    delete batch;
    std::remove(path.c_str());
}

TEST(TextReaderTest, OpenCsvHeaderSkipsMultiplePhysicalRecords)
{
    const auto path = MakeTempPath(".csv");
    WriteBytes(path, "\nname,age\nfirst,10\n");
    auto format = CsvTestOptions(true);
    std::get<CsvOptions>(format.dialect).delimited.skipInputLines = 2;
    auto options = MakeReaderOptions(
        path, 0, std::numeric_limits<int64_t>::max(), false);
    options->SetFileRowType(type::ROW(std::vector<std::string>{"name", "age"},
        std::vector<type::DataTypePtr>{type::VarcharType(), type::VarcharType()}));
    TextReader reader(options, format);
    reader.InitReader();
    auto rowReader = reader.CreateRowReader();
    std::vector<vec::BaseVector*>* batch = nullptr;
    EXPECT_EQ(rowReader->Next(&batch, nullptr, 17), 1);
    delete batch;
    std::remove(path.c_str());
}

TEST(TextLineScannerTest, CsvBomAndCountPreserveDataAndIgnoreBlankRecords)
{
    const std::string contents = "\xef\xbb\xbf \r\nvalue\r\n";
    auto file = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(contents));
    TextLineScanner scanner(file, contents.size(), 0, contents.size(), 4, true);
    EXPECT_EQ(scanner.CountRows(10, true), 1);
    TextLineScanner records(file, contents.size(), 0, contents.size(), 4, true);
    std::string_view value;
    ASSERT_TRUE(records.NextLine(value));
    EXPECT_EQ(value, " ");
    ASSERT_TRUE(records.NextLine(value));
    EXPECT_EQ(value, "value");
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
    invalid.common.splitable = false;
    EXPECT_NO_THROW(invalid.Validate());
    invalid.common.compressionCodec = "bzip2";
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
    lazyDialect.lastColumnTakesRest = true;
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

    lazy.lastColumnTakesRest = true;
    options.dialect = lazy;
    codec = CreateTextCodec(options, {2, 0}, 3);
    codec->DecodeRecord(R"(left|middle|last|keeps\|delimiters)", decoded);
    ASSERT_EQ(decoded.fields.size(), 2);
    EXPECT_EQ(decoded.fields[0].value, "last|keeps|delimiters");
    EXPECT_EQ(decoded.fields[1].value, "left");
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
    uint64_t batchSize,
    uint32_t skipInputLines = 0)
{
    const auto path = MakeTempPath(".txt");
    WriteBytes(path, content);
    auto options = std::make_shared<ReaderOptions>();
    std::string enhancementJson =
        R"({"text.source_kind":"HIVE_TEXT","text.codec_kind":"LAZY_SIMPLE",)"
        R"("text.charset":"UTF-8","text.line_separator":"",)"
        R"("text.compression_codec":"NONE","text.field_delimiter":"|",)"
        R"("text.null_literal":"NULL","text.escape_enabled":"true",)"
        R"("text.escape_char":"\\","text.skip_input_lines":")";
    enhancementJson += std::to_string(skipInputLines);
    enhancementJson += R"("})";
    options->ParseEnhanceJson(enhancementJson, codegen::FileFormat::TEXT);
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

TEST(TextReaderTest, SkipsMultipleLazySimpleHeaderRecords)
{
    auto result = ReadLazySimple(
        "\nignored|ignored|ignored|ignored\na0|b0|c0|d0\na1|b1|c1|d1",
        {"a", "c"}, 4, 2);
    EXPECT_EQ(result.rows, 2);
    EXPECT_EQ(result.values[0], (std::vector<std::string>{"a0", "a1"}));
    EXPECT_EQ(result.values[1], (std::vector<std::string>{"c0", "c1"}));
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

TEST(TextWriterTest, RoundTripsSupportedCompressionCodecs)
{
    const std::vector<std::string> codecs = {"GZIP", "DEFLATE", "SNAPPY", "LZ4"};
    const std::string longValue(300000, 'x');
    for (const auto& compression : codecs) {
        TextFormatOptions format;
        format.sourceKind = TextSourceKind::SPARK_TEXT;
        format.codecKind = TextCodecKind::RAW_LINE;
        format.common.charset = "UTF-8";
        format.common.compressionCodec = compression;
        format.common.splitable = false;
        format.dialect = RawLineOptions{};
        const auto path = MakeTempPath("." + compression);
        auto schema = type::ROW(std::vector<std::string>{"value"},
            std::vector<type::DataTypePtr>{type::VarcharType()});
        {
            TextWriter writer(format, schema);
            writer.Init(UriInfo("file", path, "", "-1"));
            auto* values = reinterpret_cast<
                vec::Vector<vec::LargeStringContainer<std::string_view>>*>(
                vec::VectorHelper::CreateStringVector(3));
            values->SetValue(0, "first");
            values->SetValue(1, longValue);
            values->SetValue(2, "last");
            writer.Write(values, 0, 3);
            writer.Close();
            delete values;
        }

        auto readerOptions = MakeReaderOptions(
            path, 0, std::numeric_limits<int64_t>::max(), true);
        TextReader reader(readerOptions, format);
        reader.InitReader();
        auto rowReader = reader.CreateRowReader();
        std::vector<vec::BaseVector*>* batch = nullptr;
        ASSERT_EQ(rowReader->Next(&batch, nullptr, 8), 3) << compression;
        ASSERT_NE(batch, nullptr);
        ASSERT_EQ(batch->size(), 1);
        EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(batch->front(), 0), "first");
        EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(batch->front(), 1), longValue);
        EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(batch->front(), 2), "last");
        delete batch->front();
        delete batch;

        auto countOptions = MakeReaderOptions(
            path, 0, std::numeric_limits<int64_t>::max(), false);
        TextReader countReader(countOptions, format);
        countReader.InitReader();
        auto countRows = countReader.CreateRowReader();
        batch = nullptr;
        EXPECT_EQ(countRows->Next(&batch, nullptr, 8), 3) << compression;
        ASSERT_NE(batch, nullptr);
        EXPECT_TRUE(batch->empty());
        delete batch;
        std::remove(path.c_str());

        const auto emptyPath = MakeTempPath(".empty." + compression);
        {
            TextWriter writer(format, schema);
            writer.Init(UriInfo("file", emptyPath, "", "-1"));
            writer.Close();
        }
        auto emptyOptions = MakeReaderOptions(
            emptyPath, 0, std::numeric_limits<int64_t>::max(), true);
        TextReader emptyReader(emptyOptions, format);
        emptyReader.InitReader();
        auto emptyRows = emptyReader.CreateRowReader();
        batch = nullptr;
        EXPECT_EQ(emptyRows->Next(&batch, nullptr, 8), 0) << compression;
        EXPECT_EQ(batch, nullptr);
        std::remove(emptyPath.c_str());
    }
}

TEST(TextCompressionStreamTest, RejectsChunkBeyondRemainingFileBeforeAllocation)
{
    // One-byte decoded block, claiming a 1 MB compressed chunk with no payload.
    const std::string header("\0\0\0\1\0\x10\0\0", 8);
    for (const auto codec : {TextCompressionKind::SNAPPY, TextCompressionKind::LZ4}) {
        auto file = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(header));
        auto input = CreateTextSequentialInput(file, codec);
        uint8_t value = 0;
        try {
            input->Read(&value, 1);
            FAIL() << "Truncated chunk was accepted";
        } catch (const std::runtime_error& error) {
            EXPECT_STREQ(error.what(), "Truncated Hadoop Text compressed chunk.");
        }
    }
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

TEST(TextValueConverterTest, ReusesTimeExpressionsForCustomFormats)
{
    TextValueConverter converter("UTC");
    std::unique_ptr<vec::BaseVector> timestampsInput(
        vec::VectorHelper::CreateStringVector(3));
    auto* timestampStrings = reinterpret_cast<
        vec::Vector<vec::LargeStringContainer<std::string_view>>*>(timestampsInput.get());
    timestampStrings->SetValue(0, "2026/08/01 04:34:56");
    timestampStrings->SetValue(1, "2026-08-02 05:35:57");
    timestampStrings->SetValue(2, "invalid");
    auto timestamps = converter.DecodeColumn(std::move(timestampsInput), type::TimestampType(), {},
        {"yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"});
    EXPECT_FALSE(timestamps->IsNull(0));
    EXPECT_FALSE(timestamps->IsNull(1));
    EXPECT_TRUE(timestamps->IsNull(2));

    auto formattedTimestamp = converter.EncodeColumn(
        timestamps.get(), type::TimestampType(), 0, 2, {}, "yyyy/MM/dd HH:mm:ss");
    EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(formattedTimestamp.get(), 0),
        "2026/08/01 04:34:56");
    EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(formattedTimestamp.get(), 1),
        "2026/08/02 05:35:57");

    std::unique_ptr<vec::BaseVector> datesInput(vec::VectorHelper::CreateStringVector(1));
    auto* dateStrings = reinterpret_cast<
        vec::Vector<vec::LargeStringContainer<std::string_view>>*>(datesInput.get());
    dateStrings->SetValue(0, "2026/08/03");
    auto dates = converter.DecodeColumn(
        std::move(datesInput), type::Date32Type(), "yyyy/MM/dd");
    ASSERT_FALSE(dates->IsNull(0));
    auto formattedDate = converter.EncodeColumn(
        dates.get(), type::Date32Type(), 0, 1, "yyyy/MM/dd");
    EXPECT_EQ(vec::VectorHelper::GetStringValueFromVector(formattedDate.get(), 0), "2026/08/03");
}

} // namespace
} // namespace omniruntime::reader::text
