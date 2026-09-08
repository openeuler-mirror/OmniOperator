/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Tests for the ORC reader producing StringView (OMNI_STRING_VIEW) vectors
// directly when the caller requests type id 26 per column.
// Step 1 (this file): the DIRECT-encoded string path — dictionary disabled via
// setDictionaryKeySizeThreshold(0.0). Dictionary (Plan A') is covered separately.

#include "reader/orc/OmniWriter.hh"
#include "reader/orc/OmniRowReaderImpl.hh"
#include "reader/orc/OrcFileOverride.hh"
#include "vector/vector.h"
#include "vector/string_view.h"
#include <vector/vector_common.h>
#include "orc/OrcFile.hh"
#include "scan_test.h"
#include <memory>
#include <orc/Type.hh>
#include <gtest/gtest.h>

using namespace omniruntime::vec;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

using OmniVarcharVector = Vector<LargeStringContainer<std::string_view>>;
using OmniStringViewVector = Vector<StringView>;

class StringViewDirectReadTest : public testing::Test {
protected:
    std::string filename;
    std::vector<BaseVector*> readBatch;

    virtual void SetUp() override {
        setenv("TZ", "Asia/Shanghai", 1);
        tzset();
        filename = "/tmp/omni_test/sv_direct_read_test_" + std::to_string(std::time(nullptr)) + ".orc";
    }

    virtual void TearDown() override {
        for (auto v : readBatch) delete v;
        readBatch.clear();
        remove(filename.c_str());
    }

    // Read ORC file; omniTypeIds is one type id per column (nullptr = auto-detect from ORC schema).
    void ScanFile(int numRows, int* omniTypeIds) {
        orc::ReaderOptions readerOpts;
        std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
                readFileOverride(UriInfo("file", filename, "", "-1")), readerOpts);

        orc::RowReaderOptions rowOpts;
        std::unique_ptr<common::JulianGregorianRebase> julian;
        std::unique_ptr<common::PredicateCondition> pred;

        auto readerImpl = dynamic_cast<OmniReaderImpl*>(reader.get());
        ASSERT_NE(readerImpl, nullptr) << "Failed to create OmniReaderImpl";

        auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
        auto omniRowReader = dynamic_cast<OmniRowReaderImpl*>(rowReader.get());
        ASSERT_NE(omniRowReader, nullptr) << "Failed to create OmniRowReaderImpl";

        omniRowReader->next(&readBatch, omniTypeIds, numRows);
    }

    // Write a single-column ORC file with DICTIONARY DISABLED (threshold 0.0 -> direct encoding).
    void WriteStringOrcFileDirect(const std::vector<std::string>& data,
                                  const std::vector<bool>& isNulls,
                                  std::unique_ptr<orc::Type> colType)
    {
        int numRows = static_cast<int>(data.size());

        UriInfo uri("file", filename, "", "-1");
        std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
        std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
        schema->addStructField("c0", std::move(colType));

        orc::WriterOptions options;
        options.setMemoryPool(orc::getDefaultPool());
        options.setStripeSize(67108864);
        options.setTimezoneName("GMT");
        options.setDictionaryKeySizeThreshold(0.0);   // force DIRECT encoding
        std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

        auto valVec = std::make_unique<OmniVarcharVector>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (!isNulls.empty() && isNulls[i]) {
                valVec->SetNull(i);
            } else {
                std::string_view sv(data[i]);
                valVec->SetValue(i, sv);
                valVec->SetNotNull(i);
            }
        }

        std::vector<BaseVector *> cols;
        cols.push_back(valVec.get());
        auto rowVec = std::make_unique<RowVector>(numRows, cols);
        for (int i = 0; i < numRows; ++i) {
            rowVec->SetNotNull(i);
        }

        writer->add(rowVec.get(), 0, numRows);
        writer->close();
    }
};

// Direct-encoded STRING column, request StringView. Mix of inline (<=12B) and
// arena (>12B) payloads to exercise both StringView storage forms.
TEST_F(StringViewDirectReadTest, ReadStringAsStringViewDirect)
{
    std::vector<std::string> data = {
        "apple",                                  // 5B  inline
        "abcdefghijkl",                           // 12B inline (boundary)
        "this_is_a_long_string_over_twelve",      // >12B arena
        "x",                                      // 1B  inline
        "another_fairly_long_value_here_ok"       // >12B arena
    };
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);

    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW)
        << "Expected OMNI_STRING_VIEW output type";
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT)
        << "Direct StringView output should be a flat vector";

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr) << "Failed to cast to Vector<StringView>";

    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "Mismatch at row " << i;
    }
}

// Direct-encoded STRING with nulls and an empty string.
TEST_F(StringViewDirectReadTest, ReadStringAsStringViewDirectWithNullsAndEmpty)
{
    std::vector<std::string> data = {
        "hello",
        "",                                       // empty string
        "a_long_enough_value_beyond_twelve",      // >12B arena
        "",                                       // null slot below
        "tail"
    };
    std::vector<bool> isNulls = {false, false, false, true, false};
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, isNulls, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        if (isNulls[i]) {
            ASSERT_TRUE(svVec->IsNull(i)) << "Row " << i << " should be NULL";
        } else {
            ASSERT_FALSE(svVec->IsNull(i)) << "Row " << i << " should NOT be NULL";
            std::string_view actual = svVec->GetValueRef(i);
            ASSERT_EQ(actual, data[i]) << "Mismatch at row " << i;
        }
    }
}

// Direct-encoded CHAR column: reader trims trailing spaces, outputs StringView. Includes a >12B
// value (was blocked by the OmniCharColumnWriter pad-length underflow, now fixed).
TEST_F(StringViewDirectReadTest, ReadCharAsStringViewDirect)
{
    std::vector<std::string> data = {"cat", "elephant", "fox", "hippopotamus_xyz"};
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, {}, orc::createCharType(orc::TypeKind::CHAR, 20));

    int charSvTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &charSvTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "CHAR trailing-space trim mismatch at row " << i;
    }
}

// Baseline: read the SAME CHAR-direct file as VARCHAR (nullptr type ids). Guards the CHAR-direct
// write+read path independent of StringView (regression for the OmniCharColumnWriter pad fix).
TEST_F(StringViewDirectReadTest, ReadCharDirectAsVarcharBaseline)
{
    std::vector<std::string> data = {"cat", "elephant", "fox", "hippopotamus_xyz"};
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, {}, orc::createCharType(orc::TypeKind::CHAR, 20));

    ScanFile(numRows, nullptr);
    ASSERT_EQ(readBatch.size(), 1);
    // Report actual encoding (OMNI_FLAT=? vs OMNI_DICTIONARY) via failure message if not flat.
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT)
        << "CHAR-direct encoding was " << readBatch[0]->GetEncoding()
        << " (OMNI_DICTIONARY would mean threshold 0.0 did not force direct for CHAR)";
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_VARCHAR);

    auto *vcVec = dynamic_cast<OmniVarcharVector *>(readBatch[0]);
    ASSERT_NE(vcVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = vcVec->GetValue(i);
        ASSERT_EQ(actual, data[i]) << "CHAR-direct VARCHAR mismatch at row " << i;
    }
}

// Regression: same direct STRING column with nullptr type ids still yields VARCHAR.
TEST_F(StringViewDirectReadTest, ReadStringDefaultStillVarchar)
{
    std::vector<std::string> data = {"regress_one", "regress_two_longer_than_twelve", "r3"};
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    ScanFile(numRows, nullptr);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_VARCHAR)
        << "Default (nullptr type ids) must remain VARCHAR";

    auto *vcVec = dynamic_cast<OmniVarcharVector *>(readBatch[0]);
    ASSERT_NE(vcVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = vcVec->GetValue(i);
        ASSERT_EQ(actual, data[i]) << "VARCHAR mismatch at row " << i;
    }
}

// Non-ASCII / UTF-8 multibyte: StringView stores raw bytes, so multibyte values must round-trip
// byte-exact. Mixes inline (<=12B) and arena (>12B) multibyte payloads. (Source file is UTF-8, so
// plain "" literals carry the UTF-8 bytes.)
TEST_F(StringViewDirectReadTest, ReadUtf8MultibyteStringViewDirect)
{
    std::vector<std::string> data = {
        "café",                                  // 5B  (é=2B) inline
        "日本語",                                 // 9B  (3x3) inline
        "😀🎉",                                   // 8B  (2 emoji x4B) inline
        "中文字符串测试内容超过十二字节",              // >12B multibyte arena
        "Grüße_aus_München_länger"               // >12B accented arena
    };
    int numRows = static_cast<int>(data.size());

    WriteStringOrcFileDirect(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "UTF-8 byte-exact mismatch at row " << i;
    }
}

// Large data forcing arena growth: many UNIQUE >12B strings so the flat StringView vector's string
// buffer must GrowStringBuffer (INITIAL_STRING_SIZE = 32KB) several times. Verifies the 2x realloc
// path AND the non-inline pointer fixup (a growth bug would corrupt earlier rows' payloads).
TEST_F(StringViewDirectReadTest, ReadLargeArenaGrowthStringViewDirect)
{
    int numRows = 3000;   // ~3000 x ~35B ≈ 105KB >> 32KB initial arena => multiple growths
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = "arena_growth_unique_value_row_" + std::to_string(i);   // unique, >12B
    }

    WriteStringOrcFileDirect(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "arena-growth mismatch at row " << i;
    }
}
