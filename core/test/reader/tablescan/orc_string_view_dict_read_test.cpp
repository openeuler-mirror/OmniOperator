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

// Step 2: ORC reader producing StringView (OMNI_STRING_VIEW) for DICTIONARY-encoded string
// columns. Plan A' — materialize to a FLAT Vector<StringView> (not a DictionaryVector), but
// zero-copy: rows share the per-stripe StringView dictionary template's string buffer.

#include "reader/orc/OmniWriter.hh"
#include "reader/orc/OmniRowReaderImpl.hh"
#include "reader/orc/OrcFileOverride.hh"
#include "vector/vector.h"
#include "vector/string_view.h"
#include "vector/dictionary_container.h"
#include <vector/vector_common.h>
#include "orc/OrcFile.hh"
#include "scan_test.h"
#include <memory>
#include <orc/Type.hh>
#include <gtest/gtest.h>

using namespace omniruntime::vec;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

using OmniStringViewVector = Vector<StringView>;

class StringViewDictReadTest : public testing::Test {
protected:
    std::string filename;
    std::vector<BaseVector*> readBatch;

    virtual void SetUp() override {
        setenv("TZ", "Asia/Shanghai", 1);
        tzset();
        filename = "/tmp/omni_test/sv_dict_read_test_" + std::to_string(std::time(nullptr)) + ".orc";
    }

    virtual void TearDown() override {
        for (auto v : readBatch) delete v;
        readBatch.clear();
        remove(filename.c_str());
    }

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

    // Write a single string column ORC file with DICTIONARY ENABLED (threshold 1.0 -> always dict).
    void WriteDictStringOrcFile(const std::vector<std::string>& data,
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
        options.setDictionaryKeySizeThreshold(1.0);   // force DICTIONARY encoding
        std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

        auto valVec = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numRows);
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

// Dictionary STRING with mixed short (inline) + long (>12B arena) entries, requested as StringView.
// Must materialize to a FLAT Vector<StringView> and resolve repeated indices correctly.
TEST_F(StringViewDictReadTest, ReadStringDictAsStringView)
{
    int32_t numRows = 100;
    std::vector<std::string> dict = {
        "apple",                              // 5B  inline
        "a_very_long_dictionary_value_xyz",   // 32B arena
        "cherry",                             // 6B  inline
        "another_long_one_over_twelve",       // 28B arena
        "fig"                                 // 3B  inline
    };
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }

    WriteDictStringOrcFile(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);

    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT)
        << "Plan A' materializes dictionary columns to a flat StringView vector";

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr) << "Failed to cast to Vector<StringView>";

    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "Mismatch at row " << i;
    }
}

// Dictionary STRING with nulls, requested as StringView.
TEST_F(StringViewDictReadTest, ReadStringDictAsStringViewWithNulls)
{
    int32_t numRows = 100;
    std::vector<std::string> dict = {"alpha", "bravo_long_enough_over_twelve", "charlie", "delta", "echo"};
    std::vector<std::string> data(numRows);
    std::vector<bool> isNulls(numRows, false);
    for (int i = 0; i < numRows; ++i) {
        if (i % 7 == 0) {
            isNulls[i] = true;
        } else {
            data[i] = dict[i % dict.size()];
        }
    }

    WriteDictStringOrcFile(data, isNulls, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT);

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

// Dictionary CHAR column as StringView, including a >12B entry (was blocked by the
// OmniCharColumnWriter pad-length underflow, now fixed): materialize to flat SV with CHAR trim.
TEST_F(StringViewDictReadTest, ReadCharDictAsStringView)
{
    int32_t numRows = 60;
    std::vector<std::string> dict = {"cat", "hippopotamus_long", "fox"};   // one >12B
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }

    WriteDictStringOrcFile(data, {}, orc::createCharType(orc::TypeKind::CHAR, 20));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "CHAR trailing-space trim mismatch at row " << i;
    }
}

// CHAR dictionary -> StringView, short (<=12B) values only: focused check of CHAR trailing-space
// trim on the StringView materialization path (the >12B case is covered by ReadCharDictAsStringView).
TEST_F(StringViewDictReadTest, ReadCharDictAsStringViewShortOnly)
{
    int32_t numRows = 60;
    std::vector<std::string> dict = {"cat", "dog", "fox"};   // all <=12B inline
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }
    WriteDictStringOrcFile(data, {}, orc::createCharType(orc::TypeKind::CHAR, 20));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "row " << i;
    }
}

// Baseline/regression: a CHAR dictionary with a >12B entry read as OMNI_CHAR (via nextAsDictionary,
// NO StringView). Guards the OmniCharColumnWriter pad fix independent of StringView.
TEST_F(StringViewDictReadTest, ReadCharDictAsCharBaseline)
{
    int32_t numRows = 60;
    std::vector<std::string> dict = {"cat", "hippopotamus_long", "fox"};   // one >12B
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }
    WriteDictStringOrcFile(data, {}, orc::createCharType(orc::TypeKind::CHAR, 20));

    int charTypeId = omniruntime::type::OMNI_CHAR;
    ScanFile(numRows, &charTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_DICTIONARY);
    auto *dictVec = dynamic_cast<Vector<DictionaryContainer<std::string_view>> *>(readBatch[0]);
    ASSERT_NE(dictVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(dictVec->GetValue(i), data[i]) << "row " << i;
    }
}

// Regression: dictionary column with nullptr type ids still yields a DictionaryVector (VARCHAR).
TEST_F(StringViewDictReadTest, ReadStringDictDefaultStillDictVector)
{
    int32_t numRows = 40;
    std::vector<std::string> dict = {"red", "green", "blue", "yellow"};
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }

    WriteDictStringOrcFile(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    ScanFile(numRows, nullptr);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_DICTIONARY)
        << "Default (nullptr type ids) must remain a DictionaryVector";
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_VARCHAR);
}

// Plan A' lifetime safety (the critical zero-copy invariant): a StringView batch must stay valid
// after the reader — and its per-stripe omniDictSV_ dictionary template — is destroyed. The output
// vector shares omniDictSV_'s string buffer via shared_ptr, so arena-backed (>12B) values must not
// dangle once that template/reader is gone. (Destroying the whole reader is strictly stronger than
// the per-stripe reader.reset() that happens on stripe boundaries, so this covers cross-stripe too.)
TEST_F(StringViewDictReadTest, StringViewBatchOutlivesReader)
{
    int32_t numRows = 100;
    std::vector<std::string> dict = {"apple", "a_very_long_dictionary_value_xyz", "cherry",
                                     "another_long_one_over_twelve", "fig"};   // includes >12B arena entries
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }
    WriteDictStringOrcFile(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    std::vector<BaseVector*> batch;
    {
        orc::ReaderOptions readerOpts;
        std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
                readFileOverride(UriInfo("file", filename, "", "-1")), readerOpts);
        orc::RowReaderOptions rowOpts;
        std::unique_ptr<common::JulianGregorianRebase> julian;
        std::unique_ptr<common::PredicateCondition> pred;
        auto readerImpl = dynamic_cast<OmniReaderImpl*>(reader.get());
        ASSERT_NE(readerImpl, nullptr);
        auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
        auto omniRowReader = dynamic_cast<OmniRowReaderImpl*>(rowReader.get());
        ASSERT_NE(omniRowReader, nullptr);
        int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
        omniRowReader->next(&batch, &svTypeId, numRows);
        // reader + rowReader (and the OmniStringDictionaryColumnReader holding omniDictSV_) are
        // destroyed at scope exit; `batch` must survive with its values intact.
    }

    ASSERT_EQ(batch.size(), 1);
    auto *svVec = dynamic_cast<OmniStringViewVector *>(batch[0]);
    ASSERT_NE(svVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "value dangled after reader destroyed, row " << i;
    }
    for (auto v : batch) delete v;
}

// Multi-column mixed schema, per-column omniTypeId dispatch: INT stays INT, one STRING column is
// requested as StringView (flat SV), another STRING column as VARCHAR (dictionary vector) — all in
// the SAME batch. Proves StringView is per-column (not all-or-nothing) and non-string columns are
// untouched. c1 includes a >12B (arena) value.
TEST_F(StringViewDictReadTest, ReadMultiColMixedStringViewAndVarchar)
{
    int32_t numRows = 80;
    std::vector<std::string> dict1 = {"apple", "a_very_long_dictionary_value_xyz", "cherry", "fig"};  // -> SV
    std::vector<std::string> dict2 = {"red", "green_longer_than_twelve_bytes", "blue", "yellow"};      // -> VARCHAR
    std::vector<int32_t> intData(numRows);
    std::vector<std::string> s1(numRows), s2(numRows);
    for (int i = 0; i < numRows; ++i) {
        intData[i] = i * 10;
        s1[i] = dict1[i % dict1.size()];
        s2[i] = dict2[i % dict2.size()];
    }

    // Write INT + STRING + STRING (dictionary-encoded).
    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::INT));
    schema->addStructField("c1", orc::createPrimitiveType(orc::TypeKind::STRING));
    schema->addStructField("c2", orc::createPrimitiveType(orc::TypeKind::STRING));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    options.setDictionaryKeySizeThreshold(1.0);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto intVec = std::make_unique<Vector<int32_t>>(numRows);
    auto strVec1 = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numRows);
    auto strVec2 = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        intVec->SetValue(i, intData[i]);
        intVec->SetNotNull(i);
        strVec1->SetValue(i, std::string_view(s1[i]));
        strVec1->SetNotNull(i);
        strVec2->SetValue(i, std::string_view(s2[i]));
        strVec2->SetNotNull(i);
    }
    std::vector<BaseVector *> cols{intVec.get(), strVec1.get(), strVec2.get()};
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }
    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    // Per-column request: INT -> INT, c1 -> StringView, c2 -> VARCHAR.
    int omniTypeIds[3] = {omniruntime::type::OMNI_INT,
                          omniruntime::type::OMNI_STRING_VIEW,
                          omniruntime::type::OMNI_VARCHAR};
    ScanFile(numRows, omniTypeIds);
    ASSERT_EQ(readBatch.size(), 3);

    // c0: INT stays a flat int vector, untouched by StringView.
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_INT);
    auto *iv = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    ASSERT_NE(iv, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(iv->GetValue(i), intData[i]) << "INT row " << i;
    }

    // c1: requested StringView -> flat Vector<StringView>.
    ASSERT_EQ(readBatch[1]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    ASSERT_EQ(readBatch[1]->GetEncoding(), OMNI_FLAT);
    auto *sv = dynamic_cast<OmniStringViewVector *>(readBatch[1]);
    ASSERT_NE(sv, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view a = sv->GetValueRef(i);
        ASSERT_EQ(a, s1[i]) << "SV row " << i;
    }

    // c2: requested VARCHAR -> DictionaryVector (unchanged dict fast path), in the same batch.
    ASSERT_EQ(readBatch[2]->GetTypeId(), omniruntime::type::OMNI_VARCHAR);
    ASSERT_EQ(readBatch[2]->GetEncoding(), OMNI_DICTIONARY);
    auto *vc = dynamic_cast<Vector<DictionaryContainer<std::string_view>> *>(readBatch[2]);
    ASSERT_NE(vc, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(vc->GetValue(i), s2[i]) << "VARCHAR row " << i;
    }
}

// True multi-stripe, batched, dictionary-encoded read: a tiny stripe size + chunked writes force
// many stripes (each with its own dictionary). We read in small batches crossing stripe boundaries
// and HOLD every batch to the end, so batches from early stripes must survive the per-stripe
// reader.reset() + omniDictSV_ rebuilds that happen as later stripes are read.
TEST_F(StringViewDictReadTest, ReadMultiStripeBatchedStringView)
{
    // Enough distinct, moderately-long values + many rows so that even dictionary-encoded the data
    // spills across MANY 4KB stripes (a few repeating short values would dict-compress into a single
    // stripe and defeat the test). Scattered access weakens RLE so the index stream stays large.
    int32_t numRows = 30000;
    std::vector<std::string> dictVals(100);
    for (int k = 0; k < 100; ++k) {
        dictVals[k] = "multistripe_dictionary_value_" + std::to_string(k);   // ~31B each
    }
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dictVals[(i * 31 + 7) % 100];
    }

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::STRING));
    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(4096);   // tiny -> many stripes
    options.setTimezoneName("GMT");
    options.setDictionaryKeySizeThreshold(1.0);   // dictionary-encoded (exercises per-stripe dict rebuild)
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);
    auto valVec = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, std::string_view(data[i]));
        valVec->SetNotNull(i);
    }
    std::vector<BaseVector *> cols{valVec.get()};
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }
    // OmniWriter only checks the stripe-size threshold once per add() call, so a single
    // add(0, numRows) buffers everything into ONE stripe. Add in small chunks so the check fires
    // repeatedly and actually cuts multiple 4KB stripes. NB: add() takes (startPos, endPos), a
    // half-open range — NOT (startPos, count).
    int32_t addChunk = 256;
    for (int32_t off = 0; off < numRows; off += addChunk) {
        int32_t end = (off + addChunk < numRows) ? (off + addChunk) : numRows;
        writer->add(rowVec.get(), static_cast<uint64_t>(off), static_cast<uint64_t>(end));
    }
    writer->close();
    writer.reset();
    outStream.reset();

    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
            readFileOverride(UriInfo("file", filename, "", "-1")), readerOpts);
    ASSERT_GT(reader->getNumberOfStripes(), 1u) << "test requires multiple stripes";
    orc::RowReaderOptions rowOpts;
    std::unique_ptr<common::JulianGregorianRebase> julian;
    std::unique_ptr<common::PredicateCondition> pred;
    auto readerImpl = dynamic_cast<OmniReaderImpl*>(reader.get());
    ASSERT_NE(readerImpl, nullptr);
    auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
    auto omniRowReader = dynamic_cast<OmniRowReaderImpl*>(rowReader.get());
    ASSERT_NE(omniRowReader, nullptr);

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    std::vector<std::pair<BaseVector*, uint64_t>> held;
    uint64_t total = 0;
    while (true) {
        auto batch = std::make_unique<std::vector<BaseVector*>>();
        uint64_t got = omniRowReader->next(batch.get(), &svTypeId, 64);
        if (got == 0) {
            break;
        }
        ASSERT_EQ(batch->size(), 1u);
        ASSERT_EQ((*batch)[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
        held.emplace_back((*batch)[0], got);
        total += got;
    }
    ASSERT_EQ(total, static_cast<uint64_t>(numRows));

    // Validate all values in read order, AFTER every stripe was read — so batches from early stripes
    // are checked post per-stripe reader.reset() (their StringView payloads must not dangle).
    int32_t row = 0;
    for (auto &entry : held) {
        auto *sv = dynamic_cast<OmniStringViewVector *>(entry.first);
        ASSERT_NE(sv, nullptr);
        for (uint64_t j = 0; j < entry.second; ++j) {
            std::string_view actual = sv->GetValueRef(static_cast<int32_t>(j));
            ASSERT_EQ(actual, data[row]) << "row " << row;
            ++row;
        }
    }
    ASSERT_EQ(row, numRows);
    for (auto &entry : held) {
        delete entry.first;
    }
}

// Non-ASCII / UTF-8 multibyte on the dictionary path: byte-exact round-trip through the flat
// StringView materialization, mixing inline (<=12B) and arena (>12B) multibyte entries.
TEST_F(StringViewDictReadTest, ReadUtf8MultibyteDictStringView)
{
    int32_t numRows = 60;
    std::vector<std::string> dict = {
        "café",                                  // inline
        "中文字符串测试内容超过十二字节",              // arena multibyte
        "😀🎉",                                   // inline emoji
        "Grüße_aus_München_länger"               // arena accented
    };
    std::vector<std::string> data(numRows);
    for (int i = 0; i < numRows; ++i) {
        data[i] = dict[i % dict.size()];
    }

    WriteDictStringOrcFile(data, {}, orc::createPrimitiveType(orc::TypeKind::STRING));

    int svTypeId = omniruntime::type::OMNI_STRING_VIEW;
    ScanFile(numRows, &svTypeId);
    ASSERT_EQ(readBatch.size(), 1);
    ASSERT_EQ(readBatch[0]->GetTypeId(), omniruntime::type::OMNI_STRING_VIEW);
    ASSERT_EQ(readBatch[0]->GetEncoding(), OMNI_FLAT);

    auto *svVec = dynamic_cast<OmniStringViewVector *>(readBatch[0]);
    ASSERT_NE(svVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        std::string_view actual = svVec->GetValueRef(i);
        ASSERT_EQ(actual, data[i]) << "UTF-8 dict byte-exact mismatch at row " << i;
    }
}
