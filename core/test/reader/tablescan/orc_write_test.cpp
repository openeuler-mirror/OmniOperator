#include "reader/orc/OmniWriter.hh"
#include "reader/orc/OmniRowReaderImpl.hh"
#include "reader/orc/OrcFileOverride.hh"
#include <vector/vector_common.h>
#include "orc/OrcFile.hh"
#include "scan_test.h"
#include <cstdio>
#include <memory>
#include <orc/Type.hh>
#include <gtest/gtest.h>
#include <fstream>


using namespace omniruntime::vec;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

using OmniStringVector = Vector<LargeStringContainer<std::string_view>>;

class WriteTest : public testing::Test {
protected:
    std::string filename;

    virtual void SetUp() override {
        setenv("TZ", "Asia/Shanghai", 1);
        tzset();
        filename = "/tmp/omni_test/write_test_" + std::to_string(std::time(nullptr)) + ".orc";
    }

    virtual void TearDown() override {
        remove(filename.c_str());
    }

    void ScanFile(int numRows, int typeId, std::vector<BaseVector*>& outBatch) {
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

        omniRowReader->next(&outBatch, &typeId, numRows);
    }
};

TEST_F(WriteTest, WriteByteFlat)
{
    std::vector<int8_t> expectedData = {
         0,
         1,
         -1,
         static_cast<int8_t>(127),
         static_cast<int8_t>(-128),
         std::numeric_limits<int8_t>::max(),
         std::numeric_limits<int8_t>::min()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::BYTE));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int8_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_BYTE;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int8_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

// Verifies that the parallel path produces a valid multi-stripe ORC file whose
// top-level columns can be read back with their values and ordering intact.
TEST_F(WriteTest, ParallelSerializeTwoIntColumnsReadback)
{
    const int numRows = 8000;

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::INT));
    schema->addStructField("c1", orc::createPrimitiveType(orc::TypeKind::INT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(8192);
    options.setTimezoneName("GMT");
    // Exercise several indexed row groups while top-level columns flush in parallel.
    options.setRowIndexStride(1000);

    OmniWriterRuntimeOptions rt;
    rt.parallelSerializeEnabled = true;
    rt.parallelSerializeMaxThreads = 4;

    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options, rt);

    auto v0 = std::make_unique<Vector<int32_t>>(numRows);
    auto v1 = std::make_unique<Vector<int32_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        v0->SetValue(i, i);
        v0->SetNotNull(i);
        v1->SetValue(i, i * 2 + 7);
        v1->SetNotNull(i);
    }
    std::vector<BaseVector *> cols;
    cols.push_back(v0.get());
    cols.push_back(v1.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
            readFileOverride(UriInfo("file", filename, "", "-1")), readerOpts);
    orc::RowReaderOptions rowOpts;
    std::unique_ptr<common::JulianGregorianRebase> julian;
    std::unique_ptr<common::PredicateCondition> pred;
    auto readerImpl = dynamic_cast<OmniReaderImpl *>(reader.get());
    ASSERT_NE(readerImpl, nullptr) << "Failed to create OmniReaderImpl";
    auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
    auto omniRowReader = dynamic_cast<OmniRowReaderImpl *>(rowReader.get());
    ASSERT_NE(omniRowReader, nullptr) << "Failed to create OmniRowReaderImpl";
    omniRowReader->next(&readBatch, nullptr, numRows);
    ASSERT_EQ(readBatch.size(), 2u);
    auto r0 = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    auto r1 = dynamic_cast<Vector<int32_t> *>(readBatch[1]);
    ASSERT_NE(r0, nullptr);
    ASSERT_NE(r1, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(r0->GetValue(i), i);
        ASSERT_EQ(r1->GetValue(i), i * 2 + 7);
    }
    for (auto v : readBatch) {
        delete v;
    }
}

// Covers dictionary-encoded string streams in the parallel path. String
// dictionary bytes are prepared before flush(), but their stream entries are
// emitted during flush(); the ordered merge must therefore keep physical bytes
// aligned with the final stripe footer order.
TEST_F(WriteTest, ParallelSerializeStringDictionaryReadback)
{
    const int numRows = 20000;
    const int distinctValues = 32;

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("payload", orc::createPrimitiveType(orc::TypeKind::STRING));
    schema->addStructField("bucket", orc::createPrimitiveType(orc::TypeKind::INT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(8192);
    options.setTimezoneName("GMT");
    options.setRowIndexStride(1000);

    OmniWriterRuntimeOptions rt;
    rt.parallelSerializeEnabled = true;
    rt.parallelSerializeMaxThreads = 4;

    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options, rt);

    std::vector<std::string> expected(numRows);
    auto strVec = std::make_unique<OmniStringVector>(numRows);
    auto intVec = std::make_unique<Vector<int32_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        expected[i] = "dict_value_" + std::to_string(i % distinctValues);
        strVec->SetValue(i, std::string_view(expected[i]));
        strVec->SetNotNull(i);
        intVec->SetValue(i, i % 997);
        intVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(strVec.get());
    cols.push_back(intVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
            readFileOverride(UriInfo("file", filename, "", "-1")), readerOpts);
    orc::RowReaderOptions rowOpts;
    std::unique_ptr<common::JulianGregorianRebase> julian;
    std::unique_ptr<common::PredicateCondition> pred;
    auto readerImpl = dynamic_cast<OmniReaderImpl *>(reader.get());
    ASSERT_NE(readerImpl, nullptr) << "Failed to create OmniReaderImpl";
    auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
    auto omniRowReader = dynamic_cast<OmniRowReaderImpl *>(rowReader.get());
    ASSERT_NE(omniRowReader, nullptr) << "Failed to create OmniRowReaderImpl";
    omniRowReader->next(&readBatch, nullptr, numRows);
    ASSERT_EQ(readBatch.size(), 2u);

    auto strRes = dynamic_cast<OmniStringVector *>(readBatch[0]);
    auto intRes = dynamic_cast<Vector<int32_t> *>(readBatch[1]);
    ASSERT_NE(strRes, nullptr);
    ASSERT_NE(intRes, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(strRes->GetValue(i), expected[i]) << " row " << i;
        ASSERT_EQ(intRes->GetValue(i), i % 997) << " row " << i;
    }
    for (auto v : readBatch) {
        delete v;
    }
}

// Writes identical input through parallel and legacy serial paths, then compares
// decoded values to guard against stream-order and merge-offset regressions.
TEST_F(WriteTest, ParallelSerializeVsSerialTwoIntColumnsSameData)
{
    const int numRows = 4000;

    // Keep schema, stripe settings and data identical; only the runtime parallel
    // options differ between the two generated files.
    auto writeOnce = [&](bool parallel, const std::string &path) {
        UriInfo uri("file", path, "", "-1");
        std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
        std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
        schema->addStructField("a", orc::createPrimitiveType(orc::TypeKind::INT));
        schema->addStructField("b", orc::createPrimitiveType(orc::TypeKind::INT));

        orc::WriterOptions options;
        options.setMemoryPool(orc::getDefaultPool());
        options.setStripeSize(16384);
        options.setTimezoneName("GMT");
        options.setRowIndexStride(0);

        OmniWriterRuntimeOptions rt;
        rt.parallelSerializeEnabled = parallel;
        rt.parallelSerializeMaxThreads = parallel ? 4U : 1U;

        std::unique_ptr<OmniWriter> w = createOmniWriter(*schema, outStream.get(), options, rt);
        auto v0 = std::make_unique<Vector<int32_t>>(numRows);
        auto v1 = std::make_unique<Vector<int32_t>>(numRows);
        for (int i = 0; i < numRows; ++i) {
            v0->SetValue(i, i % 997);
            v0->SetNotNull(i);
            v1->SetValue(i, i / 3);
            v1->SetNotNull(i);
        }
        std::vector<BaseVector *> cols;
        cols.push_back(v0.get());
        cols.push_back(v1.get());
        auto rowVec = std::make_unique<RowVector>(numRows, cols);
        for (int i = 0; i < numRows; ++i) {
            rowVec->SetNotNull(i);
        }
        w->add(rowVec.get(), 0, numRows);
        w->close();
        w.reset();
        outStream.reset();
    };

    std::string fParallel = filename + ".p";
    std::string fSerial = filename + ".s";
    writeOnce(true, fParallel);
    writeOnce(false, fSerial);

    auto readPath = [&](const std::string &path) -> std::vector<BaseVector *> {
        orc::ReaderOptions readerOpts;
        std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(
                readFileOverride(UriInfo("file", path, "", "-1")), readerOpts);
        orc::RowReaderOptions rowOpts;
        std::unique_ptr<common::JulianGregorianRebase> julian;
        std::unique_ptr<common::PredicateCondition> pred;
        auto readerImpl = dynamic_cast<OmniReaderImpl *>(reader.get());
        if (readerImpl == nullptr) {
            ADD_FAILURE() << "OmniReaderImpl is null";
            return {};
        }
        auto rowReader = readerImpl->createRowReader(rowOpts, julian, pred);
        auto omniRowReader = dynamic_cast<OmniRowReaderImpl *>(rowReader.get());
        if (omniRowReader == nullptr) {
            ADD_FAILURE() << "OmniRowReaderImpl is null";
            return {};
        }
        std::vector<BaseVector *> batch;
        omniRowReader->next(&batch, nullptr, numRows);
        return batch;
    };

    auto bp = readPath(fParallel);
    auto bs = readPath(fSerial);
    ASSERT_EQ(bp.size(), 2u);
    ASSERT_EQ(bs.size(), 2u);
    auto p0 = dynamic_cast<Vector<int32_t> *>(bp[0]);
    auto p1 = dynamic_cast<Vector<int32_t> *>(bp[1]);
    auto s0 = dynamic_cast<Vector<int32_t> *>(bs[0]);
    auto s1 = dynamic_cast<Vector<int32_t> *>(bs[1]);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(s0, nullptr);
    ASSERT_NE(s1, nullptr);
    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(p0->GetValue(i), s0->GetValue(i)) << " row " << i;
        ASSERT_EQ(p1->GetValue(i), s1->GetValue(i)) << " row " << i;
    }
    for (auto v : bp) {
        delete v;
    }
    for (auto v : bs) {
        delete v;
    }
    remove(fParallel.c_str());
    remove(fSerial.c_str());
}

TEST_F(WriteTest, WriteShortFlat)
{
    std::vector<int16_t> expectedData = {
            0,
            1,
            -1,
            128,
            -129,
            std::numeric_limits<int16_t>::max(),
            std::numeric_limits<int16_t>::min()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::SHORT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int16_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_SHORT;
    ScanFile(numRows, typeId, readBatch);

    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int16_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteIntFlat)
{
    std::vector<int32_t> expectedData = {
            0,
            1,
            -1,
            32768,
            -32769,
            std::numeric_limits<int32_t>::max(),
            std::numeric_limits<int32_t>::min()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::INT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int32_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_INT;
    ScanFile(numRows, typeId, readBatch);

    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteIntDict)
{

    int32_t dicSize = 10;
    int32_t numRows = 100;

    auto *indices = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; ++i) {
        indices[i] = i % dicSize;
    }

    auto distinctVec = std::make_shared<Vector<int32_t>>(dicSize);
    for (int32_t j = 0; j < dicSize; ++j) {
        distinctVec->SetValue(j, j + 2);
        distinctVec->SetNotNull(j);
    }

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::INT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");

    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    BaseVector* dictVecRaw = VectorHelper::CreateDictionary(indices, numRows, distinctVec.get());

    std::vector<BaseVector *> cols;
    cols.push_back(dictVecRaw);
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_INT;
    ScanFile(numRows, typeId, readBatch);

    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        // index = i % 10
        // key = index + 2
        int32_t expected = (i % dicSize) + 2;
        int32_t actual = resVec->GetValue(i);

        ASSERT_EQ(actual, expected) << "Mismatch at row " << i;
    }

    for (auto v : readBatch) delete v;
    delete dictVecRaw;
    delete[] indices;
}

TEST_F(WriteTest, WriteIntWithNulls)
{
    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::INT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    int numRows = 10;
    auto valVec = std::make_unique<Vector<int32_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        if (i % 2 != 0) {
            valVec->SetNull(i);
        } else {
            valVec->SetValue(i, i * 100);
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
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    ScanFile(numRows, omniruntime::type::OMNI_INT, readBatch);

    ASSERT_EQ(readBatch.size(), 1);
    auto resVec = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        if (i % 2 != 0) {
            ASSERT_TRUE(resVec->IsNull(i)) << "Row " << i << " should be NULL";
        } else {
            ASSERT_FALSE(resVec->IsNull(i)) << "Row " << i << " should NOT be NULL";
            ASSERT_EQ(resVec->GetValue(i), i * 100) << "Mismatch at row " << i;
        }
    }
    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteLongFlat)
{
    std::vector<int64_t> expectedData = {
            0,
            1,
            -1,
            2147483648L,
            -2147483649L,
            std::numeric_limits<int64_t>::max(),
            std::numeric_limits<int64_t>::min()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::LONG));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int64_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_LONG;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int64_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteBoolFlat)
{
    std::vector<bool> expectedData = {
            true,
            false,
            true,
            false,
            true,
            false,
            true
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::BOOLEAN));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<bool>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_BOOLEAN;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<bool> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteDoubleFlat)
{
    std::vector<double> expectedData = {
            0.0,
            1.123456789,
            -1.123456789,
            3.1415926535,
            -3.1415926535,
            std::numeric_limits<double>::max(),
            std::numeric_limits<double>::min(),
            std::numeric_limits<double>::lowest(),
            std::numeric_limits<double>::epsilon()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::DOUBLE));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<double>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_DOUBLE;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<double> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteFloatFlat)
{
    std::vector<float> expectedData = {
            0.0f,
            1.123f,
            -1.123f,
            3.1415f,
            -3.1415f,
            std::numeric_limits<float>::max(),
            std::numeric_limits<float>::min(),
            std::numeric_limits<float>::lowest(),
            std::numeric_limits<float>::epsilon()
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::FLOAT));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<float>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_FLOAT;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<float> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteDate32Flat)
{
    std::vector<int32_t> expectedData = {
            0,              //1970-01-01
            1,              //1970-01-02
            -1,             //1969-12-31
            19359,          //2023-01-02
            -25567,         //1900-01-01
            2932896         //9999-12-31
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::DATE));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int32_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_DATE32;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int32_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteTimestampFlat)
{
    std::vector<int64_t> expectedData = {
            946684800000,    // 2000-01-01
            1672531200000,   // 2023-01-01
            1704067200000,   // 2024-01-01
            1735689600000    // 2025-01-01
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::TIMESTAMP));
    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int64_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }
    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }
    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_TIMESTAMP;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int64_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    int64_t baseOffset = resVec->GetValue(0) - expectedData[0];
    for (int i = 0; i < numRows; ++i) {
        int64_t actual = resVec->GetValue(i);
        int64_t expected = expectedData[i] + baseOffset;
        ASSERT_EQ(actual, expected) << "Mismatch at row " << i;
    }

    for (auto v : readBatch) delete v;
}

using OmniStringVector = Vector<LargeStringContainer<std::string_view>>;

TEST_F(WriteTest, WriteStringFlat)
{
    std::vector<std::string> expectedData = {
            "",
            "simple",
            "Hello World",
            "No Chinese Characters",
            "!@#$%^&*()_+",
            std::string(100, 'a')
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::STRING));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<OmniStringVector>(numRows);
    for (int i = 0; i < numRows; ++i) {
        std::string_view sv(expectedData[i]);
        valVec->SetValue(i, sv);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_VARCHAR;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteVarcharFlat)
{
    std::vector<std::string> expectedData = {
            "",
            "varchar_value",
            "1234567890",
            "Variable length text",
            std::string(50, 'x')
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createCharType(orc::TypeKind::VARCHAR, 65535));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<OmniStringVector>(numRows);
    for (int i = 0; i < numRows; ++i) {
        std::string_view sv(expectedData[i]);
        valVec->SetValue(i, sv);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_VARCHAR;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteVarcharWithNulls)
{
    std::vector<std::string> expectedData = {
            "",  //空值
            "varchar_value",
            "",  //空行
            "1234567890",
            "Variable length text",
            std::string(50, 'x')
    };
    std::vector<bool> isNulls = {
            false,
            false,
            true,
            false,
            false,
            false
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createCharType(orc::TypeKind::VARCHAR, 65535));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<OmniStringVector>(numRows);
    for (int i = 0; i < numRows; ++i) {
        if (!isNulls[i]) {
            std::string_view sv(expectedData[i]);
            valVec->SetValue(i, sv);
            valVec->SetNotNull(i);
        } else {
            valVec->SetNull(i);
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
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_VARCHAR;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        if (isNulls[i]) {
            ASSERT_TRUE(resVec->IsNull(i));
        } else {
            ASSERT_FALSE(resVec->IsNull(i));
            ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
        }
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteVarcharDict)
{
    int32_t dicSize = 10;
    int32_t numRows = 100;

    auto *indices = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; ++i) {
        indices[i] = i % dicSize;
    }

    auto distinctVec = std::make_shared<OmniStringVector>(dicSize);
    for (int32_t j = 0; j < dicSize; ++j) {
        std::string val = "dict_val_" + std::to_string(j);
        std::string_view sv(val);
        distinctVec->SetValue(j, sv);
        distinctVec->SetNotNull(j);
    }

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::STRING));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");

    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);
    BaseVector* dictVecRaw = VectorHelper::CreateStringDictionary(indices, numRows, distinctVec.get());

    std::vector<BaseVector *> cols;
    cols.push_back(dictVecRaw);
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_VARCHAR;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);
    for (int i = 0; i < numRows; ++i) {
        // index = i % 10
        // key = "dict_val_" + index
        std::string expected = "dict_val_" + std::to_string(i % dicSize);
        ASSERT_EQ(resVec->GetValue(i), expected) << "Mismatch at row " << i;
    }

    for (auto v : readBatch) delete v;
    delete dictVecRaw;
    delete[] indices;
}

TEST_F(WriteTest, WriteCharFlat)
{
    std::vector<std::string> expectedData = {
            "a",
            "b",
            "char_test",
            "123",
            ""
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createCharType(orc::TypeKind::CHAR, 20));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<OmniStringVector>(numRows);
    for (int i = 0; i < numRows; ++i) {
        std::string_view sv(expectedData[i]);
        valVec->SetValue(i, sv);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_CHAR;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i]);
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteBinaryFlat)
{
    std::vector<std::string> expectedData = {
            "",
            "pure_text",
            std::string("null\0byte", 9),
            std::string("\x00\x01\x02\xFF", 4),
            std::string(10, '\0')
    };
    int numRows = expectedData.size();

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createPrimitiveType(orc::TypeKind::BINARY));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<OmniStringVector>(numRows);
    for (int i = 0; i < numRows; ++i) {
        std::string_view sv(expectedData[i]);
        valVec->SetValue(i, sv);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_VARBINARY;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<OmniStringVector *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), expectedData[i])
            << "Mismatch at row " << i << " (Check length and binary content)";
    }

    for (auto v : readBatch) delete v;
}

TEST_F(WriteTest, WriteDecimal64Flat)
{
    int32_t precision = 18;
    int32_t scale = 4;
    int numRows = 5;

    std::vector<int64_t> unscaledData = {
            12345L,             // 1.2345
            -987654321L,        // -98765.4321
            0L,                 // 0.0000
            1000L,              // 0.1000
            999999999999999999L // Max precision test
    };

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createDecimalType(precision, scale));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<int64_t>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, unscaledData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_DECIMAL64;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<int64_t> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resVec->GetValue(i), unscaledData[i]) << "Mismatch at row " << i;
    }

    for (auto v : readBatch) delete v;
}

using OmniDecimal128 = omniruntime::type::Decimal128;
TEST_F(WriteTest, WriteDecimal128Flat)
{
    int32_t precision = 38;
    int32_t scale = 10;
    int numRows = 4;

    std::vector<OmniDecimal128> expectedData;
    // "12345678901234567890.1234567890" -> Unscaled: 123456789012345678901234567890
    expectedData.push_back(OmniDecimal128("123456789012345678901234567890"));
    expectedData.push_back(OmniDecimal128("-98765432109876543210.9876543210"));
    expectedData.push_back(OmniDecimal128("0"));
    expectedData.push_back(OmniDecimal128("99999999999999999999999999999999999999"));

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createDecimalType(precision, scale));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto valVec = std::make_unique<Vector<OmniDecimal128>>(numRows);
    for (int i = 0; i < numRows; ++i) {
        valVec->SetValue(i, expectedData[i]);
        valVec->SetNotNull(i);
    }

    std::vector<BaseVector *> cols;
    cols.push_back(valVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_DECIMAL128;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resVec = dynamic_cast<Vector<OmniDecimal128> *>(readBatch[0]);
    ASSERT_NE(resVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        OmniDecimal128 actual = resVec->GetValue(i);
        OmniDecimal128 expected = expectedData[i];
        if (actual != expected) {
            FAIL() << "Mismatch at row " << i << "\n"
                   << "Expected: " << expected.ToString() << "\n"
                   << "Actual  : " << actual.ToString();
        }
    }

    for (auto v : readBatch) delete v;
}


TEST_F(WriteTest, WriteArrayFlat)
{
    int numRows = 5;
    int elementsPerRow = 2;
    int totalElements = numRows * elementsPerRow;

    std::vector<std::string> flatExpectedData;
    for (int i = 0; i < numRows; ++i) {
        flatExpectedData.push_back("row" + std::to_string(i) + "_e1");
        flatExpectedData.push_back("row" + std::to_string(i) + "_e2");
    }

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);

    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);
    schema->addStructField("c0", orc::createListType(orc::createPrimitiveType(orc::TypeKind::STRING)));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto arrayVec = std::make_unique<omniruntime::vec::ArrayVector>(numRows);
    auto childVec = std::make_shared<OmniStringVector>(totalElements);

    for (int i = 0; i < totalElements; ++i) {
        std::string_view sv(flatExpectedData[i]);
        childVec->SetValue(i, sv);
        childVec->SetNotNull(i);
    }

    for (int i = 0; i < numRows; ++i) {
        arrayVec->SetSize(i, elementsPerRow);
        arrayVec->SetNotNull(i);
    }

    arrayVec->SetElementVector(childVec);
    std::vector<BaseVector *> cols;
    cols.push_back(arrayVec.get());

    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_ARRAY;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resArrayVec = dynamic_cast<omniruntime::vec::ArrayVector *>(readBatch[0]);
    ASSERT_NE(resArrayVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resArrayVec->GetSize(i), elementsPerRow) << "Row " << i << " size mismatch";
    }

    auto elementVector = resArrayVec->GetElementVector();
    ASSERT_NE(elementVector, nullptr);

    auto resChildVec = std::dynamic_pointer_cast<OmniStringVector>(elementVector);
    ASSERT_NE(resChildVec, nullptr);

    for (int i = 0; i < totalElements; ++i) {
        ASSERT_EQ(resChildVec->GetValue(i), flatExpectedData[i])
                                    << "Mismatch at flat index " << i;
    }

    for (auto v : readBatch) delete v;
}

using OmniStringVector = Vector<LargeStringContainer<std::string_view>>;

TEST_F(WriteTest, WriteMapFlat)
{
    int numRows = 5;
    int entriesPerRow = 2;
    int totalEntries = numRows * entriesPerRow;

    std::vector<double> flatKeys;
    std::vector<std::string> flatValues;

    for (int i = 0; i < numRows; ++i) {
        flatKeys.push_back(i + 0.1);
        flatKeys.push_back(i + 0.2);
        flatValues.push_back("val_" + std::to_string(i) + "_1");
        flatValues.push_back("val_" + std::to_string(i) + "_2");
    }

    UriInfo uri("file", filename, "", "-1");
    std::unique_ptr<orc::OutputStream> outStream = writeFileOverride(uri);
    std::unique_ptr<orc::Type> schema = orc::createPrimitiveType(orc::TypeKind::STRUCT);

    schema->addStructField("c0", orc::createMapType(
            orc::createPrimitiveType(orc::TypeKind::DOUBLE),
            orc::createPrimitiveType(orc::TypeKind::STRING)
    ));

    orc::WriterOptions options;
    options.setMemoryPool(orc::getDefaultPool());
    options.setStripeSize(67108864);
    options.setTimezoneName("GMT");
    std::unique_ptr<OmniWriter> writer = createOmniWriter(*schema, outStream.get(), options);

    auto mapVec = std::make_unique<omniruntime::vec::MapVector>(numRows);
    auto keyVec = std::make_shared<Vector<double>>(totalEntries);
    auto valVec = std::make_shared<OmniStringVector>(totalEntries);

    for (int i = 0; i < totalEntries; ++i) {
        keyVec->SetValue(i, flatKeys[i]);
        keyVec->SetNotNull(i);

        std::string_view sv(flatValues[i]);
        valVec->SetValue(i, sv);
        valVec->SetNotNull(i);
    }

    mapVec->GetOffsets()[0] = 0;
    for (int i = 0; i < numRows; ++i) {
        mapVec->SetSize(i, entriesPerRow);
        mapVec->SetNotNull(i);
    }

    mapVec->SetKeyVector(keyVec);
    mapVec->SetValueVector(valVec);

    std::vector<BaseVector *> cols;
    cols.push_back(mapVec.get());
    auto rowVec = std::make_unique<RowVector>(numRows, cols);
    for (int i = 0; i < numRows; ++i) {
        rowVec->SetNotNull(i);
    }

    writer->add(rowVec.get(), 0, numRows);
    writer->close();
    writer.reset();
    outStream.reset();

    std::vector<BaseVector *> readBatch;
    int typeId = omniruntime::type::OMNI_MAP;
    ScanFile(numRows, typeId, readBatch);
    ASSERT_EQ(readBatch.size(), 1);

    auto resMapVec = dynamic_cast<omniruntime::vec::MapVector *>(readBatch[0]);
    ASSERT_NE(resMapVec, nullptr);

    auto resKeyBase = resMapVec->GetKeyVector();
    auto resValBase = resMapVec->GetValueVector();
    ASSERT_NE(resKeyBase, nullptr);
    ASSERT_NE(resValBase, nullptr);

    auto resKeyVec = std::dynamic_pointer_cast<Vector<double>>(resKeyBase);
    auto resValVec = std::dynamic_pointer_cast<OmniStringVector>(resValBase);
    ASSERT_NE(resKeyVec, nullptr);
    ASSERT_NE(resValVec, nullptr);

    for (int i = 0; i < numRows; ++i) {
        ASSERT_EQ(resMapVec->GetSize(i), entriesPerRow);
    }

    for (int i = 0; i < totalEntries; ++i) {
        ASSERT_DOUBLE_EQ(resKeyVec->GetValue(i), flatKeys[i]);
        ASSERT_EQ(resValVec->GetValue(i), flatValues[i]);
    }

    for (auto v : readBatch) delete v;
}