//
// Created by root on 5/15/24.
//
#include "vector/omni_row.h"
#include "gtest/gtest.h"
#include "test/util/test_util.h"
#include "operator/hash_util.h"
#include <iostream>

namespace omniruntime::vec::test {
    using namespace omniruntime::vec;
    using namespace omniruntime::TestUtil;
    TEST(omni_row, compact_value_test) {
        Vector<int16_t> shortVec(1);
        shortVec.SetValue(0,10);
        SerializedValue<int16_t> value;
        value.TransValue(&shortVec,0);
        EXPECT_EQ(value.CompactLength(), 1 + 1);
    }

    TEST(omni_row, compact_set_value) {
        int32_t value = (int32_t )(INT16_MAX) + 1;
        SerializedValue<int32_t> serializedValue;
        serializedValue.SetValue(value);
        EXPECT_EQ(serializedValue.CompactLength(), 1+3);
    }

    TEST(omni_row, compact_set_null) {
        SerializedValue<int32_t> serializedValue;
        serializedValue.SetNull();
        EXPECT_EQ(serializedValue.CompactLength(), 1);
    }

    TEST(omni_row, compact_set_string_null) {
        SerializedValue<std::string_view> serializedValue;
        serializedValue.SetNull();
        EXPECT_EQ(serializedValue.CompactLength(), 1);
    }

    TEST(omni_row, compact_set_string) {
        SerializedValue<std::string_view> serializedValue;
        std::string_view testStr("test", 4);
        serializedValue.SetValue(testStr);
        EXPECT_EQ(serializedValue.CompactLength(), 1 + 1 + 4);
    }

    TEST(omni_row, fill_buffer_no_null) {
        std::vector<DataTypePtr> types({IntDataType::Instance(),DoubleDataType::Instance(),VarcharDataType::Instance()});
        RowBuffer rowBuffer(types);
        std::string_view testStr("hello world", 11);
        auto *buffer = rowBuffer.GetOneOfRow(0);
        auto *intValue = reinterpret_cast<SerializedValue<int32_t>*>(buffer);
        intValue->SetValue(129);
        buffer = rowBuffer.GetOneOfRow(1);
        auto *doubleValue = reinterpret_cast<SerializedValue<double>*>(buffer);
        double ori = 3.1415926;
        doubleValue->SetValue(ori);
        buffer = rowBuffer.GetOneOfRow(2);
        auto *strValue = reinterpret_cast<SerializedValue<std::string_view>*>(buffer);
        strValue->SetValue(testStr);
        rowBuffer.FillBuffer();
        auto *buf = rowBuffer.GetRowBuffer();

        EXPECT_EQ(buf[0], 0b00000010);
        int16_t data = (int32_t )buf[1];
        EXPECT_EQ(data, 129);

        EXPECT_EQ(buf[3], 0b00001000);
        double d = *(double*)(buf+4);

        EXPECT_TRUE(0 == memcmp((void*)&d, &ori, 8));
        EXPECT_EQ(buf[12], 0b10000001);
        EXPECT_EQ(buf[13], 11);
        EXPECT_TRUE(0 == memcmp((void*)(buf + 14), (void*)testStr.data(), testStr.length()));
        mem::GetMemoryPool()->Release(buf);
    }

    TEST(omni_row, fill_buffer_and_deserial_to_vector) {
        std::vector<DataTypePtr> types({LongDataType::Instance(),DoubleDataType::Instance(),VarcharDataType::Instance()});
        RowBuffer rowBuffer(types);
        int32_t rowNumber = 5;
        int64_t data1[] = {111, 222, 333, 444, 555};
        double data2[] = {999.99f, 999.0f, 999.999f, 99.96f, 999.99999f};
        std::string data3[] = {"Asleep, high machines shall no", "Asleep, indian sciences may in",
                               "As junior schools love simply.", "A", "Ab"};

        DataTypes dataTypes(types);
        VectorBatch *vecBatch = CreateVectorBatch(dataTypes, rowNumber, data1, data2, data3);
        std::vector<uint8_t *> rows;
        rows.reserve(rowNumber);
        for(int32_t i=0;i<vecBatch->GetRowCount(); ++i) {
            rowBuffer.TransValueFromVectorBatch(vecBatch, i);
            rowBuffer.FillBuffer();
            rows.push_back(rowBuffer.GetRowBuffer());
        }

        auto parser = std::make_unique<RowParser>(types);

        // fill fake data into result vector batch before parse function
        int64_t fakedata1[] = {0, 0, 0, 0, 0};
        double fakedata2[] = {0.1f, 0.1f, 0.1f, 0.2f, 0.3f};
        std::string fakedata3[] = {"a","b","c","d","e"};

        VectorBatch *result = CreateVectorBatch(dataTypes, rowNumber, fakedata1, fakedata2, fakedata3);
        BaseVector * vecs[types.size()];
        for(int i=0;i< types.size(); ++i) {
            vecs[i] = result->Get(i);
        }

        for(int32_t i=0;i<vecBatch->GetRowCount(); ++i) {
            parser->ParseOneRow(rows[i], vecs, i);
        }

        // after parse, result should be the same as vecbatch
        EXPECT_TRUE(VecBatchMatch(vecBatch, result));
        VectorHelper::FreeVecBatch(vecBatch);
        VectorHelper::FreeVecBatch(result);
    }


    TEST(omni_row, fill_buffer_and_check_hash) {
        std::vector<DataTypePtr> types({LongDataType::Instance(),DoubleDataType::Instance(),VarcharDataType::Instance()});
        RowBuffer rowBuffer(types, 2);
        int32_t rowNumber = 5;
        int64_t data1[] = {111, 222, 333, 444, 555};
        double data2[] = {999.99f, 999.0f, 999.999f, 99.96f, 999.99999f};
        std::string data3[] = {"Asleep, high machines shall no", "Asleep, indian sciences may in",
                               "As junior schools love simply.", "A", "Ab"};

        DataTypes dataTypes(types);
        VectorBatch *vecBatch = CreateVectorBatch(dataTypes, rowNumber, data1, data2, data3);
        std::vector<uint8_t *> rows;
        rows.reserve(rowNumber);
        for(int32_t i=0;i<vecBatch->GetRowCount(); ++i) {
            rowBuffer.TransValueFromVectorBatch(vecBatch, i);
            rowBuffer.FillBuffer();
            rows.push_back(rowBuffer.GetRowBuffer());
            // 3.get hash position for shuffle
            int32_t hashPos = rowBuffer.CalculateHashPos();

            auto *buffer = rowBuffer.GetRowBuffer();

            auto hashVal = op::HashUtil::HashValue((int8_t *)buffer, hashPos);
            std::cout<<"test calculate hash "<<hashVal<<std::endl;
        }

        VectorHelper::FreeVecBatch(vecBatch);
    }
}