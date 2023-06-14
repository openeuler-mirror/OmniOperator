/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/vector_batch.h"
#include "vector_test_util.h"
#include "vector/dictionary_container.h"

namespace omniruntime::vec::test {
TEST(vector2, vec_batch)
{
    int32_t rowCnt = 32;
    auto intVec = std::make_unique<Vector<int32_t>>(rowCnt);
    auto stringVec = std::make_unique<Vector<std::string>>(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        intVec->SetValue(i, i * 2);
        std::string value("hello" + std::to_string(i));
        stringVec->SetValue(i, value);
    }

    int32_t dictSize = 4;
    int32_t *values = new int32_t[rowCnt];
    for (int32_t i = 0; i < rowCnt; i++) {
        values[i] = i % dictSize;
    }
    using DICTIONARY_DATA_TYPE = typename TYPE_UTIL<int32_t>::DICTIONARY_TYPE;
    auto dictionary = CreateDictionary<DICTIONARY_DATA_TYPE>(dictSize);
    auto container = std::make_shared<DictionaryContainer<int32_t>>(values, rowCnt, dictionary, dictSize, 0);
    std::shared_ptr<AlignedBuffer<bool>> nullsBuffer = std::make_shared<AlignedBuffer<bool>>(rowCnt);
    bool *nulls = nullsBuffer->GetBuffer();
    for (int i = 0; i < rowCnt; i++) {
        nulls[i] = false;
    }
    auto intDictVec = std::make_unique<Vector<DictionaryContainer<int32_t>>>(rowCnt, container, nullsBuffer);

    VectorBatch vectorBatch(rowCnt);
    vectorBatch.Append(intVec.release());
    vectorBatch.Append(stringVec.release());
    vectorBatch.Append(intDictVec.release());

    auto intCol0 = reinterpret_cast<Vector<int32_t> *>(vectorBatch.Get(0));
    auto stringCol1 = reinterpret_cast<Vector<std::string> *>(vectorBatch.Get(1));
    auto intDictCol2 = reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vectorBatch.Get(2));
    for (int32_t i = 0; i < rowCnt; i++) {
        EXPECT_EQ(intCol0->GetValue(i), i * 2);
        EXPECT_EQ(stringCol1->GetValue(i), "hello" + std::to_string(i));
        EXPECT_EQ(intDictCol2->GetValue(i), (i % dictSize) * 2 / 3);
    }
    delete[] values;
    vectorBatch.FreeAllVectors();
}
}