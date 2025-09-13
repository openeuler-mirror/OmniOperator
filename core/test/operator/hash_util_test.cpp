/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join operator test implementations
 */
#include <climits>

#include "gtest/gtest.h"
#include "operator/hash_util.h"
#include "operator/hashmap/crc_hasher.h"

using namespace omniruntime::op;
using namespace omniruntime::simdutil;

namespace HashUtilTest {
TEST(HashUtilTest, TestHashValueInt)
{
    int32_t numbers[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectedHashes[] = {
        0,
        9155312661752487122,
        -2783790655855066069,
        -7676864684827762435,
        -1169223928725764010,
        7986088733026723112,
        -3953014584580830079,
        2554626171521168346,
        -2338447857451528020,
        -229164484333897723};
    int32_t length = sizeof(numbers) / sizeof(int32_t);
    int64_t hash = 0;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(numbers[i]);
        EXPECT_EQ(hash, expectedHashes[i]);
    }

    int32_t negativeNumbers[] = {-1, -2, -3, -4, -5, -6, -7, -8, -9, -10};
    int64_t expectedNegativeHashes[] = {
        -6507640756101998425,
        5431462561505554766,
        -8122207483231300484,
        3816895834376252707,
        -5338416827376234415,
        6600686490231318776,
        93045734129320351,
        4986119763102016717,
        2876836389984386420,
        7769910418957082786};
    length = sizeof(negativeNumbers) / sizeof(int32_t);
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(negativeNumbers[i]);
        EXPECT_EQ(hash, expectedNegativeHashes[i]);
    }
}

TEST(HashUtilTest, TestHashValueLong)
{
    int64_t numbers[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectedHashes[] = {
        0,
        9155312661752487122,
        -2783790655855066069,
        -7676864684827762435,
        -1169223928725764010,
        7986088733026723112,
        -3953014584580830079,
        2554626171521168346,
        -2338447857451528020,
        -229164484333897723};
    int32_t expectedPartitions[] = {3, 4, 7, 6, 3, 3, 0, 2, 0, 5};
    int32_t expectedPositions[] = {0, 6, 4, 3, 7, 2, 1, 5, 2, 0};

    int32_t length = sizeof(numbers) / sizeof(int64_t);
    int64_t hash = 0;
    int32_t partition = 0;
    int32_t position = 0;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(numbers[i]);
        EXPECT_EQ(hash, expectedHashes[i]);
        partition = HashUtil::GetRawHashPartition(hash, 7);
        EXPECT_EQ(partition, expectedPartitions[i]);
        position = HashUtil::GetRawHashPosition(hash, 7);
        EXPECT_EQ(position, expectedPositions[i]);
    }

    int64_t negativeNumbers[] = {-1, -2, -3, -4, -5, -6, -7, -8, -9, -10};
    int64_t expectedNegativeHashes[] = {
        -6507640756101998425,
        5431462561505554766,
        -8122207483231300484,
        3816895834376252707,
        -5338416827376234415,
        6600686490231318776,
        93045734129320351,
        4986119763102016717,
        2876836389984386420,
        7769910418957082786};
    int32_t expectedNegativePartitions[] = {1, 6, 3, 6, 2, 6, 5, 1, 3, 0};
    int32_t expectedNegativePositions[] = {7, 4, 2, 2, 3, 7, 5, 7, 1, 6};

    length = sizeof(numbers) / sizeof(int64_t);
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(negativeNumbers[i]);
        EXPECT_EQ(hash, expectedNegativeHashes[i]);
        partition = HashUtil::GetRawHashPartition(hash, 7);
        EXPECT_EQ(partition, expectedNegativePartitions[i]);
        position = HashUtil::GetRawHashPosition(hash, 7);
        EXPECT_EQ(position, expectedNegativePositions[i]);
    }
}

TEST(HashUtilTest, TestHashValueDouble)
{
    double numbers[] = {0.2, 1.2, 2.2, 3.2, 4.2, 5.2, 6.2, 7.2, 8.2, 9.2};
    int64_t expectedHashes[] = {
        -3243017314691278225,
        -6598528617508713083,
        -6834874129546569105,
        6578488081660418671,
        -2493862444946855948,
        -2811374684705953804,
        -3128886924465051660,
        -7844756547208517644,
        5011170576638581514,
        4852414456759032586};
    int32_t expectedPartitions[] = {7, 2, 4, 5, 0, 6, 3, 2, 2, 7};
    int32_t expectedPositions[] = {2, 1, 0, 6, 2, 0, 5, 1, 6, 0};
    int32_t length = sizeof(numbers) / sizeof(double);
    int64_t hash = 0;
    int32_t partition = 0;
    int32_t position = 0;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(numbers[i]);
        EXPECT_EQ(hash, expectedHashes[i]);
        partition = HashUtil::GetRawHashPartition(hash, 7);
        EXPECT_EQ(partition, expectedPartitions[i]);
        position = HashUtil::GetRawHashPosition(hash, 7);
        EXPECT_EQ(position, expectedPositions[i]);
    }

    double negativeNumbers[] = {-0.2, -1.2, -2.2, -3.2, -4.2, -5.2, -6.2, -7.2, -8.2, -9.2};
    int64_t expectedNegativeHashes[] = {
        8179533413655681647,
        425664727853878661,
        189319215816022639,
        -445705263702173073,
        8928688283400103924,
        4212818660656637940,
        3895306420897540084,
        3577794181138442228,
        -6411380151708378358,
        -6570136271587927286};
    int32_t expectedNegativePartitions[] = {4, 5, 1, 7, 4, 1, 4, 0, 4, 0};
    int32_t expectedNegativePositions[] = {2, 6, 2, 7, 0, 0, 5, 4, 3, 6};
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(negativeNumbers[i]);
        EXPECT_EQ(hash, expectedNegativeHashes[i]);
        partition = HashUtil::GetRawHashPartition(hash, 7);
        EXPECT_EQ(partition, expectedNegativePartitions[i]);
        position = HashUtil::GetRawHashPosition(hash, 7);
        EXPECT_EQ(position, expectedNegativePositions[i]);
    }
}

TEST(HashUtilTest, TestHashValueBoolean)
{
    bool values[] = {true, false, true};
    int64_t expectedHashes[] = {1231, 1237, 1231};
    int32_t length = sizeof(values) / sizeof(bool);
    int64_t hash;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(values[i]);
        EXPECT_EQ(hash, expectedHashes[i]);
    }
}

TEST(HashUtilTest, TestHashValueDecimal64)
{
    // "1.1", "2.2", "3.3", "-1.1", "-2.2", "-3.3"
    int64_t values[] = {11, 22, 33, -11, -22, -33};
    int64_t expectedHashes[] = {11, 22, 33, -11, -22, -33};
    int32_t length = sizeof(values) / sizeof(int64_t);
    int64_t hash;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashDecimal64Value(values[i]);
        EXPECT_EQ(hash, expectedHashes[i]);
    }
}

TEST(HashUtilTest, TestHashValueDecimal128)
{
    // "1.2", "1.0", "2.0", "1.5", "-1.2", "-1.0", "-2.0", "-1.5"
    int64_t values[][2] = {
            {12, 0},
            {10, 0},
            {20, 0},
            {15, 0},
            {12, INT64_MIN},
            {10, INT64_MIN},
            {20, INT64_MIN},
            {15, INT64_MIN}};
    int64_t expectedHashes[] = {
        -7175125149685805438,
        3211658658053674137,
        -3971667818412147958,
        -6524242042616555437,
        -7175125149685805438,
        3211658658053674137,
        -3971667818412147958,
        -6524242042616555437};
    int32_t length = 8;
    int64_t hash;
    for (int32_t i = 0; i < length; i++) {
        hash = HashUtil::HashValue(values[i][0], values[i][1]);
        EXPECT_EQ(hash, expectedHashes[i]);
    }
}

TEST(HashUtilTest, TestHashValueVarchar)
{
    // lenth has 3, 4, 5, 8, 11, 20, 32, 36, 72, 180
    std::string values[] = {
        "abc",
        "abcd",
        "abcde",
        "abcde222",
        "abcdefgh222",
        "abcdefghijklmnopqrst",
        "abcdefghijklmnopqrstuvwxyz123456",
        "abcdefghijklmnopqrstuvwxyz1234567890",
        "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890",
        "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890ab"
        "cdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890"
    };
    int64_t expectedHashes[] = {
        4952883123889572249,
        -2449070131962342708,
        568411279426701291,
        -6957354030615793260,
        -2030035951930513236,
        -230643138868552546,
        9832087890187035,
        -8161331784937432616,
        4864693492930352182,
        -1077384717859208249};
    int64_t hash;
    for (int32_t i = 0; i < 8; i++) {
        hash = HashUtil::HashValue((int8_t *)(values[i].c_str()), (int32_t)(values[i].length()));
        EXPECT_EQ(hash, expectedHashes[i]);
    }
}

TEST(HashUtilTest, TestCRCHashNumericTypes)
{
    size_t hash;
    int16_t shortValues[] = {1, 2, 3, INT16_MIN, INT16_MAX};
    size_t expectedShortHashes[] = {1, 2, 3, static_cast<uint64_t>(INT16_MIN), static_cast<uint64_t>(INT16_MAX)};
    for (int i = 0; i < 5; ++i) {
        hash = HashCRC32<int16_t>()(shortValues[i]);
        EXPECT_EQ(hash, expectedShortHashes[i]);
    }

    int32_t intValues[] = {1, 2, 3, INT_MIN, INT_MAX};
    size_t expectedIntHashes[] = {1, 2, 3, static_cast<uint64_t>(INT_MIN), static_cast<uint64_t>(INT_MAX)};
    for (int i = 0; i < 5; ++i) {
        hash = HashCRC32<int32_t>()(intValues[i]);
        EXPECT_EQ(hash, expectedIntHashes[i]);
    }

    int64_t longValues[] = {1L, 2L, 3L, INT64_MIN, INT64_MAX};
    size_t expectedLongHashes[] = {1UL, 2UL, 3UL, static_cast<uint64_t>(INT64_MIN), static_cast<uint64_t>(INT64_MAX)};
    for (int i = 0; i < 5; ++i) {
        hash = HashCRC32<int64_t>()(longValues[i]);
        EXPECT_EQ(hash, expectedLongHashes[i]);
    }

    Decimal128 decimal128Values[] = {Decimal128(0, 1L), Decimal128(0, 2L), Decimal128(0, 3L),
                                     Decimal128(INT64_MAX, UINT64_MAX), Decimal128(INT64_MIN, 0)};
    size_t expectedDecimal128Hashes[] = {1334012139, 1551566872, 2927035878, 2451998871, 1064918637};
    for (int i = 0; i < 5; ++i) {
        hash = HashCRC32<Decimal128>()(decimal128Values[i]);
        EXPECT_EQ(hash, expectedDecimal128Hashes[i]);
    }
}

TEST(HashUtilTest, TestCRCHashVarchar)
{
    size_t hash;
    const std::string str1 = "1234567";
    const std::string str2 = "123456789";
    const std::string str3 = "123456789123456789";
    const std::string str4 = "abcdefghijklmnopqrstuvwxyz";
    const std::string str5 = "01234567890123456789abcdefghijklmnopqrstuvwxyz";
    StringRef stringValue[] = {StringRef(str1), StringRef(str2), StringRef(str3), StringRef(str4), StringRef(str5)};
    size_t expectedStringHashes[] = {306354154, 3808858755, 2825671668, 2665934629, 1551261344};
    for (int i = 0; i < 5; ++i) {
        hash = HashCRC32<StringRef>()(stringValue[i]);
        EXPECT_EQ(hash, expectedStringHashes[i]);
    }
}
}