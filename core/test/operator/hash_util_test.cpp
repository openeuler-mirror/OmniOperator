/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join operator test implementations
 */
#include "gtest/gtest.h"
#include "../../src/operator/hash_util.h"

TEST(HashUtilTest, TestHashValue)
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
            -229164484333897723 };
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
}

TEST(HashUtilTest, TestNegativeHashValue)
{
    int64_t numbers[] = {-1, -2, -3, -4, -5, -6, -7, -8, -9, -10};
    int64_t expectedHashes[] = {
            -6507640756101998425,
            5431462561505554766,
            -8122207483231300484,
            3816895834376252707,
            -5338416827376234415,
            6600686490231318776,
            93045734129320351,
            4986119763102016717,
            2876836389984386420,
            7769910418957082786 };
    int32_t expectedPartitions[] = {1, 6, 3, 6, 2, 6, 5, 1, 3, 0};
    int32_t expectedPositions[] = {7, 4, 2, 2, 3, 7, 5, 7, 1, 6};

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
}