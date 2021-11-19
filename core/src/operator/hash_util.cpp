/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash util implementations
 */
#include "hash_util.h"
#include <stdint.h>
#include <cmath>


int64_t NextPowerOfTwo(int64_t x);
int32_t HashUtil::HashArraySize(int32_t expected, float f)
{
    double result = static_cast<double>(expected) / static_cast<double>(f);
    int64_t s = static_cast<int64_t>(std::ceil(result));
    s = NextPowerOfTwo(s);

    if (s > MAX_ARRAY_SIZE) {
        // TODO:
        return expected;
    } else {
        return static_cast<int32_t>(s);
    }
}

int64_t NextPowerOfTwo(int64_t x)
{
    if (x == 0) {
        return 1;
    } else {
        --x;
        x |= x >> ROTATE_DISTANCE_1;
        x |= x >> ROTATE_DISTANCE_2;
        x |= x >> ROTATE_DISTANCE_4;
        x |= x >> ROTATE_DISTANCE_8;
        x |= x >> ROTATE_DISTANCE_16;
        return (x | x >> ROTATE_DISTANCE_32) + 1;
    }
}





