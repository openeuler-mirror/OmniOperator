/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_BIT_ARRAY_H
#define OMNI_RUNTIME_BIT_ARRAY_H

#include <math.h>
#include <sstream>
#include "omni_exception.h"
#include "bit_map.h"

namespace omniruntime {
using namespace std;

class BitArray {
public:
    uint64_t GetBitSize()
    {
        return wordsNum * sizeof(uint64_t) * 8;
    }

    /* * Returns true if the bit changed value. */
    bool Set(uint64_t index)
    {
        if (!Get(index)) {
            reinterpret_cast<uint64_t *>(data)[index >> 6] |=
                (1L << (index % 64)); // set the 'index' position of the bit map as 1.
            return true;
        }
        return false;
    }

    bool Get(uint64_t index)
    {
        return ((reinterpret_cast<uint64_t *>(data))[index >> 6] & (1L << (index % 64))) != 0;
    }

    BitArray(int8_t *dataInput)
    {
        wordsNum = (reinterpret_cast<int32_t *>(dataInput))[0]; // numWords is 4 Bytes
        data = dataInput + 4;                                   // offset is 4 Bytes
    }

    int32_t GetWordsNum()
    {
        return wordsNum;
    }

private:
    int8_t *data;
    int32_t wordsNum;
}; // class bitbarray
} // namespace omniruntime

#endif // OMNI_RUNTIME_BIT_ARRAY_H