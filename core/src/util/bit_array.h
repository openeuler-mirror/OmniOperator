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
    uint64_t GetNumWords(uint64_t numBits)
    {
        if (numBits <= 0) {
            throw omniruntime::exception::OmniException("ILLEGAL_INPUT",
                "numBits must be positive, but got " + numBits);
        }
        uint64_t numWords = (uint64_t)ceil(numBits / 64.0);
        if (numWords > UINT64_MAX) {
            throw omniruntime::exception::OmniException("ILLEGAL_INPUT",
                "Can't allocate enough space for bits length " + numBits);
        }
        return numWords;
    }

    uint64_t GetBitSize()
    {
        return wordsNum * sizeof(uint64_t) * 8;
    }

    /* * Returns true if the bit changed value. */
    bool Set(uint64_t index)
    {
        if (!Get(index)) {
            data[(uint32_t)((index >> 3) >> 3)] |=
                (1L << (index % 64)); // set the 'index' position of the bit map as 1.
            bitCount++;
            return true;
        }
        return false;
    }

    bool Get(uint64_t index)
    {
        return (data[(uint32_t)((index >> 3) >> 3)] & (1L << (index % 64))) != 0;
    }

    BitArray(uint64_t *dataInput)
    {
        wordsNum = ((int32_t *)dataInput)[0];            // numWords is 4 Bytes
        data = (uint64_t *)(((int32_t *)dataInput) + 1); // offset is 4 Bytes
        uint64_t word;
        uint64_t bitCountTmp = 0;
        for (int32_t i = 0; i < wordsNum; i++) {
            word = (uint64_t)data[i];
            bitCountTmp += BitMap::BitCountLong(word);
        }
        bitCount = bitCountTmp;
    }

    int32_t GetWordsNum()
    {
        return wordsNum;
    }

    int64_t GetBitCount()
    {
        return bitCount;
    }

private:
    uint64_t *data;
    int32_t wordsNum;
    int64_t bitCount;
}; // class bitbarray
} // namespace omniruntime

#endif // OMNI_RUNTIME_BIT_ARRAY_H