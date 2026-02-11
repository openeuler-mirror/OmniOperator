/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_BIT_ARRAY_H
#define OMNI_RUNTIME_BIT_ARRAY_H

#include <math.h>
#include <memory>
#include <sstream>
#include <cstring>
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

    int8_t* GetData()
    {
        return data;
    }

    BitArray(int8_t *dataInput)
    {
        wordsNum = (reinterpret_cast<int32_t *>(dataInput))[0]; // numWords is 4 Bytes
        data = dataInput + 4;                                   // offset is 4 Bytes
    }

    /**
     * The BitArray constructor accepts wordsNum as a parameter, allocates memory, and initializes the bit array.
     *
     * @param wordsNum: The length of the bit array, expressed in the number of uint64_t.
     */
    BitArray(int32_t wordsNum) : wordsNum(wordsNum)
    {
        data = new int8_t[wordsNum * sizeof(uint64_t)];
        memset(data, 0, wordsNum * sizeof(uint64_t));
    }

    /**
     * Merge the data from another bit array into the current bit array.
     *
     * @param bitsdata: A pointer to another bit array data, stored as an array of uint64_t.
     */
    void Merge(const uint64_t *bitsdata)
    {
        // Convert the data pointer to type uint64_t*
        uint64_t *currentData = reinterpret_cast<uint64_t *>(data);

        // Perform a bitwise OR operation on each character.
        for (int32_t i = 0; i < wordsNum; i++) {
            currentData[i] |= bitsdata[i];
        }
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