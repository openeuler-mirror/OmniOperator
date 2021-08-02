#ifndef __BITMAP_UTIL_H__
#define __BITMAP_UTIL_H__


#include <iostream>
namespace BitMapUtil
{
    static int ROUND_8_MASK_INT = 0xFFFFFFF8;
// get the index of byte corresponding to bit index in bitmap.
static inline int ByteIndex(int absoluteBitIndex)
{
    return (absoluteBitIndex >> 3);
}

// Get the relative index of bit within the byte in bitmap.
static inline int BitIndex(int absoluteBitIndex)
{
    return absoluteBitIndex & 0x07;
}

static inline void Set(uint8_t* bits, int index)
{
    const int byteIdx = ByteIndex(index);
    const int bitIdx = BitIndex(index);
    bits[byteIdx] |= (1 << bitIdx);
}

static inline void UnSet(uint8_t* bits, int index)
{
    const int byteIdx = ByteIndex(index);
    const int bitIdx = BitIndex(index);
    bits[byteIdx] &= ~(1 << bitIdx);
}

static inline bool GetBit(const uint8_t* bits, int index)
{
    int byteIdx = ByteIndex(index);
    int bitIdx = BitIndex(index);
    uint8_t currentByte = bits[byteIdx];
    return (currentByte >> bitIdx) & 0x01;
}

// calculate the nearest number of bytes based on the number of elements
static inline int computeSizeInBytes(int size)
{
    return ((size + 7) & ROUND_8_MASK_INT) >> 3;
}
}

#endif // __BITMAP_UTIL_H__