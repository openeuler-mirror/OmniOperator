#ifndef __BITMAP_UTIL_H__
#define __BITMAP_UTIL_H__


#include <iostream>
namespace BitMapUtil
{
// get the index of byte corresponding to bit index in bitmap.
static inline int byteIndex(int absoluteBitIndex)
{
    return (absoluteBitIndex >> 3);
}

// Get the relative index of bit within the byte in bitmap.
static inline int bitIndex(int absoluteBitIndex)
{
    return absoluteBitIndex & 0x07;
}

static inline void set(uint8_t* bits, int index)
{
    const int byteIdx = byteIndex(index);
    const int bitIdx = bitIndex(index);
    bits[byteIdx] |= (1 << bitIdx);
}

static inline void unset(uint8_t* bits, int index)
{
    const int byteIdx = byteIndex(index);
    const int bitIdx = bitIndex(index);
    bits[byteIdx] &= ~(1 << bitIdx);
}

static inline bool getBit(const uint8_t* bits, int index)
{
    int byteIdx = byteIndex(index);
    int bitIdx = bitIndex(index);
    uint8_t currentByte = bits[byteIdx];
    return (currentByte >> bitIdx) & 0x01;
}
}

#endif // __BITMAP_UTIL_H__