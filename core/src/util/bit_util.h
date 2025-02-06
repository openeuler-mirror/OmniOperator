/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 */

#ifndef OMNI_RUNTIME_BIT_UTIL_H
#define OMNI_RUNTIME_BIT_UTIL_H

#include <stdint.h>
#include <math.h>

namespace omniruntime {
class BitUtil {
public:
    constexpr static uint8_t kZeroBitmasks[] = {
        static_cast<uint8_t>(~(1 << 0)),
        static_cast<uint8_t>(~(1 << 1)),
        static_cast<uint8_t>(~(1 << 2)),
        static_cast<uint8_t>(~(1 << 3)),
        static_cast<uint8_t>(~(1 << 4)),
        static_cast<uint8_t>(~(1 << 5)),
        static_cast<uint8_t>(~(1 << 6)),
        static_cast<uint8_t>(~(1 << 7)),
    };

    template <typename T, typename U> constexpr static inline T RoundUp(T value, U factor)
    {
        return (value + (factor - 1)) / factor * factor;
    }

    constexpr static inline int32_t Nbytes(int32_t bits)
    {
        return RoundUp(bits, 8) / 8;
    }

    constexpr static inline uint64_t Nwords(int32_t bits)
    {
        return RoundUp(bits, 64) / 64;
    }

    constexpr static inline uint64_t LowMask(int32_t bits)
    {
        return (1UL << bits) - 1;
    }

    constexpr static inline uint64_t HighMask(int32_t bits)
    {
        return LowMask(bits) << (64 - bits);
    }

    template <typename T> static inline bool IsBitSet(const T *bits, int32_t idx)
    {
        return bits[idx / (sizeof(bits[0]) * 8)] & (static_cast<T>(1) << (idx & ((sizeof(bits[0]) * 8) - 1)));
    }

    template <typename T> static inline void SetBit(T *bits, uint32_t idx)
    {
        auto bitsAs8Bit = reinterpret_cast<uint8_t *>(bits);
        bitsAs8Bit[idx / 8] |= (1 << (idx % 8));
    }

    template <typename T> static inline void ClearBit(T *bits, uint32_t idx)
    {
        auto bitsAs8Bit = reinterpret_cast<uint8_t *>(bits);
        bitsAs8Bit[idx / 8] &= kZeroBitmasks[idx % 8];
    }

    template <typename T> static inline void SetBit(T *bits, uint32_t idx, bool value)
    {
        value ? SetBit(bits, idx) : ClearBit(bits, idx);
    }

    template <typename PartialWordFunc, typename FullWordFunc>
    static inline void ForEachWord(int32_t begin, int32_t end, PartialWordFunc partialWordFunc,
        FullWordFunc fullWordFunc)
    {
        if (begin >= end) {
            return;
        }
        int32_t firstWord = RoundUp(begin, 64);
        int32_t lastWord = end & ~63L;
        if (lastWord < firstWord) {
            partialWordFunc(lastWord / 64, LowMask(end - lastWord) & HighMask(firstWord - begin));
            return;
        }
        if (begin != firstWord) {
            partialWordFunc(begin / 64, HighMask(firstWord - begin));
        }
        for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
            fullWordFunc(i / 64);
        }
        if (end != lastWord) {
            partialWordFunc(lastWord / 64, LowMask(end - lastWord));
        }
    }

    template <typename PartialWordFunc, typename FullWordFunc>
    static inline bool testWords(int32_t begin, int32_t end, PartialWordFunc partialWordFunc, FullWordFunc fullWordFunc)
    {
        if (begin >= end) {
            return true;
        }
        int32_t firstWord = RoundUp(begin, 64);
        int32_t lastWord = end & ~63L;
        if (lastWord < firstWord) {
            return partialWordFunc(lastWord / 64, LowMask(end - lastWord) & HighMask(firstWord - begin));
        }
        if (begin != firstWord) {
            if (!partialWordFunc(begin / 64, HighMask(firstWord - begin))) {
                return false;
            }
        }
        for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
            if (!fullWordFunc(i / 64)) {
                return false;
            }
        }
        if (end != lastWord) {
            return partialWordFunc(lastWord / 64, LowMask(end - lastWord));
        }
        return true;
    }

    static inline void FillBits(uint64_t *bits, int32_t begin, int32_t end, bool value)
    {
        ForEachWord(
            begin, end,
            [bits, value](int32_t idx, uint64_t mask) {
                if (value) {
                    bits[idx] |= static_cast<uint64_t>(-1) & mask;
                } else {
                    bits[idx] &= ~mask;
                }
            },
            [bits, value](int32_t idx) { bits[idx] = value ? -1 : 0; });
    }

    static inline int32_t CountBits(const uint64_t *bits, int32_t begin, int32_t end)
    {
        int32_t count = 0;
        ForEachWord(
            begin, end, [&count, bits](int32_t idx, uint64_t mask) { count += __builtin_popcountll(bits[idx] & mask); },
            [&count, bits](int32_t idx) { count += __builtin_popcountll(bits[idx]); });
        return count;
    }

    static inline bool HasBitSet(const uint64_t *bits, int32_t begin, int32_t end)
    {
        return !testWords(
            begin, end,
            [bits](int32_t idx, uint64_t mask) {
                uint64_t word = bits[idx] & mask;
                return !word;
            },
            [bits](int32_t idx) {
                uint64_t word = bits[idx];
                return !word;
            });
    }

    template <typename T> static inline T LoadBits(const uint64_t *source, uint64_t bitOffset, uint8_t numBits)
    {
        constexpr int32_t kBitSize = 8 * sizeof(T);
        auto address = reinterpret_cast<uint64_t>(source) + bitOffset / 8;
        T word = *reinterpret_cast<const T *>(address);
        auto bit = bitOffset & 7;
        if (!bit) {
            return word;
        }
        if (numBits + bit <= kBitSize) {
            return word >> bit;
        }
        uint8_t lastByte = reinterpret_cast<const uint8_t *>(address)[sizeof(T)];
        uint64_t lastBits = static_cast<T>(lastByte) << (kBitSize - bit);
        return (word >> bit) | lastBits;
    }

    template <typename T>
    static inline void StoreBits(uint64_t *target, uint64_t offset, uint64_t word, uint8_t numBits)
    {
        constexpr int32_t kBitSize = 8 * sizeof(T);
        T *address = reinterpret_cast<T *>(reinterpret_cast<uint64_t>(target) + (offset / 8));
        auto bitOffset = offset & 7;
        uint64_t mask = (numBits == 64 ? ~0UL : ((1UL << numBits) - 1)) << bitOffset;
        *address = (*address & ~mask) | (mask & (word << bitOffset));
        if (numBits + bitOffset > kBitSize) {
            uint8_t *lastByteAddress = reinterpret_cast<uint8_t *>(address) + sizeof(T);
            uint8_t lastByteBits = bitOffset + numBits - kBitSize;
            uint8_t lastByteMask = (1 << lastByteBits) - 1;
            *lastByteAddress = (*lastByteAddress & ~lastByteMask) | (lastByteMask & (word >> (kBitSize - bitOffset)));
        }
    }

    static inline void CopyBits(const uint64_t *source, uint64_t sourceOffset, uint64_t *target, uint64_t targetOffset,
        uint64_t numBits)
    {
        uint64_t i = 0;
        for (; i + 64 <= numBits; i += 64) {
            uint64_t word = LoadBits<uint64_t>(source, i + sourceOffset, 64);
            StoreBits<uint64_t>(target, targetOffset + i, word, 64);
        }
        if (i + 32 <= numBits) {
            auto lastWord = LoadBits<uint32_t>(source, sourceOffset + i, 32);
            StoreBits<uint32_t>(target, targetOffset + i, lastWord, 32);
            i += 32;
        }
        if (i + 16 <= numBits) {
            auto lastWord = LoadBits<uint16_t>(source, sourceOffset + i, 16);
            StoreBits<uint16_t>(target, targetOffset + i, lastWord, 16);
            i += 16;
        }
        for (; i < numBits; i += 8) {
            auto copyBits = std::min<uint64_t>(numBits - i, 8);
            auto lastWord = LoadBits<uint8_t>(source, sourceOffset + i, copyBits);
            StoreBits<uint8_t>(target, targetOffset + i, lastWord, copyBits);
        }
    }
};
}

#endif // OMNI_RUNTIME_BIT_UTIL_H
