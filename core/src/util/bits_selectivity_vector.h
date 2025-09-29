/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef BITS_SELECTIVITY_VECTOR_H
#define BITS_SELECTIVITY_VECTOR_H
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace omniruntime {
constexpr uint8_t NUMBER_OF_BITS = 64;
constexpr uint8_t BITS_OFFSET = 63;
constexpr uint8_t NUMBER_OF_BYTES = 8;
static constexpr uint8_t kZeroBitmasks[] = {
    static_cast<uint8_t>(~(1 << 0)),
    static_cast<uint8_t>(~(1 << 1)),
    static_cast<uint8_t>(~(1 << 2)),
    static_cast<uint8_t>(~(1 << 3)),
    static_cast<uint8_t>(~(1 << 4)),
    static_cast<uint8_t>(~(1 << 5)),
    static_cast<uint8_t>(~(1 << 6)),
    static_cast<uint8_t>(~(1 << 7)),
};

template <typename T, typename U> constexpr inline T RoundUp(T value, U factor)
{
    return (value + (factor - 1)) / factor * factor;
}

constexpr inline uint64_t Nwords(int32_t bits)
{
    return RoundUp(bits, NUMBER_OF_BITS) / NUMBER_OF_BITS;
}

template <typename T> inline bool IsBitSet(const T *bits, int32_t idx)
{
    return bits[idx / (sizeof(bits[0]) * NUMBER_OF_BYTES)] &
        (static_cast<T>(1) << (idx & ((sizeof(bits[0]) * NUMBER_OF_BYTES) - 1)));
}

constexpr inline uint64_t LowMask(int32_t bits)
{
    return (1UL << bits) - 1;
}

template <bool negate>
inline void OrRange(uint64_t *target, const uint64_t *left, const uint64_t *right, int32_t begin, int32_t end)
{
    ForEachWord(
        begin, end,
        [target, left, right](int32_t idx, uint64_t mask) {
            target[idx] = (target[idx] & ~mask) | (mask & (left[idx] | (negate ? ~right[idx] : right[idx])));
        },
        [target, left, right](int32_t idx) { target[idx] = left[idx] | (negate ? ~right[idx] : right[idx]); });
}

inline void OrBits(uint64_t *target, const uint64_t *left, const uint64_t *right, int32_t begin, int32_t end)
{
    OrRange<false>(target, left, right, begin, end);
}
inline void OrBits(uint64_t *target, const uint64_t *right, int32_t begin, int32_t end)
{
    OrRange<false>(target, target, right, begin, end);
}
constexpr inline uint64_t HighMask(int32_t bits)
{
    return LowMask(bits) << (NUMBER_OF_BITS - bits);
}

template <typename PartialWordFunc, typename FullWordFunc>
inline bool TestWords(int32_t begin, int32_t end, PartialWordFunc partialWordFunc, FullWordFunc fullWordFunc)
{
    if (begin >= end) {
        return true;
    }
    int32_t firstWord = RoundUp(begin, NUMBER_OF_BITS);
    int32_t lastWord = end & ~63L;
    if (lastWord < firstWord) {
        return partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord) & HighMask(firstWord - begin));
    }
    if (begin != firstWord) {
        if (!partialWordFunc(begin / NUMBER_OF_BITS, HighMask(firstWord - begin))) {
            return false;
        }
    }
    for (int32_t i = firstWord; i + NUMBER_OF_BITS <= lastWord; i += NUMBER_OF_BITS) {
        if (!fullWordFunc(i / NUMBER_OF_BITS)) {
            return false;
        }
    }
    if (end != lastWord) {
        return partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord));
    }
    return true;
}

template <typename T> inline void SetBit(T *bits, uint32_t idx)
{
    auto bitsAs8Bit = reinterpret_cast<uint8_t *>(bits);
    bitsAs8Bit[idx / NUMBER_OF_BYTES] |= (1 << (idx % NUMBER_OF_BYTES));
}

template <typename T> inline void ClearBit(T *bits, uint32_t idx)
{
    auto bitsAs8Bit = reinterpret_cast<uint8_t *>(bits);
    bitsAs8Bit[idx / NUMBER_OF_BYTES] &= kZeroBitmasks[idx % NUMBER_OF_BYTES];
}

template <typename T> inline void SetBit(T *bits, uint32_t idx, bool value)
{
    value ? SetBit(bits, idx) : ClearBit(bits, idx);
}

inline void Negate(char *bits, int32_t size)
{
    int32_t i = 0;
    for (; i + NUMBER_OF_BITS <= size; i += NUMBER_OF_BITS) {
        auto wordPtr = reinterpret_cast<uint64_t *>(bits + (i / 8));
        *wordPtr = ~*wordPtr;
    }
    for (; i + NUMBER_OF_BYTES <= size; i += NUMBER_OF_BYTES) {
        bits[i / NUMBER_OF_BYTES] = ~bits[i / NUMBER_OF_BYTES];
    }
    for (; i < size; ++i) {
        SetBit(bits, i, !IsBitSet(bits, i));
    }
}

template <typename PartialWordFunc, typename FullWordFunc>
inline bool TestWordsReverse(int32_t begin, int32_t end, PartialWordFunc partialWordFunc, FullWordFunc fullWordFunc)
{
    if (begin >= end) {
        return true;
    }
    int32_t firstWord = RoundUp(begin, NUMBER_OF_BITS);
    int32_t lastWord = end & ~63L;
    if (lastWord < firstWord) {
        return partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord) & HighMask(firstWord - begin));
    }
    if (end != lastWord) {
        if (!partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord))) {
            return false;
        }
    }
    for (int32_t i = lastWord - NUMBER_OF_BITS; i >= firstWord; i -= NUMBER_OF_BITS) {
        if (!fullWordFunc(i / NUMBER_OF_BITS)) {
            return false;
        }
    }
    if (begin != firstWord) {
        return partialWordFunc(begin / NUMBER_OF_BITS, HighMask(firstWord - begin));
    }
    return true;
}

inline int32_t FindFirstBit(const uint64_t *bits, int32_t begin, int32_t end)
{
    int32_t found = -1;
    TestWords(
        begin, end,
        [bits, &found](int32_t idx, uint64_t mask) {
            uint64_t word = bits[idx] & mask;
            if (word) {
                found = idx * NUMBER_OF_BITS + __builtin_ctzll(word);
                return false;
            }
            return true;
        },
        [bits, &found](int32_t idx) {
            uint64_t word = bits[idx];
            if (word) {
                found = idx * NUMBER_OF_BITS + __builtin_ctzll(word);
                return false;
            }
            return true;
        });
    return found;
}

template <bool negate>
inline void AndRange(uint64_t *target, const uint64_t *left, const uint64_t *right, int32_t begin, int32_t end)
{
    ForEachWord(
        begin, end,
        [target, left, right](int32_t idx, uint64_t mask) {
            target[idx] = (target[idx] & ~mask) | (mask & left[idx] & (negate ? ~right[idx] : right[idx]));
        },
        [target, left, right](int32_t idx) { target[idx] = left[idx] & (negate ? ~right[idx] : right[idx]); });
}

inline void AndBits(uint64_t *target, const uint64_t *left, const uint64_t *right, int32_t begin, int32_t end)
{
    AndRange<false>(target, left, right, begin, end);
}

inline void AndBits(uint64_t *target, const uint64_t *right, int32_t begin, int32_t end)
{
    AndRange<false>(target, target, right, begin, end);
}

inline bool IsAllSet(const uint64_t *bits, int32_t begin, int32_t end, bool value = true)
{
    if (begin >= end) {
        return true;
    }
    uint64_t word = value ? -1 : 0;
    return TestWords(
        begin, end, [bits, word](int32_t idx, uint64_t mask) { return (word & mask) == (bits[idx] & mask); },
        [bits, word](int32_t idx) { return word == bits[idx]; });
}

inline int32_t FindLastBit(const uint64_t *bits, int32_t begin, int32_t end, bool value = true)
{
    int32_t found = -1;
    TestWordsReverse(
        begin, end,
        [bits, &found, value](int32_t idx, uint64_t mask) {
            uint64_t word = (value ? bits[idx] : ~bits[idx]) & mask;
            if (word) {
                found = idx * NUMBER_OF_BITS + BITS_OFFSET - __builtin_clzll(word);
                return false;
            }
            return true;
        },
        [bits, &found, value](int32_t idx) {
            uint64_t word = value ? bits[idx] : ~bits[idx];
            if (word) {
                found = idx * NUMBER_OF_BITS + BITS_OFFSET - __builtin_clzll(word);
                return false;
            }
            return true;
        });
    return found;
}

template <typename PartialWordFunc, typename FullWordFunc>
inline void ForEachWord(int32_t begin, int32_t end, PartialWordFunc partialWordFunc, FullWordFunc fullWordFunc)
{
    if (begin >= end) {
        return;
    }
    int32_t firstWord = RoundUp(begin, NUMBER_OF_BITS);
    int32_t lastWord = end & ~63L;
    if (lastWord < firstWord) {
        partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord) & HighMask(firstWord - begin));
        return;
    }
    if (begin != firstWord) {
        partialWordFunc(begin / NUMBER_OF_BITS, HighMask(firstWord - begin));
    }
    for (int32_t i = firstWord; i + NUMBER_OF_BITS <= lastWord; i += NUMBER_OF_BITS) {
        fullWordFunc(i / NUMBER_OF_BITS);
    }
    if (end != lastWord) {
        partialWordFunc(lastWord / NUMBER_OF_BITS, LowMask(end - lastWord));
    }
}

inline int32_t CountBits(const uint64_t *bits, int32_t begin, int32_t end)
{
    int32_t count = 0;
    ForEachWord(
        begin, end, [&count, bits](int32_t idx, uint64_t mask) { count += __builtin_popcountll(bits[idx] & mask); },
        [&count, bits](int32_t idx) { count += __builtin_popcountll(bits[idx]); });
    return count;
}

inline void FillBits(uint64_t *bits, int32_t begin, int32_t end, bool value)
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
}


#endif // BITS_SELECTIVITY_VECTOR_H
