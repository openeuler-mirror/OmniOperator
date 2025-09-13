/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_UTF8_UTIL_H
#define OMNI_RUNTIME_UTF8_UTIL_H

#include <iostream>
#include "bit_map.h"

namespace omniruntime {
class Utf8Util {
    static constexpr uint32_t TOP_MASK32 = 0X80808080;
    static constexpr uint64_t TOP_MASK64 = 0X8080808080808080L;

public:
    static inline int32_t OffsetOfCodePoint(const char *data, int32_t len, int32_t codePointCount)
    {
        return OffsetOfCodePoint(data, len, 0, codePointCount);
    }

    static inline int32_t OffsetOfCodePoint(const char *data, int32_t len, int position, int32_t codePointCount)
    {
        if (len - position <= codePointCount) {
            return -1;
        }
        if (codePointCount == 0) {
            return position;
        }

        int32_t actualIndex = codePointCount + position;
        int length8 = len & 0x7FFFFFF8;
        while (position < length8 && actualIndex >= position + 8) {
            auto *align = reinterpret_cast<const uint64_t *>(data + position);
            actualIndex += CountContinuationBytes(*align);
            position += 8;
        }

        int32_t length4 = len & 0x7FFFFFFC;
        while (position < length4 && actualIndex >= position + 4) {
            auto *align = reinterpret_cast<const uint32_t *>(data + position);
            actualIndex += CountContinuationBytes(*align);
            position += 4;
        }

        while (position < len) {
            actualIndex += CountContinuationBytes(static_cast<uint8_t>(*(data + position)));
            if (position == actualIndex) {
                break;
            }
            position++;
        }

        if (position == actualIndex && actualIndex < len) {
            return actualIndex;
        }

        return -1;
    }

    static inline int32_t CountCodePoints(const char *data, int32_t len)
    {
        return CountCodePoints(data, 0, len);
    }

    static inline int32_t CountCodePoints(const char *data, int32_t offset, int32_t len)
    {
        if (len == 0) {
            return 0;
        }

        int32_t continuationBytesCount = 0;
        int length8 = len & 0x7FFFFFF8;
        while (offset < length8) {
            auto *align = reinterpret_cast<const uint64_t *>(data + offset);
            continuationBytesCount += CountContinuationBytes(*align);
            offset += 8;
        }

        int32_t length4 = len & 0x7FFFFFFC;
        if (offset + 4 < length4) {
            auto *align = reinterpret_cast<const uint32_t *>(data + offset);
            continuationBytesCount += CountContinuationBytes(*align);
            offset += 4;
        }

        while (offset < len) {
            continuationBytesCount += CountContinuationBytes(static_cast<uint8_t>(*(data + offset)));
            offset++;
        }
        return len - continuationBytesCount;
    }

    static inline int32_t LengthOfCodePoint(const char c)
    {
        uint32_t unsignedStartByte = c & 0xFF;
        if (unsignedStartByte < 0b10000000) {
            // normal ASCII
            // 0xxx_xxxx
            return 1;
        }
        if (unsignedStartByte < 0b11000000) {
            // illegal bytes
            // 10xx_xxxx
            return -1;
        }
        if (unsignedStartByte < 0b11100000) {
            // 110x_xxxx 10xx_xxxx
            return 2;
        }
        if (unsignedStartByte < 0b11110000) {
            // 1110_xxxx 10xx_xxxx 10xx_xxxx
            return 3;
        }
        if (unsignedStartByte < 0b11111000) {
            // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
            return 4;
        }
        return -1;
    }

private:
    static inline int32_t CountContinuationBytes(uint8_t value)
    {
        value = value & 0xff;
        return (value >> 7) & (~value >> 6);
    }

    static inline int32_t CountContinuationBytes(uint32_t value)
    {
        value = ((value & TOP_MASK32) >> 1) & (~value);
        return BitMap::BitCountInt(value);
    }

    static inline int32_t CountContinuationBytes(uint64_t value)
    {
        value = ((value & TOP_MASK64) >> 1) & (~value);
        return BitMap::BitCountLong(value);
    }
}; // class utf8Util
} // namespace omniruntime
#endif // OMNI_RUNTIME_UTF8_UTIL_H