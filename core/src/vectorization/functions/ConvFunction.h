/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Conv function for vectorization framework - converts numbers between bases
 */

#pragma once

#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <string>
#include <string_view>
#include <cstdint>
#include <cmath>
#include <limits>
#include <cctype>
#include <cstring>

namespace omniruntime::vectorization {

template <typename T>
struct ConvFunction {
    static constexpr uint64_t kMaxUnsignedInt64 = 0xFFFFFFFFFFFFFFFF;
    static constexpr int kMinBase = 2;
    static constexpr int kMaxBase = 36;

    static bool checkInput(const std::string_view &input, int32_t fromBase, int32_t toBase)
    {
        if (input.empty()) {
            return false;
        }
        if (fromBase < kMinBase || fromBase > kMaxBase ||
            std::abs(toBase) < kMinBase || std::abs(toBase) > kMaxBase) {
            return false;
        }
        return true;
    }

    static int32_t skipLeadingSpaces(const std::string_view &input)
    {
        int32_t i = 0;
        for (; i < static_cast<int32_t>(input.size()); i++) {
            if (input[i] != ' ') {
                break;
            }
        }
        return i;
    }

    static uint64_t toUnsigned(const std::string_view &input, int32_t start, int32_t fromBase)
    {
        const char *p = input.data() + start;
        const char *end = input.data() + input.size();
        if (p >= end) {
            return 0;
        }
        const auto base = static_cast<uint32_t>(fromBase);
        uint64_t unsignedValue = 0;
        bool any = false;
        while (p < end) {
            const unsigned char uc = static_cast<unsigned char>(*p);
            int digit = -1;
            if (uc >= '0' && uc <= '9') {
                digit = static_cast<int>(uc - '0');
            } else if (uc >= 'a' && uc <= 'z') {
                digit = static_cast<int>(uc - 'a' + 10);
            } else if (uc >= 'A' && uc <= 'Z') {
                digit = static_cast<int>(uc - 'A' + 10);
            } else {
                break;
            }
            if (digit >= fromBase) {
                break;
            }
            any = true;
            const auto ud = static_cast<uint64_t>(digit);
            if (unsignedValue > (std::numeric_limits<uint64_t>::max() - ud) / static_cast<uint64_t>(base)) {
                return kMaxUnsignedInt64;
            }
            unsignedValue = unsignedValue * static_cast<uint64_t>(base) + ud;
            ++p;
        }
        if (!any) {
            return 0;
        }
        return unsignedValue;
    }

    static void toUpperCase(char *buffer, int32_t size)
    {
        for (int32_t i = 0; i < size; i++) {
            buffer[i] = static_cast<char>(std::toupper(static_cast<unsigned char>(buffer[i])));
        }
    }

    static std::pair<int64_t, int32_t> getSignedValueAndResultSize(
        uint64_t unsignedValue,
        bool isNegativeInput,
        int32_t toBase)
    {
        auto isMinInt64Num =
            unsignedValue == static_cast<uint64_t>(std::numeric_limits<int64_t>::min());
        int64_t signedValue;
        int64_t absValue;
        if (isMinInt64Num) {
            signedValue = static_cast<int64_t>(unsignedValue);
            absValue = std::numeric_limits<int64_t>::max();
        } else if (!isNegativeInput) {
            signedValue = static_cast<int64_t>(unsignedValue);
            absValue = std::abs(signedValue);
        } else {
            signedValue = -std::abs(static_cast<int64_t>(unsignedValue));
            absValue = std::abs(signedValue);
        }
        int32_t resultSize =
            static_cast<int32_t>(std::floor(std::log(absValue) / std::log(-toBase))) + 1;
        if (signedValue < 0) {
            ++resultSize;
        }
        return std::make_pair(signedValue, resultSize);
    }

    static std::pair<uint64_t, int32_t> getUnsignedValueAndResultSize(
        uint64_t unsignedInput,
        bool isNegativeInput,
        int32_t toBase)
    {
        uint64_t unsignedValue = unsignedInput;
        if (isNegativeInput) {
            int64_t negativeInput = -std::abs(static_cast<int64_t>(unsignedValue));
            unsignedValue = static_cast<uint64_t>(negativeInput);
        }
        int32_t resultSize =
            static_cast<int32_t>(std::floor(std::log(unsignedValue) / std::log(toBase))) + 1;
        return std::make_pair(unsignedValue, resultSize);
    }

    static void unsignedMagnitudeToBaseRightAligned(char *outEnd, uint64_t u, int base, char *&outBegin)
    {
        char *p = outEnd;
        if (u == 0) {
            *--p = '0';
            outBegin = p;
            return;
        }
        const auto b = static_cast<uint32_t>(base);
        while (u > 0) {
            const int d = static_cast<int>(u % b);
            u /= b;
            *--p = static_cast<char>(d < 10 ? '0' + d : 'a' + (d - 10));
        }
        outBegin = p;
    }

    static void toCharsSigned(
        std::string &result,
        int64_t signedValue,
        int32_t toBase,
        int32_t resultSize)
    {
        const int base = -toBase;
        const bool neg = signedValue < 0;
        const uint64_t u = neg ? (static_cast<uint64_t>(-(signedValue + 1)) + 1U) : static_cast<uint64_t>(signedValue);
        result.resize(static_cast<size_t>(resultSize));
        char *const buf = result.data();
        char *const last = buf + resultSize;
        char *begin = last;
        unsignedMagnitudeToBaseRightAligned(last, u, base, begin);
        if (neg) {
            *--begin = '-';
        }
        const size_t len = static_cast<size_t>(last - begin);
        if (begin != buf) {
            std::memmove(buf, begin, len);
        }
        result.resize(len);
    }

    static void toCharsUnsigned(
        std::string &result,
        uint64_t unsignedValue,
        int32_t toBase,
        int32_t resultSize)
    {
        const int base = std::abs(toBase);
        result.resize(static_cast<size_t>(resultSize));
        char *const buf = result.data();
        char *const last = buf + resultSize;
        char *begin = last;
        unsignedMagnitudeToBaseRightAligned(last, unsignedValue, base, begin);
        const size_t len = static_cast<size_t>(last - begin);
        if (begin != buf) {
            std::memmove(buf, begin, len);
        }
        result.resize(len);
    }

    ALWAYS_INLINE bool call(
        std::string &result,
        const std::string_view &input,
        const int32_t &fromBase,
        const int32_t &toBase)
    {

        if (!checkInput(input, fromBase, toBase)) {
            return false;
        }

        auto i = skipLeadingSpaces(input);
        if (i == static_cast<int32_t>(input.size())) {
            return false;
        }
        const bool isNegativeInput = (input[i] == '-');
        if (isNegativeInput) {
            ++i;
        }

        uint64_t unsignedInput = toUnsigned(input, i, fromBase);
        if (unsignedInput == 0) {
            result = "0";
            return true;
        }

        if (toBase < 0) {
            auto [signedValue, resultSize] =
                getSignedValueAndResultSize(unsignedInput, isNegativeInput, toBase);
            toCharsSigned(result, signedValue, toBase, resultSize);
        } else {
            auto [unsignedValue, resultSize] =
                getUnsignedValueAndResultSize(unsignedInput, isNegativeInput, toBase);
            toCharsUnsigned(result, unsignedValue, toBase, resultSize);
        }

        if (std::abs(toBase) > 10) {
            toUpperCase(result.data(), static_cast<int32_t>(result.size()));
        }
        return true;
    }

    ALWAYS_INLINE bool callNullable(
        std::string &result,
        const std::string_view *input,
        const int32_t *fromBase,
        const int32_t *toBase)
    {
        if (input == nullptr || fromBase == nullptr || toBase == nullptr) {
            return false;
        }
        return call(result, *input, *fromBase, *toBase);
    }
};

}  