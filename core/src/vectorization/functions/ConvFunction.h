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
#include <charconv>
#include <limits>
#include <cctype>
#include <iostream>

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
        uint64_t unsignedValue = 0;
        auto fromStatus = std::from_chars(
            input.data() + start,
            input.data() + input.size(),
            unsignedValue,
            fromBase);
        if (fromStatus.ec == std::errc::invalid_argument) {
            return 0;
        }
        if (fromStatus.ec == std::errc::result_out_of_range) {
            return kMaxUnsignedInt64;
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

    static void toCharsSigned(
        std::string &result,
        int64_t signedValue,
        int32_t toBase,
        int32_t resultSize)
    {
        result.resize(resultSize);
        auto toStatus = std::to_chars(
            result.data(), result.data() + result.size(), signedValue, -toBase);
        result.resize(toStatus.ptr - result.data());
    }

    static void toCharsUnsigned(
        std::string &result,
        uint64_t unsignedValue,
        int32_t toBase,
        int32_t resultSize)
    {
        result.resize(resultSize);
        auto toStatus = std::to_chars(
            result.data(), result.data() + result.size(), unsignedValue, std::abs(toBase));
        result.resize(toStatus.ptr - result.data());
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