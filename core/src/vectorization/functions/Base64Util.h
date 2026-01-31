/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RFC 4648 Base64 encode/decode for string functions
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include "vectorization/Status.h"

namespace omniruntime::vectorization {

/// Standard Base64 charset (RFC 4648)
constexpr const char kBase64Charset[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encoded output size for given binary input size (with padding)
inline size_t Base64EncodedSize(size_t inputSize) {
    return (inputSize + 2) / 3 * 4;
}

/// Encode binary data to Base64 string. Writes into result (must be pre-sized).
inline void Base64Encode(const char* input, size_t inputSize, char* result) {
    size_t out = 0;
    for (size_t i = 0; i + 2 < inputSize; i += 3) {
        uint32_t v = (static_cast<uint8_t>(input[i]) << 16) |
                     (static_cast<uint8_t>(input[i + 1]) << 8) |
                     static_cast<uint8_t>(input[i + 2]);
        result[out++] = kBase64Charset[(v >> 18) & 63];
        result[out++] = kBase64Charset[(v >> 12) & 63];
        result[out++] = kBase64Charset[(v >> 6) & 63];
        result[out++] = kBase64Charset[v & 63];
    }
    size_t rem = inputSize % 3;
    if (rem == 0) {
        return;
    }
    uint32_t v = static_cast<uint8_t>(input[inputSize - rem]) << 16;
    if (rem == 2) {
        v |= static_cast<uint8_t>(input[inputSize - 1]) << 8;
    }
    result[out++] = kBase64Charset[(v >> 18) & 63];
    result[out++] = kBase64Charset[(v >> 12) & 63];
    if (rem == 2) {
        result[out++] = kBase64Charset[(v >> 6) & 63];
        result[out++] = '=';
    } else {
        result[out++] = '=';
        result[out++] = '=';
    }
}

/// Reverse lookup: base64 char -> 0..63, 255 if invalid
inline uint8_t Base64DecodeChar(char c) {
    if (c >= 'A' && c <= 'Z') return static_cast<uint8_t>(c - 'A');
    if (c >= 'a' && c <= 'z') return static_cast<uint8_t>(c - 'a' + 26);
    if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0' + 52);
    if (c == '+') return 62;
    if (c == '/') return 63;
    return 255;
}

/// Decode Base64 string to binary. Skips non-base64 chars (e.g. CR/LF).
/// On success returns OK and sets decoded size; on error returns Status.
inline Status Base64Decode(const char* input, size_t inputSize, char* output, size_t outputSize, size_t* decodedSize) {
    size_t out = 0;
    uint32_t quad = 0;
    int n = 0;
    for (size_t i = 0; i < inputSize; ++i) {
        uint8_t v = Base64DecodeChar(input[i]);
        if (v == 255) {
            if (input[i] == '=') {
                break;
            }
            continue; /* skip whitespace etc */
        }
        quad = (quad << 6) | v;
        ++n;
        if (n == 4) {
            if (out + 3 > outputSize) {
                return Status::UserError("Base64 decode buffer overflow");
            }
            output[out++] = static_cast<char>((quad >> 16) & 0xFF);
            output[out++] = static_cast<char>((quad >> 8) & 0xFF);
            output[out++] = static_cast<char>(quad & 0xFF);
            n = 0;
            quad = 0;
        }
    }
    if (n == 2) {
        if (out + 1 > outputSize) {
            return Status::UserError("Base64 decode buffer overflow");
        }
        output[out++] = static_cast<char>((quad >> 4) & 0xFF);
    } else if (n == 3) {
        if (out + 2 > outputSize) {
            return Status::UserError("Base64 decode buffer overflow");
        }
        output[out++] = static_cast<char>((quad >> 10) & 0xFF);
        output[out++] = static_cast<char>((quad >> 2) & 0xFF);
    } else if (n == 1) {
        return Status::UserError("Base64 last unit does not have enough valid bits");
    }
    *decodedSize = out;
    return Status::OK();
}

/// Calculate decoded size by scanning input (skip non-base64, handle padding).
/// Returns error if invalid (e.g. incomplete last group).
inline Status Base64DecodedSize(const char* input, size_t inputSize, size_t* decodedSize) {
    size_t valid = 0;
    size_t padding = 0;
    for (size_t i = 0; i < inputSize; ++i) {
        if (input[i] == '=') {
            ++padding;
        } else if (Base64DecodeChar(input[i]) != 255) {
            ++valid;
        }
    }
    if (padding > 2) {
        return Status::UserError("Base64 wrong 4-byte ending unit");
    }
    if (valid + padding < 2) {
        return Status::UserError("Base64 input should at least have 2 bytes");
    }
    size_t blocks = (valid + 3) / 4;
    *decodedSize = blocks * 3;
    if (padding == 1) {
        *decodedSize -= 1;
    } else if (padding == 2) {
        *decodedSize -= 2;
    } else if (valid % 4 == 1) {
        return Status::UserError("Base64 last unit does not have enough valid bits");
    }
    return Status::OK();
}

} // namespace omniruntime::vectorization
