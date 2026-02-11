/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Implementation of misc helper functions for vectorization framework
 */

#include "Misc.h"

#include <cstdint>
#include <cstdio>
#include <random>
#include <string>

namespace omniruntime::vectorization {

std::string GenerateUuidV4(std::mt19937& generator)
{
    // Generate 16 random bytes
    uint8_t bytes[16];
    for (int i = 0; i < 16; i += 4) {
        uint32_t r = generator();
        bytes[i]     = static_cast<uint8_t>(r & 0xFF);
        bytes[i + 1] = static_cast<uint8_t>((r >> 8) & 0xFF);
        bytes[i + 2] = static_cast<uint8_t>((r >> 16) & 0xFF);
        bytes[i + 3] = static_cast<uint8_t>((r >> 24) & 0xFF);
    }

    // Set version to 4: byte[6] high nibble = 0x4
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    // Set variant to RFC 4122: byte[8] high 2 bits = 0b10
    bytes[8] = (bytes[8] & 0x3F) | 0x80;

    // Format as UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    char buf[37]; // 36 chars + null terminator
    snprintf(buf, sizeof(buf),
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    return std::string(buf, 36);
}

} // namespace omniruntime::vectorization
