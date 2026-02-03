/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Reverse function implementation for Array and String types
 */

#include "ReverseFunction.h"

#include <vector>
#include "vectorization/SimpleFunction.h"
#include "./vectorization/VectorReaders.h"
#include "type/data_type.h"
#include "../SimpleFunctionMetadata.h"
#include "util/compiler_util.h"
#include "../registration/SimpleFunctionRegistry.h"
#include "vectorization/ComplexViewTypes.h"

namespace omniruntime::vectorization {
namespace {

/// Simple reverse function for string types using SimpleFunction template
/// This provides an alternative registration path for string reversal
template <typename TExecParams>
struct ReverseStringFunc {
    template <typename TOutput, typename TInput>
    ALWAYS_INLINE bool call(TOutput &out, const TInput &input)
    {
        if (input.size() == 0) {
            out.resize(0);
            return true;
        }

        auto inputSize = input.size();
        out.resize(inputSize);

        // Check if it's ASCII-only for optimization
        bool isAscii = true;
        for (size_t i = 0; i < inputSize; ++i) {
            if (static_cast<unsigned char>(input[i]) > 127) {
                isAscii = false;
                break;
            }
        }

        if (isAscii) {
            // Simple byte-level reverse for ASCII
            for (size_t i = 0; i < inputSize; ++i) {
                out[i] = input[inputSize - 1 - i];
            }
        } else {
            // UTF-8 aware reverse
            std::vector<std::pair<size_t, size_t>> charPositions;  // (start, length)
            size_t i = 0;
            while (i < inputSize) {
                size_t charLen = GetUtf8CharLengthInternal(static_cast<unsigned char>(input[i]));
                if (i + charLen > inputSize) {
                    charLen = 1;  // Invalid UTF-8 sequence
                }
                charPositions.push_back({i, charLen});
                i += charLen;
            }

            // Copy characters in reverse order
            size_t outIdx = 0;
            for (auto it = charPositions.rbegin(); it != charPositions.rend(); ++it) {
                for (size_t j = 0; j < it->second; ++j) {
                    out[outIdx++] = input[it->first + j];
                }
            }
        }

        return true;
    }

private:
    /// Get the length of a UTF-8 character based on its first byte
    static size_t GetUtf8CharLengthInternal(unsigned char firstByte)
    {
        if ((firstByte & 0x80) == 0) {
            return 1;  // ASCII
        } else if ((firstByte & 0xE0) == 0xC0) {
            return 2;  // 2-byte UTF-8
        } else if ((firstByte & 0xF0) == 0xE0) {
            return 3;  // 3-byte UTF-8
        } else if ((firstByte & 0xF8) == 0xF0) {
            return 4;  // 4-byte UTF-8
        }
        return 1;  // Invalid, treat as single byte
    }
};

} // namespace

} // namespace omniruntime::vectorization
