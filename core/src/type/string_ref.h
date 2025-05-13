/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_STRING_REF_H
#define OMNI_RUNTIME_STRING_REF_H
#include <string>
#include <cstring>
#include <arm_neon.h>

namespace omniruntime {
namespace type {
// replace stringRef with std::string_view
struct StringRef {
    const char *data = nullptr;
    size_t size = 0;

    StringRef() = default;

    template <typename T, typename = std::enable_if_t<sizeof(T) == 1>>
    StringRef(const T *d, size_t s) : data(reinterpret_cast<char *>(d)), size(s)
    {}

    StringRef(const std::string &s) : data(const_cast<char *>(s.data())), size(s.size()) {}

    explicit StringRef(char *data_) : data(data_), size(strlen(data_)) {}

    explicit StringRef(char *data_, size_t sz) : data(data_), size(sz) {}

    friend bool ALWAYS_INLINE operator == (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        const char* leftData = lhs.data;
        const char* rightData = rhs.data;
        if (leftSize != rightSize) {
            return false;
        }
        if (leftSize == 0) {
            return true;
        }

        constexpr size_t simd_size = 16;
        size_t i = 0;

        for (;i + simd_size <= leftSize;i += simd_size) {
            uint8x16_t lhs_vec = vld1q_u8(leftData + i);
            uint8x16_t rhs_vec = vld1q_u8(rightData + i);

            uint8x16_t cmp_result = vceqq_u8(lhs_vec, rhs_vec);
            uint64x2_t cmp_wide = vreinterpretq_u64_u8(cmp_result);
            if (vgetq_lane_u64(cmp_wide, 0) != ~0ULL || vgetq_lane_u64(cmp_wide, 1) != ~0ULL) {
                return false;
            }
        }

        for (; i < leftSize; i++) {
            if (leftData[i] != rightData[i]) {
                return false;
            }
        }
        return true;
    }

    friend bool operator != (const StringRef &lhs, const StringRef &rhs)
    {
        return !(lhs == rhs);
    }

    friend bool operator < (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        int cmp = memcmp(lhs.data, rhs.data, std::min(leftSize, rightSize));
        return cmp < 0 || (cmp == 0 && leftSize < rightSize);
    }

    friend bool operator > (const StringRef &lhs, const StringRef &rhs)
    {
        auto leftSize = lhs.size;
        auto rightSize = rhs.size;
        int cmp = memcmp(lhs.data, rhs.data, std::min(leftSize, rightSize));
        return cmp > 0 || (cmp == 0 && leftSize > rightSize);
    }

    std::string ToString() const
    {
        return std::string(data, size);
    }
};
}
}
#endif // OMNI_RUNTIME_STRING_REF_H
