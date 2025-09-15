/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: Integer256
 */

#ifndef OMNI_RUNTIME_INTEGER256_H
#define OMNI_RUNTIME_INTEGER256_H

#include <string>
#include "util/compiler_util.h"

namespace omniruntime::type {
using uint128_t = __uint128_t;
using int128_t = __int128_t;

class Integer256 {
public:
    Integer256() = default;

    template<typename T, typename = std::enable_if<std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>>>
    Integer256(T x) : data { x }, size(1), sign(false)
    {}

    Integer256(int64_t x) : data { static_cast<uint64_t>(std::abs(x)) }, size(1), sign(x < 0)
    {}

    Integer256(int32_t x) : data { static_cast<uint64_t>(std::abs(x)) }, size(1), sign(x < 0)
    {}

    Integer256(uint128_t x) :size(x > UINT64_MAX ? 2 : 1), sign(false)
    {
        data[0] = static_cast<uint64_t>(x & UINT64_MAX);
        data[1] = static_cast<uint64_t>(x >> BITS_64);
    }

    Integer256(int128_t x) : size(
        x < 0 ? (static_cast<uint128_t>(std::abs(x)) > UINT64_MAX ? 2 : 1) : (x > UINT64_MAX ? 2 : 1)), sign(x < 0)
    {
        uint128_t k = static_cast<uint128_t>(std::abs(x));
        data[0] = static_cast<uint64_t>(k & UINT64_MAX);
        data[1] = static_cast<uint64_t>(k >> BITS_64);
    }

    inline void Negate()
    {
        sign = !sign;
        if (sign && size == 1 && data[0] == 0) {
                sign = false;
        }
    }

    inline int CompareUnsigned(const Integer256 &x) const
    {
        if (this->size != x.size) {
            return this->size > x.size ? 1 : -1;
        }
        auto pa = this->data;
        auto pb = x.data;
        for (auto i = static_cast<std::ptrdiff_t>(this->size - 1); i >= 0; --i) {
            if (pa[i] != pb[i]) {
                return pa[i] > pb[i] ? 1 : -1;
            }
        }
        return 0;
    }

    inline int Compare(const Integer256 &x) const
    {
        if (this->sign) {
            if (x.sign) {
                return -CompareUnsigned(x);
            } else {
                return -1;
            }
        } else {
            if (x.sign) {
                return 1;
            }
            return CompareUnsigned(x);
        }
    }

    static bool IsZero(const Integer256 &x)
    {
        return (x.size == 1) && (x.data[0] == 0);
    }

    static inline int ComputeSign(const Integer256 &x)
    {
        return IsZero(x) ? 0 : x.sign ? -1 : 1;
    }

    bool inline GetSign() const
    {
        return sign;
    }

    void inline ReSign(bool inputSign)
    {
        sign = inputSign;
    }

    void inline Swap(Integer256 &x)
    {
        std::swap(data, x.data);
        std::swap(sign, x.sign);
        std::swap(size, x.size);
    }

    static inline void AddUnsigned(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        uint128_t carry = 0;
        uint64_t *pr = result.data;
        const uint64_t *px = x.data;
        const uint64_t *py = y.data;
        if (x.size < y.size)
            std::swap(px, py);
        std::size_t maxSize = std::max(x.size, y.size);
        std::size_t minSize = std::min(x.size, y.size);

        if (maxSize == 1) {
            bool s = x.sign;
            result = static_cast<uint128_t>(*x.data) + static_cast<uint128_t>(*y.data);
            result.sign = s;
            return;
        }
        result.size = maxSize;

        uint64_t *pr_end = pr + minSize;
        while (pr != pr_end) {
            carry += static_cast<uint128_t>(*px) + static_cast<uint128_t>(*py);
            *pr = static_cast<uint64_t>(carry);
            carry >>= BITS_64;
            ++pr, ++px, ++py;
        }
        pr_end += maxSize - minSize;
        while (pr != pr_end) {
            if (!carry) {
                if (px != pr) {
                    Copy(px, px + (pr_end - pr), pr);
                }
                break;
            }
            carry += static_cast<uint128_t>(*px);

            *pr = static_cast<uint64_t>(carry);
            carry >>= BITS_64;
            ++pr, ++px;
        }

        if (carry) {
            result.size = maxSize + 1;
            result.data[maxSize] = static_cast<uint64_t>(1u);
        }
        result.Normalize();
        result.sign = x.sign;
    }

    static inline void SubtractUnsigned(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        std::size_t maxSize = std::max(x.size, y.size);
        std::size_t minSize = std::min(x.size, y.size);
        if (maxSize == 1) {
            bool s = x.sign;
            uint64_t al = *x.data;
            uint64_t bl = *y.data;
            if (bl > al) {
                std::swap(al, bl);
                s = !s;
            }
            result = al - bl;
            result.sign = s;
            return;
        }

        int c = x.CompareUnsigned(y);
        result.size = maxSize;
        uint128_t borrow = 0;
        uint64_t *pr = result.data;
        const uint64_t *px = x.data;
        const uint64_t *py = y.data;
        bool swapped = false;
        if (c < 0) {
            std::swap(px, py);
            swapped = true;
        } else if (c == 0) {
            result = Integer256((int64_t) 0);
            return;
        }

        std::size_t i = 0;
        while (i < minSize) {
            borrow = static_cast<uint128_t>(px[i]) - static_cast<uint128_t>(py[i]) - borrow;
            pr[i] = static_cast<uint64_t>(borrow);
            borrow = (borrow >> BITS_64) & 1u;
            ++i;
        }
        while (borrow && (i < maxSize)) {
            borrow = static_cast<uint128_t>(px[i]) - borrow;
            pr[i] = static_cast<uint64_t>(borrow);
            borrow = (borrow >> BITS_64) & 1u;
            ++i;
        }

        if ((maxSize != i) && (px != pr)) {
            Copy(px + i, px + maxSize, pr + i);
        }

        result.Normalize();
        result.sign = x.sign;
        if (swapped) {
            result.Negate();
        }
    }

    static inline void Subtract(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        if (x.sign != y.sign) {
            AddUnsigned(x, y, result);
            return;
        }
        SubtractUnsigned(x, y, result);
    }

    static inline void Subtract(Integer256 &result, const Integer256 &x)
    {
        Subtract(result, x, result);
    }

    static inline void Add(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        if (x.sign != y.sign) {
            SubtractUnsigned(x, y, result);
            return;
        }
        AddUnsigned(x, y, result);
    }

    static inline void Add(Integer256 &result, const Integer256 &x)
    {
        Add(x, result, result);
    }

    static void Multiply(const Integer256 &x, const Integer256 &y, Integer256 &result);

    static inline void Multiply(const Integer256 &x, const uint64_t &y, Integer256 &result)
    {
        if (y == 0) {
            result = static_cast<uint64_t>(0);
            return;
        }
        if ((void *) &x != (void *) &result) {
            result.size = x.size;
        }
        uint128_t carry = 0;
        uint64_t *p = result.data;
        uint64_t *pe = result.data + result.size;
        const uint64_t *px = x.data;
        while (p != pe) {
            carry += static_cast<uint128_t>(*px) * static_cast<uint128_t>(y);
            *p = static_cast<uint64_t>(carry);
            carry >>= BITS_64;
            ++p, ++px;
        }
        if (carry) {
            std::size_t i = result.size;
            result.size = i + 1;
            if (result.size > i) {
                result.data[i] = static_cast<uint64_t>(carry);
            }
        }
        result.sign = x.sign;
        result.Normalize();
    }

    static void DivideUnsigned(const Integer256 &x, const Integer256 &y, Integer256 &result, Integer256 &r);

    static inline void Divide(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        Integer256 r(0);
        bool s = x.GetSign() != y.GetSign();
        DivideUnsigned(x, y, result, r);
        result.ReSign(s);
    }

    static inline void Divide(const Integer256 &x, const Integer256 &y, Integer256 &q, Integer256 &r)
    {
        bool s = x.GetSign() != y.GetSign();
        DivideUnsigned(x, y, q, r);
        q.ReSign(s);
        r.ReSign(x.GetSign());
    }

    static inline void Modulus(const Integer256 &x, const Integer256 &y, Integer256 &result)
    {
        Integer256 q(0);
        DivideUnsigned(x, y, q, result);
        result.ReSign(x.GetSign());
    }

    template<typename T>
    inline T ConvertTo()
    {
        if constexpr (std::is_same_v<T, uint64_t>) {
            return data[0];
        } else if constexpr (std::is_same_v<T, uint128_t>) {
            uint128_t t = data[1];
            t = (t << BITS_64) | data[0];
            return t;
        }
    }

    constexpr static int BITS_64 = 64;

private:
    static inline void Decrement(Integer256 &result) noexcept
    {
        constexpr const uint64_t one = 1;
        if (!result.sign && result.data[0]) {
            --result.data[0];
        } else if (result.sign && (result.data[0] < UINT64_MAX)) {
            ++result.data[0];
        } else {
            Subtract(result, one);
        }
    }

    static inline void Increment(Integer256 &result) noexcept
    {
        constexpr const uint64_t one = 1;
        if (!result.sign && (result.data[0] < UINT64_MAX)) {
            ++result.data[0];
        } else if (result.sign && result.data[0]) {
            --result.data[0];
            if (!result.data[0] && (result.size == 1)) {
                result.sign = false;
            }
        } else {
            Add(result, one);
        }
    }

    void inline Normalize()
    {
        while ((size - 1) != 0 && data[size - 1] == 0) {
            --size;
        }
    }

    static inline void Copy(const uint64_t *first, const uint64_t *last, uint64_t *result)
    {
        while (first != last) {
            *result = *first;
            ++first;
            ++result;
        }
    }

    uint64_t data[4] { 0 };
    std::size_t size = 1;
    bool sign = false;
};

Integer256 operator+(const Integer256 &x, const Integer256 &y);

Integer256 operator-(const Integer256 &x, const Integer256 &y);

Integer256 operator*(const Integer256 &x, const Integer256 &y);

Integer256 operator/(const Integer256 &x, const Integer256 &y);

Integer256 operator%(const Integer256 &x, const Integer256 &y);

bool operator>(const Integer256 &x, const Integer256 &y);

bool operator<(const Integer256 &x, const Integer256 &y);

bool operator==(const Integer256 &x, const Integer256 &y);

bool operator>=(const Integer256 &x, const Integer256 &y);

bool operator<=(const Integer256 &x, const Integer256 &y);
}

#endif // OMNI_RUNTIME_INTEGER256_H
