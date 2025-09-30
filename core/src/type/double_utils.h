/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * @Description: double utils implementations
 */
// Copyright 2012 the V8 project authors. All rights reserved.
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are
//met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
//      copyright notice, this list of conditions and the following
//      disclaimer in the documentation and/or other materials provided
//      with the distribution.
//    * Neither the name of Google Inc. nor the names of its
//      contributors may be used to endorse or promote products derived
//      from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
//"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
//LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
//OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
//LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
//DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
//THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef OMNI_RUNTIME_DOUBLE_UTILS_H
#define OMNI_RUNTIME_DOUBLE_UTILS_H

#include <vector>
#include <string>
#include <cstring>
#include <limits>
#include <cstring>
#include "width_integer.h"
#include "big_integer.h"

#define DOUBLE_CONVERSION_UINT64_2PART_C(a, b) (((static_cast<uint64_t>(a) << 32) + 0x##b##u))
#define DOUBLE_CONVERSION_ARRAY_SIZE(a)                 \
    ((sizeof(a) / sizeof(*(a))) /                       \
    static_cast<size_t>(!(sizeof(a) % sizeof(*(a)))))

namespace omniruntime::type {
static const int MAX_DECIMAL_POWER = 309;
static const int MIN_DECIMAL_POWER = -324;
static const int MAX_EXACT_DOUBLE_INT_DECIMAL = 15;
static constexpr uint64_t MAX_UINT64 = DOUBLE_CONVERSION_UINT64_2PART_C(0xFFFFFFFF, FFFFFFFF);
static const int DECIMAL_EXPONENT_DISTANCE = 8;
static const int POWERS_OFFSET = 348;
static const int MIN_DECIMAL_EXPONENT = -348;
// 2^64 = 18446744073709551616 > 10^19
static const int MAX_UINT64_DECIMAL_DIGITS = 19;

static const int SIGNIFICAND_SIZE = 53;
static const int PHYSICAL_SIGNIFICAND_SIZE = 52;  // Excludes the hidden bit.
static constexpr int EXPONENT_BIAS = 0x3FF + PHYSICAL_SIGNIFICAND_SIZE;
static constexpr int NORMAL_EXPONENT = -EXPONENT_BIAS + 1;

static constexpr double EXACT_POWERS_OF_TEN[] = {
    1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0, 1000000000.0, 10000000000.0,
    100000000000.0, 1000000000000.0, 10000000000000.0, 100000000000000.0, 1000000000000000.0, 10000000000000000.0,
    100000000000000000.0, 1000000000000000000.0, 10000000000000000000.0, 100000000000000000000.0,
    1000000000000000000000.0, 10000000000000000000000.0
};

template<class Dest, class Source>
static inline Dest BitCast(const Source &source)
{
    Dest dest;
    memmove(&dest, &source, sizeof(dest));
    return dest;
}

static inline uint64_t DoubleToUint64(double d)
{
    return BitCast<uint64_t>(d);
}

static inline double Uint64ToDouble(uint64_t d64)
{
    return BitCast<double>(d64);
}

struct Power {
    uint64_t significand;
    int16_t binaryExponent;
    int16_t decimalExponent;
};

static constexpr Power POWERS_VALUE[] = {
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xfa8fd5a0, 081c0288), -1220, -348},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xbaaee17f, a23ebf76), -1193, -340},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8b16fb20, 3055ac76), -1166, -332},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xcf42894a, 5dce35ea), -1140, -324},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9a6bb0aa, 55653b2d), -1113, -316},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xe61acf03, 3d1a45df), -1087, -308},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xab70fe17, c79ac6ca), -1060, -300},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xff77b1fc, bebcdc4f), -1034, -292},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xbe5691ef, 416bd60c), -1007, -284},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8dd01fad, 907ffc3c), -980,  -276},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd3515c28, 31559a83), -954,  -268},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9d71ac8f, ada6c9b5), -927,  -260},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xea9c2277, 23ee8bcb), -901,  -252},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xaecc4991, 4078536d), -874,  -244},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x823c1279, 5db6ce57), -847,  -236},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc2109436, 4dfb5637), -821,  -228},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9096ea6f, 3848984f), -794,  -220},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd77485cb, 25823ac7), -768,  -212},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa086cfcd, 97bf97f4), -741,  -204},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xef340a98, 172aace5), -715,  -196},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb23867fb, 2a35b28e), -688,  -188},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x84c8d4df, d2c63f3b), -661,  -180},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc5dd4427, 1ad3cdba), -635,  -172},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x936b9fce, bb25c996), -608,  -164},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xdbac6c24, 7d62a584), -582,  -156},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa3ab6658, 0d5fdaf6), -555,  -148},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xf3e2f893, dec3f126), -529,  -140},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb5b5ada8, aaff80b8), -502,  -132},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x87625f05, 6c7c4a8b), -475,  -124},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc9bcff60, 34c13053), -449,  -116},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x964e858c, 91ba2655), -422,  -108},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xdff97724, 70297ebd), -396,  -100},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa6dfbd9f, b8e5b88f), -369,  -92},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xf8a95fcf, 88747d94), -343,  -84},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb9447093, 8fa89bcf), -316,  -76},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8a08f0f8, bf0f156b), -289,  -68},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xcdb02555, 653131b6), -263,  -60},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x993fe2c6, d07b7fac), -236,  -52},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xe45c10c4, 2a2b3b06), -210,  -44},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xaa242499, 697392d3), -183,  -36},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xfd87b5f2, 8300ca0e), -157,  -28},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xbce50864, 92111aeb), -130,  -20},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8cbccc09, 6f5088cc), -103,  -12},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd1b71758, e219652c), -77,   -4},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9c400000, 00000000), -50,   4},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xe8d4a510, 00000000), -24,   12},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xad78ebc5, ac620000), 3,     20},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x813f3978, f8940984), 30,    28},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc097ce7b, c90715b3), 56,    36},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8f7e32ce, 7bea5c70), 83,    44},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd5d238a4, abe98068), 109,   52},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9f4f2726, 179a2245), 136,   60},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xed63a231, d4c4fb27), 162,   68},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb0de6538, 8cc8ada8), 189,   76},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x83c7088e, 1aab65db), 216,   84},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc45d1df9, 42711d9a), 242,   92},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x924d692c, a61be758), 269,   100},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xda01ee64, 1a708dea), 295,   108},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa26da399, 9aef774a), 322,   116},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xf209787b, b47d6b85), 348,   124},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb454e4a1, 79dd1877), 375,   132},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x865b8692, 5b9bc5c2), 402,   140},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xc83553c5, c8965d3d), 428,   148},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x952ab45c, fa97a0b3), 455,   156},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xde469fbd, 99a05fe3), 481,   164},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa59bc234, db398c25), 508,   172},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xf6c69a72, a3989f5c), 534,   180},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xb7dcbf53, 54e9bece), 561,   188},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x88fcf317, f22241e2), 588,   196},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xcc20ce9b, d35c78a5), 614,   204},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x98165af3, 7b2153df), 641,   212},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xe2a0b5dc, 971f303a), 667,   220},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xa8d9d153, 5ce3b396), 694,   228},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xfb9b7cd9, a4a7443c), 720,   236},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xbb764c4c, a7a44410), 747,   244},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8bab8eef, b6409c1a), 774,   252},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd01fef10, a657842c), 800,   260},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9b10a4e5, e9913129), 827,   268},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xe7109bfb, a19c0c9d), 853,   276},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xac2820d9, 623bf429), 880,   284},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x80444b5e, 7aa7cf85), 907,   292},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xbf21e440, 03acdd2d), 933,   300},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x8e679c2f, 5e44ff8f), 960,   308},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xd433179d, 9c8cb841), 986,   316},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0x9e19db92, b4e31ba9), 1013,  324},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xeb96bf6e, badf77d9), 1039,  332},
    {DOUBLE_CONVERSION_UINT64_2PART_C(0xaf87023b, 9bf0ee6b), 1066,  340},
};

static constexpr int EXACT_POWERS_OF_TEN_SIZE = DOUBLE_CONVERSION_ARRAY_SIZE(EXACT_POWERS_OF_TEN);

class DiyFp {
public:
    static const int SIGNIFICAND_SIZE = 64;

    DiyFp() : significand_(0), exponent_(0)
    {}

    DiyFp(const uint64_t significand, const int32_t exponent) : significand_(significand), exponent_(exponent)
    {}

    // this -= other.
    // The exponents of both numbers must be the same and the significand of this
    // must be greater or equal than the significand of other.
    // The result will not be normalized.
    void Subtract(const DiyFp &other)
    {
        significand_ -= other.significand_;
    }

    // Returns a - b.
    // The exponents of both numbers must be the same and a must be greater
    // or equal than b. The result will not be normalized.
    static DiyFp Minus(const DiyFp &a, const DiyFp &b)
    {
        DiyFp result = a;
        result.Subtract(b);
        return result;
    }

    // this *= other.
    void Multiply(const DiyFp &other)
    {
        // Simply "emulates" a 128 bit multiplication.
        // However: the resulting number only contains 64 bits. The least
        // significant 64 bits are only used for rounding the most significant 64
        // bits.
        const uint64_t kM32 = 0xFFFFFFFFU;
        const uint64_t a = significand_ >> 32;
        const uint64_t b = significand_ & kM32;
        const uint64_t c = other.significand_ >> 32;
        const uint64_t d = other.significand_ & kM32;
        const uint64_t ac = a * c;
        const uint64_t bc = b * c;
        const uint64_t ad = a * d;
        const uint64_t bd = b * d;
        // By adding 1U << 31 to tmp we round the final result.
        // Halfway cases will be rounded up.
        const uint64_t tmp = (bd >> 32) + (ad & kM32) + (bc & kM32) + (1U << 31);
        exponent_ += other.exponent_ + 64;
        significand_ = ac + (ad >> 32) + (bc >> 32) + (tmp >> 32);
    }

    static DiyFp Times(const DiyFp &a, const DiyFp &b)
    {
        DiyFp product = a;
        product.Multiply(b);
        return product;
    }

    void Normalize()
    {
        uint64_t significand = significand_;
        int32_t exponent = exponent_;

        // This method is mainly called for normalizing boundaries. In general,
        // boundaries need to be shifted by 10 bits, and we optimize for this case.
        const uint64_t k10MSBits = DOUBLE_CONVERSION_UINT64_2PART_C(0xFFC00000, 00000000);
        while ((significand & k10MSBits) == 0) {
            significand <<= 10;
            exponent -= 10;
        }
        while ((significand & UINT64_MSB) == 0) {
            significand <<= 1;
            exponent--;
        }
        significand_ = significand;
        exponent_ = exponent;
    }

    static DiyFp Normalize(const DiyFp &a)
    {
        DiyFp result = a;
        result.Normalize();
        return result;
    }

    uint64_t GetSignificand() const
    {
        return significand_;
    }

    int32_t GetExponent() const
    {
        return exponent_;
    }

    void SetSignificand(uint64_t newValue)
    {
        significand_ = newValue;
    }

    void SetExponent(int32_t newValue)
    {
        exponent_ = newValue;
    }

private:
    static const uint64_t UINT64_MSB = DOUBLE_CONVERSION_UINT64_2PART_C(0x80000000, 00000000);

    uint64_t significand_;
    int32_t exponent_;
};

// Helper functions for doubles.
class Double {
public:
    static const uint64_t SIGN_MASK = DOUBLE_CONVERSION_UINT64_2PART_C(0x80000000, 00000000);
    static const uint64_t EXPONENT_MASK = DOUBLE_CONVERSION_UINT64_2PART_C(0x7FF00000, 00000000);
    static const uint64_t SIGNIFICAND_MASK = DOUBLE_CONVERSION_UINT64_2PART_C(0x000FFFFF, FFFFFFFF);
    static const uint64_t HIDDEN_BIT = DOUBLE_CONVERSION_UINT64_2PART_C(0x00100000, 00000000);
    static const uint64_t QUIET_K_NAN_BIT = DOUBLE_CONVERSION_UINT64_2PART_C(0x00080000, 00000000);
    static const int PHYSICAL_SIGNIFICAND_SIZE = 52;  // Excludes the hidden bit.
    static const int SIGNIFICAND_SIZE = 53;
    static const int EXPONENT_BIAS = 0x3FF + PHYSICAL_SIGNIFICAND_SIZE;
    static const int MAX_EXPONENT = 0x7FF - EXPONENT_BIAS;

    Double() : float64_(0)
    {}

    explicit Double(double d) : float64_(DoubleToUint64(d))
    {}

    explicit Double(uint64_t d64) : float64_(d64)
    {}

    explicit Double(DiyFp diyFp) : float64_(DiyFpToUint64(diyFp))
    {}

    // The value encoded by this Double must be greater or equal to +0.0.
    // It must not be special (K_INFINITY, or K_NAN).
    DiyFp AsDiyFp() const
    {
        return DiyFp(Significand(), Exponent());
    }

    // The value encoded by this Double must be strictly greater than 0.
    DiyFp AsNormalizedDiyFp() const
    {
        uint64_t f = Significand();
        int e = Exponent();

        // The current double could be a denormal.
        while ((f & HIDDEN_BIT) == 0) {
            f <<= 1;
            e--;
        }
        // Do the final shifts in one go.
        f <<= DiyFp::SIGNIFICAND_SIZE - SIGNIFICAND_SIZE;
        e -= DiyFp::SIGNIFICAND_SIZE - SIGNIFICAND_SIZE;
        return DiyFp(f, e);
    }

    // Returns the double's bit as uint64.
    uint64_t AsUint64() const
    {
        return float64_;
    }

    // Returns the next greater double. Returns +K_INFINITY on input +K_INFINITY.
    double NextDouble() const
    {
        if (float64_ == INFINITY_VALUE) return Double(INFINITY_VALUE).value();
        if (Sign() < 0 && Significand() == 0) {
            // -0.0
            return 0.0;
        }
        if (Sign() < 0) {
            return Double(float64_ - 1).value();
        } else {
            return Double(float64_ + 1).value();
        }
    }

    double PreviousDouble() const
    {
        if (float64_ == (INFINITY_VALUE | SIGN_MASK)) return -Infinity();
        if (Sign() < 0) {
            return Double(float64_ + 1).value();
        } else {
            if (Significand() == 0) return -0.0;
            return Double(float64_ - 1).value();
        }
    }

    int Exponent() const
    {
        if (IsDenormal()) return NORMAL_EXPONENT;

        uint64_t d64 = AsUint64();
        int biased_e =
            static_cast<int>((d64 & EXPONENT_MASK) >> PHYSICAL_SIGNIFICAND_SIZE);
        return biased_e - EXPONENT_BIAS;
    }

    uint64_t Significand() const
    {
        uint64_t d64 = AsUint64();
        uint64_t significand = d64 & SIGNIFICAND_MASK;
        if (!IsDenormal()) {
            return significand + HIDDEN_BIT;
        } else {
            return significand;
        }
    }

    // Returns true if the double is a denormal.
    bool IsDenormal() const
    {
        uint64_t d64 = AsUint64();
        return (d64 & EXPONENT_MASK) == 0;
    }

    // We consider denormals not to be special.
    // Hence only K_INFINITY and K_NAN are special.
    bool IsSpecial() const
    {
        uint64_t d64 = AsUint64();
        return (d64 & EXPONENT_MASK) == EXPONENT_MASK;
    }

    bool IsNan() const
    {
        uint64_t d64 = AsUint64();
        return ((d64 & EXPONENT_MASK) == EXPONENT_MASK) && ((d64 & SIGNIFICAND_MASK) != 0);
    }

    bool IsQuietNAN() const
    {
        return IsNan() && ((AsUint64() & QUIET_K_NAN_BIT) != 0);
    }

    bool IsSignalingNAN() const
    {
        return IsNan() && ((AsUint64() & QUIET_K_NAN_BIT) == 0);
    }

    bool IsInfinite() const
    {
        uint64_t d64 = AsUint64();
        return ((d64 & EXPONENT_MASK) == EXPONENT_MASK) && ((d64 & SIGNIFICAND_MASK) == 0);
    }

    int Sign() const
    {
        uint64_t d64 = AsUint64();
        return (d64 & SIGN_MASK) == 0 ? 1 : -1;
    }

    // Precondition: the value encoded by this Double must be greater or equal
    // than +0.0.
    DiyFp UpperBoundary() const
    {
        return DiyFp(Significand() * 2 + 1, Exponent() - 1);
    }

    // Computes the two boundaries of this.
    // The bigger boundary (plus) is normalized. The lower boundary has the same
    // exponent as plus.
    // Precondition: the value encoded by this Double must be greater than 0.
    void NormalizedBoundaries(DiyFp *outMinus, DiyFp *outPlus) const
    {
        DiyFp v = this->AsDiyFp();
        DiyFp plus = DiyFp::Normalize(DiyFp((v.GetSignificand() << 1) + 1, v.GetExponent() - 1));
        DiyFp minus;
        if (LowerBoundaryIsCloser()) {
            minus = DiyFp((v.GetSignificand() << 2) - 1, v.GetExponent() - 2);
        } else {
            minus = DiyFp((v.GetSignificand() << 1) - 1, v.GetExponent() - 1);
        }
        minus.SetSignificand(minus.GetSignificand() << (minus.GetExponent() - plus.GetExponent()));
        minus.SetExponent(plus.GetExponent());
        *outPlus = plus;
        *outMinus = minus;
    }

    bool LowerBoundaryIsCloser() const
    {
        // The boundary is closer when the significand is in the form f == 2^p - 1,
        // which makes the lower boundary closer to the value.
        // For example, consider v = 1000e10 and v- = 9999e9.
        // In this case, the boundary (calculated as (v - v-) / 2) is not just at a distance of 1e9,
        // but rather at a distance of 1e8.
        // The only exception to this rule is for the smallest normal number:
        // the largest denormal number is at the same distance from its successor.
        // Note that denormal numbers share the same exponent as the smallest normal numbers.
        bool isSignificandZero = ((AsUint64() & SIGNIFICAND_MASK) == 0);
        return isSignificandZero && (Exponent() != NORMAL_EXPONENT);
    }

    double value() const
    {
        return Uint64ToDouble(float64_);
    }

    // Returns the significand size for a given order of magnitude.
    // If v = f*2^e with 2^p-1 <= f <= 2^p then p+e is v's order of magnitude.
    // This function returns the number of significant binary digits v will have
    // once it's encoded into a double. In almost all cases this is equal to
    // SIGNIFICAND_SIZE. The only exceptions are denormals. They start with
    // leading zeroes and their effective significand-size is hence smaller.
    static int SignificandSizeForOrderOfMagnitude(int order)
    {
        if (order >= (NORMAL_EXPONENT + SIGNIFICAND_SIZE)) {
            return SIGNIFICAND_SIZE;
        }
        if (order <= NORMAL_EXPONENT) return 0;
        return order - NORMAL_EXPONENT;
    }

    static double Infinity()
    {
        return Double(INFINITY_VALUE).value();
    }

    static double NaN()
    {
        return Double(NAN_VALUE).value();
    }

private:
    static const int NORMAL_EXPONENT = -EXPONENT_BIAS + 1;
    static const uint64_t INFINITY_VALUE = DOUBLE_CONVERSION_UINT64_2PART_C(0x7FF00000, 00000000);
    static const uint64_t NAN_VALUE = DOUBLE_CONVERSION_UINT64_2PART_C(0x7FF80000, 00000000);

    const uint64_t float64_;

    static uint64_t DiyFpToUint64(DiyFp diyFp)
    {
        uint64_t significand = diyFp.GetSignificand();
        int exponent = diyFp.GetExponent();
        while (significand > HIDDEN_BIT + SIGNIFICAND_MASK) {
            significand >>= 1;
            exponent++;
        }
        if (exponent >= MAX_EXPONENT) {
            return INFINITY_VALUE;
        }
        if (exponent < NORMAL_EXPONENT) {
            return 0;
        }
        while (exponent > NORMAL_EXPONENT && (significand & HIDDEN_BIT) == 0) {
            significand <<= 1;
            exponent--;
        }
        uint64_t biasedExponent;
        if (exponent == NORMAL_EXPONENT && (significand & HIDDEN_BIT) == 0) {
            biasedExponent = 0;
        } else {
            biasedExponent = static_cast<uint64_t>(exponent + EXPONENT_BIAS);
        }
        return (significand & SIGNIFICAND_MASK) | (biasedExponent << PHYSICAL_SIGNIFICAND_SIZE);
    }
};

static inline bool ConsumeSubString(const char *buffer, int &offset, int end, const char *substring)
{
    for (substring++; *substring != '\0'; substring++) {
        offset++;
        if (offset == end || std::tolower(buffer[offset]) != *substring) {
            return false;
        }
    }
    offset++;
    return true;
}

static inline uint64_t ReadUint64(std::string_view buffer, int *numberOfReadDigits)
{
    uint64_t result = 0;
    int i = 0;
    while (i < buffer.length() && result <= (MAX_UINT64 / 10 - 1)) {
        int digit = buffer[i++] - '0';
        result = 10 * result + digit;
    }
    *numberOfReadDigits = i;
    return result;
}

static inline bool DoubleStrtod(std::string_view trimmed, int exponent, double *result)
{
    if (static_cast<int>(trimmed.length()) <= MAX_EXACT_DOUBLE_INT_DECIMAL) {
        int readDigits;
        // The trimmed input fits into a double.
        // If the 10^exponent fits into a double too then we can compute the
        // result-double simply by multiplying (resp. dividing) the two numbers.
        // This is possible because IEEE guarantees that floating-point operations
        // return the best possible approximation.
        if (exponent < 0 && -exponent < EXACT_POWERS_OF_TEN_SIZE) {
            // 10^-exponent fits into a double.
            *result = static_cast<double>(ReadUint64(trimmed, &readDigits));
            *result /= EXACT_POWERS_OF_TEN[-exponent];
            return true;
        }
        if (0 <= exponent && exponent < EXACT_POWERS_OF_TEN_SIZE) {
            // 10^exponent fits into a double.
            *result = static_cast<double>(ReadUint64(trimmed, &readDigits));
            *result *= EXACT_POWERS_OF_TEN[exponent];
            return true;
        }
        int remainingDigits =
            MAX_EXACT_DOUBLE_INT_DECIMAL - static_cast<int>(trimmed.length());
        if ((0 <= exponent) &&
            (exponent - remainingDigits < EXACT_POWERS_OF_TEN_SIZE)) {
            // The trimmed string was short and we can multiply it with
            // 10^remainingDigits. As a result the remaining exponent now fits
            // into a double too.
            *result = static_cast<double>(ReadUint64(trimmed, &readDigits));
            *result *= EXACT_POWERS_OF_TEN[remainingDigits];
            *result *= EXACT_POWERS_OF_TEN[exponent - remainingDigits];
            return true;
        }
    }
    return false;
}

static inline void GetPowerForDecimalExponent(int requestedExponent, DiyFp *power, int *foundExponent)
{
    int index = (requestedExponent + POWERS_OFFSET) / DECIMAL_EXPONENT_DISTANCE;
    Power cachedPower = POWERS_VALUE[index];
    *power = DiyFp(cachedPower.significand, cachedPower.binaryExponent);
    *foundExponent = cachedPower.decimalExponent;
}

static inline DiyFp AdjustmentPowerOfTen(int exponent)
{
    switch (exponent) {
        case 1:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0xa0000000, 00000000), -60);
        case 2:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0xc8000000, 00000000), -57);
        case 3:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0xfa000000, 00000000), -54);
        case 4:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0x9c400000, 00000000), -50);
        case 5:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0xc3500000, 00000000), -47);
        case 6:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0xf4240000, 00000000), -44);
        case 7:
            return DiyFp(DOUBLE_CONVERSION_UINT64_2PART_C(0x98968000, 00000000), -40);
        default:
            throw omniruntime::exception::OmniException("Runtime Error", "DiyFp get error exponent");
    }
}

static inline int SignificandSizeForOrderOfMagnitude(int order)
{
    if (order >= (NORMAL_EXPONENT + SIGNIFICAND_SIZE)) {
        return SIGNIFICAND_SIZE;
    }
    if (order <= NORMAL_EXPONENT) return 0;
    return order - NORMAL_EXPONENT;
}

static inline void ReadDiyFp(std::string_view buffer,
    DiyFp *result,
    int *remainingDecimals)
{
    int readDigits;
    uint64_t significand = ReadUint64(buffer, &readDigits);
    if (buffer.length() == readDigits) {
        *result = DiyFp(significand, 0);
        *remainingDecimals = 0;
    } else {
        // Round the significand.
        if (buffer[readDigits] >= '5') {
            significand++;
        }
        // Compute the binary exponent.
        int exponent = 0;
        *result = DiyFp(significand, exponent);
        *remainingDecimals = buffer.length() - readDigits;
    }
}

static bool DiyFpStrtod(std::string_view buffer, int exponent, double *result)
{
    DiyFp input;
    int remainingDecimals;
    ReadDiyFp(buffer, &input, &remainingDecimals);
    // Since we may have dropped some digits the input is not accurate.
    // If remainingDecimals is different than 0 than the error is at most
    // .5 ulp (unit in the last place).
    // We don't want to deal with fractions and therefore keep a common
    // denominator.
    const int denominatorLog = 3;
    const int denominator = 1 << denominatorLog;
    // Move the remaining decimals into the exponent.
    exponent += remainingDecimals;
    uint64_t error = (remainingDecimals == 0 ? 0 : denominator / 2);

    int oldE = input.GetExponent();
    input.Normalize();
    error <<= oldE - input.GetExponent();

    if (exponent < MIN_DECIMAL_EXPONENT) {
        *result = 0.0;
        return true;
    }
    DiyFp cachedPower;
    int cachedDecimalExponent;
    GetPowerForDecimalExponent(exponent, &cachedPower, &cachedDecimalExponent);

    if (cachedDecimalExponent != exponent) {
        int adjustmentExponent = exponent - cachedDecimalExponent;
        DiyFp adjustmentPower = AdjustmentPowerOfTen(adjustmentExponent);
        input.Multiply(adjustmentPower);
        if (MAX_UINT64_DECIMAL_DIGITS - buffer.length() >= adjustmentExponent) {
            // The product of input with the adjustment power fits into a 64 bit
            // integer.
        } else {
            // The adjustment power is exact. There is hence only an error of 0.5.
            error += denominator / 2;
        }
    }

    input.Multiply(cachedPower);
    // The error introduced by a multiplication of a*b equals
    //   error_a + errorB + error_a*errorB/2^64 + 0.5
    // Substituting a with 'input' and b with 'cachedPower' we have
    //   errorB = 0.5  (all cached powers have an error of less than 0.5 ulp),
    //   errorAb = 0 or 1 / kDenominator > error_a*errorB/ 2^64
    int errorB = denominator / 2;
    int errorAb = (error == 0 ? 0 : 1);  // We round up to 1.
    int fixedError = denominator / 2;
    error += errorB + errorAb + fixedError;

    oldE = input.GetExponent();
    input.Normalize();
    error <<= oldE - input.GetExponent();

    // See if the double's significand changes if we add/subtract the error.
    int orderOfMagnitude = DiyFp::SIGNIFICAND_SIZE + input.GetExponent();
    int effectiveSignificandSize = SignificandSizeForOrderOfMagnitude(orderOfMagnitude);
    int precisionDigitsCount =
        DiyFp::SIGNIFICAND_SIZE - effectiveSignificandSize;
    if (precisionDigitsCount + denominatorLog >= DiyFp::SIGNIFICAND_SIZE) {
        // This can only happen for very small denormals. In this case the
        // half-way multiplied by the denominator exceeds the range of an uint64.
        // Simply shift everything to the right.
        int shift_amount = (precisionDigitsCount + denominatorLog) -
            DiyFp::SIGNIFICAND_SIZE + 1;
        input.SetSignificand(input.GetSignificand() >> shift_amount);
        input.SetExponent(input.GetExponent() + shift_amount);
        // We add 1 for the lost precision of error, and kDenominator for
        // the lost precision of input.f().
        error = (error >> shift_amount) + 1 + denominator;
        precisionDigitsCount -= shift_amount;
    }
    uint64_t one64 = 1;
    uint64_t precisionBitsMask = (one64 << precisionDigitsCount) - 1;
    uint64_t precisionBits = input.GetSignificand() & precisionBitsMask;
    uint64_t halfWay = one64 << (precisionDigitsCount - 1);
    precisionBits *= denominator;
    halfWay *= denominator;
    DiyFp roundedInput(input.GetSignificand() >> precisionDigitsCount,
        input.GetExponent() + precisionDigitsCount);
    if (precisionBits >= halfWay + error) {
        roundedInput.SetSignificand(roundedInput.GetSignificand() + 1);
    }
    // If the last_bits are too close to the half-way case than we are too
    // inaccurate and round down. In this case we return false so that we can
    // fall back to a more precise algorithm.

    *result = Double(roundedInput).value();
    if (halfWay - error < precisionBits && precisionBits < halfWay + error) {
        // Too imprecise. The caller will have to fall back to a slower version.
        // However the returned number is guaranteed to be either the correct double, or the next-lower double.
        return false;
    } else {
        return true;
    }
}

static inline bool ComputeGuess(std::string_view trimmed, int exponent,
    double *guess)
{
    if (trimmed.empty()) {
        *guess = 0.0;
        return true;
    }
    if (exponent + static_cast<int>(static_cast<int>(trimmed.length())) - 1 >= MAX_DECIMAL_POWER) {
        *guess = std::numeric_limits<double>::infinity();
        return true;
    }

    if (exponent + static_cast<int>(trimmed.length()) <= MIN_DECIMAL_POWER) {
        *guess = 0.0;
        return true;
    }

    if (DoubleStrtod(trimmed, exponent, guess) || DiyFpStrtod(trimmed, exponent, guess)) {
        return true;
    }
    if (*guess == std::numeric_limits<double>::infinity()) {
        return true;
    }
    return false;
}

static inline int CompareBufferWithDiyFp(std::string_view buffer, int exponent, DiyFp diyFp)
{
    BigInteger bufferNum(buffer);
    BigInteger diyFpNum(diyFp.GetSignificand());
    if (exponent >= 0) {
        bufferNum.MultiplyByPowerOfTen(exponent);
    } else {
        diyFpNum.MultiplyByPowerOfTen(-exponent);
    }
    if (diyFp.GetExponent() > 0) {
        diyFpNum.ShiftLeft(diyFp.GetExponent());
    } else {
        bufferNum.ShiftLeft(-diyFp.GetExponent());
    }
    return BigInteger::Compare(bufferNum, diyFpNum);
}

static inline double StrtodTrimmed(std::string_view trimmed, int exponent)
{
    double guessValue;
    const bool isCorrect = ComputeGuess(trimmed, exponent, &guessValue);
    if (isCorrect) {
        return guessValue;
    }
    DiyFp upperBoundary = Double(guessValue).UpperBoundary();
    int comparison = CompareBufferWithDiyFp(trimmed, exponent, upperBoundary);
    if (comparison < 0) {
        return guessValue;
    } else if (comparison > 0) {
        return Double(guessValue).NextDouble();
    } else if ((Double(guessValue).Significand() & 1) == 0) {
        // Round towards even.
        return guessValue;
    } else {
        return Double(guessValue).NextDouble();
    }
}
}

#endif // OMNI_RUNTIME_DOUBLE_UTILS_H
