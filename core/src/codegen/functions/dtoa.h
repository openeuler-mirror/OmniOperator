/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: registry  function  implementation
 */

#ifndef OMNI_RUNTIME_DTOA_H
#define OMNI_RUNTIME_DTOA_H

#include <iostream>
#include <vector>
#include <sstream>
#include <string>
#include <iomanip>
#include <cstring>
#include <memory>
#include "util/omni_exception.h"
#include "util/compiler_util.h"

namespace omniruntime::codegen::function {
constexpr std::size_t MAX_DATA_LENGTH = 27;

class FDBigInteger {
public:
    FDBigInteger() = default;

    FDBigInteger(int *data, int offset, int length)
    {
        this->offset = offset;
        this->nWords = length;
        this->length = length;
        memcpy(this->data, data, nWords * sizeof(int));
        this->TrimLeadingZeros();
    }

    FDBigInteger(long lValue, const char *digits, int kDigits, int nDigits);

    ~FDBigInteger() = default;

    void Resize(int size)
    {
        nWords = size;
    }

    /**
     * Removes all leading zeros from this <code>FDBigInteger</code> adjusting
     * the offset and number of non-zero leading words accordingly.
     */
    void TrimLeadingZeros();

    void MakeImmutable();

    /**
     * Compares the parameter with this <code>FDBigInteger</code>. Returns an
     * integer accordingly as:
     * <pre>
     * >0: this > other
     *  0: this == other
     * <0: this < other
     * </pre>
     *
     * @param other The <code>FDBigInteger</code> to compare.
     * @return A negative value, zero, or a positive value according to the
     * result of the comparison.
     */
    int Cmp(const FDBigInteger &other) const;

    /**
     * Multiplies by a constant value a big integer represented as an array.
     * The constant factor is an <code>int</code>.
     *
     * @param src The array representation of the big integer.
     * @param srcLen The number of elements of <code>src</code> to use.
     * @param value The constant factor by which to multiply.
     * @param dst The product array.
     */
    ALWAYS_INLINE static void Mul(const int *src, int srcLen, int value, int *dst);

    /**
     * Multiplies this <code>FDBigInteger</code> by an integer.
     *
     * @param i The factor by which to multiply this <code>FDBigInteger</code>.
     * @return This <code>FDBigInteger</code> multiplied by an integer.
     */
    FDBigInteger Mul(int i);

    /**
     * Shifts this <code>FDBigInteger</code> to the left. The shift is performed
     * in-place unless the <code>FDBigInteger</code> is immutable in which case
     * a new instance of <code>FDBigInteger</code> is returned.
     *
     * @param shift The number of bits to shift left.
     * @return The shifted <code>FDBigInteger</code>.
     */
    FDBigInteger LeftShift(int shift);

    /**
     * Computes <code>5</code> raised to a given power.
     *
     * @param p The exponent of 5.
     * @return <code>5<sup>p</sup></code>.
     */
    static FDBigInteger Big5PowRec(int p);

    /**
     * Computes <code>5</code> raised to a given power.
     *
     * @param p The exponent of 5.
     * @return <code>5<sup>p</sup></code>.
     */
    ALWAYS_INLINE static FDBigInteger Big5Pow(int p);

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>5<sup>p5</sup> * 2<sup>p2</sup></code>.
     *
     * @param p5 The exponent of the power-of-five factor.
     * @param p2 The exponent of the power-of-two factor.
     * @return <code>5<sup>p5</sup> * 2<sup>p2</sup></code>
     */
    ALWAYS_INLINE static FDBigInteger ValueOfPow52(int p5, int p2);

    /**
     * Compares this <code>FDBigInteger</code> with <code>x + y</code>. Returns a
     * value according to the comparison as:
     * <pre>
     * -1: this <  x + y
     *  0: this == x + y
     *  1: this >  x + y
     * </pre>
     * @param x The first addend of the sum to compare.
     * @param y The second addend of the sum to compare.
     * @return -1, 0, or 1 according to the result of the comparison.
     */
    int AddAndCmp(const FDBigInteger &x, const FDBigInteger &y);

    /**
     * Multiplies this <code>FDBigInteger</code> by 10. The operation will be
     * performed in place unless the <code>FDBigInteger</code> is immutable in
     * which case a new <code>FDBigInteger</code> will be returned.
     *
     * @return The <code>FDBigInteger</code> multiplied by 10.
     */
    FDBigInteger MulBy10();

    /**
     * Retrieves the normalization bias of the <code>FDBigInteger</code>. The
     * normalization bias is a left shift such that after it the highest word
     * of the value will have the 4 highest bits equal to zero:
     * <code>(highestWord & 0xf0000000) == 0</code>, but the next bit should be 1
     * <code>(highestWord & 0x08000000) != 0</code>.
     *
     * @return The normalization bias.
     */
    int GetNormalizationBias() const;

    /**
     * Computes
     * <pre>
     * q = (int)( this / S )
     * this = 10 * ( this mod S )
     * Return q.
     * </pre>
     * This is the iteration step of digit development for output.
     * We assume that S has been normalized, as above, and that
     * "this" has been left-shifted accordingly.
     * Also assumed, of course, is that the result, q, can be expressed
     * as an integer, 0 <= q < 10.
     *
     * @param The divisor of this <code>FDBigInteger</code>.
     * @return <code>q = (int)(this / S)</code>.
     */
    int QuoRemIteration(FDBigInteger &s);

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>value * 5<sup>p5</sup> * 2<sup>p2</sup></code>.
     *
     * @param value The constant factor.
     * @param p5 The exponent of the power-of-five factor.
     * @param p2 The exponent of the power-of-two factor.
     * @return <code>value * 5<sup>p5</sup> * 2<sup>p2</sup></code>
     */
    ALWAYS_INLINE static FDBigInteger ValueOfMulPow52(long value, int p5, int p2);

    int *GetData()
    {
        return data;
    }

    constexpr static int SMALL_5_POW[] = {
        1, 5, 25, 125, 625, 3125, 15625, 78125, 390625, 1953125, 9765625, 48828125, 244140625, 1220703125};

    constexpr static int SMALL_5_POW_SIZE = 14;

    constexpr static long LONG_5_POW[] = {
        1L, 5L, 25L, 125L, 625L, 3125L, 15625L, 78125L, 390625L, 1953125L, 9765625L, 48828125L, 244140625L, 1220703125L,
        6103515625L, 30517578125L, 152587890625L, 762939453125L, 3814697265625L, 19073486328125L, 95367431640625L,
        476837158203125L, 2384185791015625L, 11920928955078125L, 59604644775390625L, 298023223876953125L,
        1490116119384765625L};

    constexpr static long LONG_5_POW_SIZE = 27;

    constexpr static int MAX_FIVE_POW = 340;
    static std::vector<FDBigInteger> POW_5_CACHE;
    static FDBigInteger ZERO;
    constexpr static long LONG_MASK = 4294967295L;
private:
    int Size() const
    {
        return nWords + offset;
    }

    int DataSize() const
    {
        return static_cast<int>(this->length);
    }

    /**
    * Determines whether all elements of an array are zero for all indices less
    * than a given index.
    *
    * @param a The array to be examined.
    * @param from The index strictly below which elements are to be examined.
    * @return Zero if all elements in range are zero, 1 otherwise.
    */
    static int CheckZeroTail(const int *a, int from);

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>2<sup>p2</sup></code>.
     *
     * @param p2 The exponent of 2.
     * @return <code>2<sup>p2</sup></code>
     */
    ALWAYS_INLINE static FDBigInteger ValueOfPow2(int p2)
    {
        int wordcount = p2 >> 5;
        int bitCount = p2 & 0x1f;
        int temp[] = {1 << bitCount};
        return {temp, wordcount, 1};
    }

    FDBigInteger Add(const FDBigInteger &other);

    ALWAYS_INLINE static int MulAndCarryBy10(const int *src, int srcLen, int *dst);

    void UpdateDataVector(int *newData)
    {
        memcpy(this->data, newData, nWords * sizeof(int));
    }

    /**
     * Multiplies the parameters and subtracts them from this
     * <code>FDBigInteger</code>.
     *
     * @param q The integer parameter.
     * @param s The <code>FDBigInteger</code> parameter.
     * @return <code>this - q*S</code>.
     */
    long MulDiffMe(long q, FDBigInteger &s);

    /**
     * Multiplies by a constant value a big integer represented as an array.
     * The constant factor is a long represent as two <code>int</code>s.
     *
     * @param src The array representation of the big integer.
     * @param srcLen The number of elements of <code>src</code> to use.
     * @param v0 The lower 32 bits of the long factor.
     * @param v1 The upper 32 bits of the long factor.
     * @param dst The product array.
     */
    static void Mul(const int *src, int srcLen, int v0, int v1, int *dst);

    /**
     * Left shifts the contents of one int array into another.
     *
     * @param src The source array.
     * @param idx The initial index of the source array.
     * @param result The destination array.
     * @param bitCount The left shift.
     * @param antiCount The left anti-shift, e.g., <code>32-bitCount</code>.
     * @param prev The prior source value.
     */
    static void LeftShift(const int *src, int idx, int *result, int bitCount, int antiCount, int prev);

    /**
     * Multiplies two big integers represented as int arrays.
     *
     * @param s1 The first array factor.
     * @param s1Len The number of elements of <code>s1</code> to use.
     * @param s2 The second array factor.
     * @param s2Len The number of elements of <code>s2</code> to use.
     * @param dst The product array.
     */
    static void Mul(const int *s1, int s1Len, const int *s2, int s2Len, int *dst);

    /**
     * Multiplies this <code>FDBigInteger</code> by another <code>FDBigInteger</code>.
     *
     * @param other The <code>FDBigInteger</code> factor by which to multiply.
     * @return The product of this and the parameter <code>FDBigInteger</code>s.
     */
    FDBigInteger Mul(FDBigInteger other);

    void MulAddMe(int iv, int addend);

    int data[MAX_DATA_LENGTH]{0};
    int offset = 0;
    int nWords = 0;
    int length = 0;
    bool isImmutable = false;
};

class DoubleConsts {
public:
    static constexpr int EXP_BIAS = 1023;
    static constexpr long SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;
    static constexpr long EXP_BIT_MASK = 0x7FF0000000000000L;
    static constexpr long SIGN_BIT_MASK = 0x8000000000000000L;
};

class DoubleToString {
public:
    DoubleToString() = default;

    void setSign(bool value)
    {
        this->isNegative = value;
    }

    /**
     * This is the easy subcase
     * all the significant bits, after scaling, are held in lvalue.negSign and decExponent tell us
     * what processing and scaling has already been done. Exceptional cases have already been stripped out.
     * In particular:
     * lvalue is a finite number (not Inf, nor NaN)
     * lvalue > 0L (not zero, nor negative).
     *
     * The only reason that we develop the digits here, rather than calling on Long.toString() is
     * that we can do it a little faster,and besides want to treat trailing 0s specially.
     * If Long.toString changes, we should re-evaluate this strategy!
     */
    void DevelopLongDigits(int exponent, long leftValue, int insignificantDigits);

    /**
    * Calculates
    * <pre>
    * InsignificantDigitsForPow2(v) == insignificantDigits(1L<<v)
    * </pre>
    */
    static int InsignificantDigitsForPow2(int p2);

    /**
     * Estimate decimal exponent. (If it is small-ish, we could double-check.)
     *
     * First, scale the mantissa bits such that 1 <= d2 < 2. We are then going to estimate
     *     log10(d2) ~=~  (d2-1.5)/1.5 + log(1.5)
     * and so we can estimate
     *     log10(d) ~=~ log10(d2) + binExp * log10(2)
     * take the floor and call it decExp.
     */
    static int EstimateDecExp(long fractBits, int binExp);

    void Dtoa(int binExp, long fractBits, int nSignificantBits, bool isCompatibleFormat);

    void Roundup();

    int GetChars(char *result) const;

    // just for ut test
    static std::string DoubleToStringConverter(double d);

    static std::size_t DoubleToStringConverter(double d, char *result);

    std::string ToString()
    {
        int len = GetChars(buffer);
        auto res = std::string(buffer, len);
        return res;
    }

    std::size_t ToString(char *result)
    {
        return GetChars(result);
    }

    static constexpr int SIGNIFICAND_WIDTH = 53;
    static constexpr int MAX_DIGIT_INDEX = 20;
    static constexpr int EXP_SHIFT = SIGNIFICAND_WIDTH - 1;
    static constexpr long FRACT_HOB = (1L << EXP_SHIFT);
    static constexpr int MAX_SMALL_BIN_EXP = 62;
    static constexpr int MIN_SMALL_BIN_EXP = -(63 / 3);
    static constexpr long EXP_ONE = ((long)DoubleConsts::EXP_BIAS) << EXP_SHIFT; // exponent of 1.0
    static constexpr int INSIGNIFICANT_DIGITS_NUMBER[] = {
        0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 9, 10, 10,
        10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15, 16, 16, 16, 17, 17, 17, 18, 18, 18, 19};
    static constexpr int INSIGNIFICANT_DIGITS_NUMBER_SIZE = 64;
    static constexpr int N_5_BITS[] = {
        0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56, 59, 61};
    static constexpr int N_5_BITS_SIZE = 27;
private:
    bool isNegative;
    int decExponent;
    int firstDigitIndex;
    int nDigits;
    char digits[MAX_DIGIT_INDEX];
    char buffer[26];
};
}
#endif // OMNI_RUNTIME_DTOA_H
