/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <iostream>
#include <string>
#include <cstring>
#include <regex>
#include <cmath>
#include <huawei_secure_c/include/securec.h>
#include "context_helper.h"
#include "codegen/functions/decimalfunctions.h"

#ifdef _WIN32
#else
#define DLLEXPORT
#endif

using namespace std;
using namespace omniruntime::codegen;
using namespace omniruntime::type;

namespace {
const int THOUSANDS = 1000;
const int HUNDREDS = 100;
const int TENS = 10;
const double SECOND_OF_DAY = 86400.0;
const int BASE_YEAR = 1900;

const int THOU = 0;
const int HUN = 1;
const int TEN = 2;
const int ONE = 3;
}

extern DLLEXPORT int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen)
{
    int min = bpLen;
    if (apLen < min) {
        min = apLen;
    }

    int32_t result = memcmp(ap, bp, min);
    if (result != 0) {
        return result;
    } else {
        return apLen - bpLen;
    }
}

extern DLLEXPORT bool LikeStr(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen)
{
    string s = string(str, strLen);
    string r = string(regexToMatch, regexLen);

    regex re = regex(r);
    return regex_match(s, re);
}

extern DLLEXPORT bool LikeChar(const char *str, int32_t strWidth, int32_t strLen, const char *regexToMatch,
    int32_t regexLen)
{
    string s = string(str, strWidth);
    string r = string(regexToMatch, regexLen);

    regex re = regex(r);
    return regex_match(s, re);
}

extern DLLEXPORT const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, int32_t *outLen)
{
    *outLen = apLen + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *ConcatStrStrRetNull(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, int32_t *outLen, bool *isNull)
{
    *outLen = apLen + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    *outLen = aWidth + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    errno_t res4 = memset_s(ret + aWidth + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK || res4 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharCharRetNull(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen, bool *isNull)
{
    *outLen = aWidth + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    errno_t res4 = memset_s(ret + aWidth + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK || res4 != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen)
{
    *outLen = aWidth + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharStrRetNull(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen, bool *isNull)
{
    *outLen = aWidth + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    *outLen = apLen + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    errno_t res3 = memset_s(ret + apLen + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        char message[] = "Cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *ConcatStrCharRetNull(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, int32_t *outLen, bool *isNull)
{
    *outLen = apLen + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    errno_t res3 = memset_s(ret + apLen + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT int32_t CastStringToDate(int64_t contextPtr, const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    string regexToMatch = "\\d{4}-\\d{2}-\\d{2}$";
    regex re = regex(regexToMatch);
    string s = string(str, strLen);
    if (!regex_match(s, re)) {
        char message[] = "Only support cast date\'YYYY-MM-DD\' to integer";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return -1;
    }

    int32_t i1 = 5;
    int32_t i2 = 8;
    int base = static_cast<int32_t>('0');
    int yr =
        THOUSANDS * (str[THOU] - base) + HUNDREDS * (str[HUN] - base) + TENS * (str[TEN] - base) + (str[ONE] - base);
    int mnth = TENS * (str[i1] - base) + (str[i1 + 1] - base); // compute month
    int day = TENS * (str[i2] - base) + (str[i2 + 1] - base);  // compute day

    struct std::tm epoch = { 0, 0, 0, 1, 1, 70 };
    struct std::tm t = { 0, 0, 0, day, mnth, yr - BASE_YEAR };
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}

extern DLLEXPORT int32_t CastStringToDateRetNull(const char *str, int32_t strLen, bool *isNull)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    string regexToMatch = "\\d{4}-\\d{2}-\\d{2}$";
    regex re = regex(regexToMatch);
    string s = string(str, strLen);
    if (!regex_match(s, re)) {
        *isNull = true;
        return -1;
    }

    *isNull = false;
    int32_t i1 = 5;
    int32_t i2 = 8;
    int base = static_cast<int32_t>('0');
    int yr =
        THOUSANDS * (str[THOU] - base) + HUNDREDS * (str[HUN] - base) + TENS * (str[TEN] - base) + (str[ONE] - base);
    int mnth = TENS * (str[i1] - base) + (str[i1 + 1] - base); // compute month
    int day = TENS * (str[i2] - base) + (str[i2 + 1] - base);  // compute day

    struct std::tm epoch = { 0, 0, 0, 1, 1, 70 };
    struct std::tm t = { 0, 0, 0, day, mnth, yr - BASE_YEAR };
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}

extern DLLEXPORT const char *ToUpper(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    int step = static_cast<int>('a') - static_cast<int>('A');
    for (int i = 0; i < strLen; i++) {
        if (*(str + i) >= static_cast<int>('a') && *(str + i) <= static_cast<int>('z')) {
            *(ret + i) = *(str + i) - step;
        } else {
            *(ret + i) = *(str + i);
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen)
{
    return ToUpper(contextPtr, str, strLen, outLen);
}

// Cast numeric type to string
extern DLLEXPORT const char *CastIntToString(int64_t contextPtr, int32_t value, int32_t *outLen)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(str.size()) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return ret;
    }
    return ret;
}

extern DLLEXPORT const char *CastIntToStringRetNull(int64_t contextPtr, int32_t value, int32_t *outLen, bool *isNull)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(str.size()) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        return ret;
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *CastLongToString(int64_t contextPtr, int64_t value, int32_t *outLen)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str())) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *CastLongToStringRetNull(int64_t contextPtr, int64_t value, int32_t *outLen, bool *isNull)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str())) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *CastDoubleToString(int64_t contextPtr, double value, int32_t *outLen)
{
    int precision = 15;

    ostringstream oss;
    oss.precision(precision);
    oss << value;
    *outLen = static_cast<int32_t>(oss.str().size()) + 1;
    if (ceil(value) == floor(value)) {
        int appendLength = 2;
        *outLen = *outLen + appendLength;
        oss << ".0";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, (oss.str()).c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *CastDoubleToStringRetNull(int64_t contextPtr, double value, int32_t *outLen, bool *isNull)
{
    int precision = 15;

    ostringstream oss;
    oss.precision(precision);
    oss << value;
    *outLen = static_cast<int32_t>(oss.str().size()) + 1;
    if (ceil(value) == floor(value)) {
        int appendLength = 2;
        *outLen = *outLen + appendLength;
        oss << ".0";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, (oss.str()).c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *CastDecimal64ToString(int64_t contextPtr, int64_t x, int32_t precision,
    int32_t scale, int32_t *outLen)
{
    string str = DecimalOperations::Decimal64ToString(x, scale);
    *outLen = static_cast<int32_t>(str.size()) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal64ToStringRetNull(int64_t contextPtr, int64_t x, int32_t precision,
    int32_t scale, int32_t *outLen, bool *isNull)
{
    string str = DecimalOperations::Decimal64ToString(x, scale);
    *outLen = static_cast<int32_t>(str.size()) + 1;
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

extern DLLEXPORT const char *CastDecimal128ToString(int64_t contextPtr, int64_t high, uint64_t low, int32_t precision,
    int32_t scale, int32_t *outLen)
{
    Decimal128 inputDecimal(high, low);
    string stringDecimal = DecimalOperations::Decimal128ToString(inputDecimal.ToString(), scale);
    *outLen = static_cast<int32_t>(stringDecimal.length() + 1);
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, stringDecimal.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return "";
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal128ToStringRetNull(int64_t contextPtr, int64_t high, uint64_t low,
    int32_t precision, int32_t scale, int32_t *outLen, bool *isNull)
{
    Decimal128 inputDecimal(high, low);
    string stringDecimal = DecimalOperations::Decimal128ToString(inputDecimal.ToString(), scale);
    *outLen = static_cast<int32_t>(stringDecimal.length() + 1);
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, stringDecimal.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        return "";
    }
    *isNull = false;
    return ret;
}

// Cast string to numeric type
extern DLLEXPORT int32_t CastStringToInt(int64_t contextPtr, const char *str, int32_t strLen)
{
    int32_t result;
    regex r("([+-])?[[:digit:]]+");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }

    try {
        result = stoi(s);
    } catch (std::exception &e) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }

    return result;
}

extern DLLEXPORT int32_t CastStringToIntRetNull(const char *str, int32_t strLen, bool *isNull)
{
    int32_t result;
    regex r("([+-])?[[:digit:]]+");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        *isNull = true;
        return 0;
    }

    try {
        result = stoi(s);
    } catch (std::exception &e) {
        *isNull = true;
        return 0;
    }

    *isNull = false;
    return result;
}

extern DLLEXPORT int64_t CastStringToLong(int64_t contextPtr, const char *str, int32_t strLen)
{
    int64_t result;
    regex r("([+-])?[[:digit:]]+");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }

    try {
        result = stol(s);
    } catch (std::exception &e) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }

    return result;
}

extern DLLEXPORT int64_t CastStringToLongRetNull(const char *str, int32_t strLen, bool *isNull)
{
    int64_t result;
    regex r("([+-])?[[:digit:]]+");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        *isNull = true;
        return 0;
    }

    try {
        result = stol(s);
    } catch (std::exception &e) {
        *isNull = true;
        return 0;
    }

    *isNull = false;
    return result;
}

extern DLLEXPORT double CastStringToDouble(int64_t contextPtr, const char *str, int32_t strLen)
{
    double result;
    regex r("([+-])?[[:digit:]]+(.[[:digit:]]+)?");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to DOUBLE";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }

    try {
        result = stod(s);
    } catch (std::exception &e) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to DOUBLE. Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern DLLEXPORT double CastStringToDoubleRetNull(const char *str, int32_t strLen, bool *isNull)
{
    double result;
    regex r("([+-])?[[:digit:]]+(.[[:digit:]]+)?");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        *isNull = true;
        return 0;
    }

    try {
        result = stod(s);
    } catch (std::exception &e) {
        *isNull = true;
        return 0;
    }
    *isNull = false;
    return result;
}

extern DLLEXPORT int64_t CastStringToDecimal64(int64_t contextPtr, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale)
{
    string s = string(str, strLen);
    int precision = 0;
    int scale = 0;
    int64_t result = 0;
    OpStatus status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale
              << ")";
        if (status == OP_OVERFLOW) {
            errorMessage << ". Value too large.";
        }
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    status = DecimalOperations::Rescale64(result, outScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale
              << "). Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern DLLEXPORT int64_t CastStringToDecimal64RetNull(const char *str, int32_t strLen, int32_t outPrecision,
    int32_t outScale, bool *isNull)
{
    string s = string(str, strLen);
    int precision = 0;
    int scale = 0;
    int64_t result = 0;
    OpStatus status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    status = DecimalOperations::Rescale64(result, outScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    *isNull = false;
    return result;
}

extern DLLEXPORT void CastStringToDecimal128(int64_t contextPtr, const char *str, int32_t strLen, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    string s = string(str, strLen);
    Decimal128 result;
    int precision = 0;
    int scale = 0;
    OpStatus status = DecimalOperations::StringToDecimal128(s, result, scale, precision);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale
              << ")";
        if (status == OP_OVERFLOW) {
            errorMessage << ". Value too large.";
        }
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    status = DecimalOperations::Rescale128(result, outScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale
              << "). Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern DLLEXPORT void CastStringToDecimal128RetNull(const char *str, int32_t strLen, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr, bool *isNull)
{
    string s = string(str, strLen);
    Decimal128 result;
    int precision = 0;
    int scale = 0;
    OpStatus status = DecimalOperations::StringToDecimal128(s, result, scale, precision);
    if (status != SUCCESS) {
        *isNull = true;
        return;
    }
    status = DecimalOperations::Rescale128(result, outScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return;
    }
    *isNull = false;
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}