/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <string>
#include <cstring>
#include <regex>
#include <cmath>
#include <huawei_secure_c/include/securec.h>
#include "context_helper.h"
#include "codegen/functions/decimalfunctions.h"
#include "util/engine.h"
#include "stringfunctions.h"

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

extern DLLEXPORT bool LikeStr(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen, bool isNull)
{
    if (isNull) {
        return false;
    }
    string s = string(str, strLen);
    string r = string(regexToMatch, regexLen);

    wregex re(StringUtil::ToWideString(r));
    return regex_match(StringUtil::ToWideString(s), re);
}

extern DLLEXPORT bool LikeChar(const char *str, int32_t strWidth, int32_t strLen, const char *regexToMatch,
    int32_t regexLen, bool isNull)
{
    int32_t paddingCount = strWidth - omniruntime::Utf8Util::CountCodePoints(str, strLen);
    string originalStr;
    originalStr.reserve(strLen + paddingCount);
    originalStr.append(str, strLen);
    for (int i = 0; i < paddingCount; i++) {
        originalStr.append(" ");
    }
    string r = string(regexToMatch, regexLen);
    wregex re(StringUtil::ToWideString(r));
    return regex_match(StringUtil::ToWideString(originalStr), re);
}

extern DLLEXPORT const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    const char *ret = StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    bool hasErr = false;
    const char *ret = StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    bool hasErr = false;
    const char *ret = StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    const char *ret = StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT int32_t CastStringToDate(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
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

extern DLLEXPORT const char *ToUpperStr(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    for (int32_t i = 0; i < strLen; i++) {
        auto currItem = *(str + i);
        if (currItem >= static_cast<int>('a') && currItem <= static_cast<int>('z')) {
            *(ret + i) = static_cast<char>(currItem - STEP);
        } else {
            *(ret + i) = currItem;
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ToUpperStr(contextPtr, str, strLen, isNull, outLen);
}

extern DLLEXPORT const char *ToLowerStr(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    for (int32_t i = 0; i < strLen; i++) {
        auto currItem = *(str + i);
        if (currItem >= static_cast<int>('A') && currItem <= static_cast<int>('Z')) {
            *(ret + i) = static_cast<char>(currItem + STEP);
        } else {
            *(ret + i) = currItem;
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToLowerChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ToLowerStr(contextPtr, str, strLen, isNull, outLen);
}

extern DLLEXPORT int64_t LengthChar(const char *str, int32_t width, int32_t strLen, bool isNull)
{
    return isNull ? 0 : width;
}

extern DLLEXPORT int32_t LengthCharReturnInt32(const char *str, int32_t width, int32_t strLen, bool isNull)
{
    return isNull ? 0 : width;
}

extern DLLEXPORT int32_t LengthStrReturnInt32(const char *str, int32_t strLen, bool isNull)
{
    return isNull ? 0 : omniruntime::Utf8Util::CountCodePoints(str, strLen);
}

extern DLLEXPORT int64_t LengthStr(const char *str, int32_t strLen, bool isNull)
{
    return isNull ? 0 : omniruntime::Utf8Util::CountCodePoints(str, strLen);
}

extern DLLEXPORT const char *ReplaceStrStrStrWithRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    char *ret;
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (searchLen == 0 && engineType == EngineType::Spark) {
        *outLen = strLen;
        ret = const_cast<char *>(str);
    } else if (searchLen == 0) {
        auto result =
            StringUtil::ReplaceWithSearchEmpty(contextPtr, str, strLen, replaceStr, replaceLen, &hasErr, outLen);
        ret = (const_cast<char *>(result));
    } else {
        auto result = StringUtil::ReplaceWithSearchNotEmpty(contextPtr, str, strLen, searchStr, searchLen, replaceStr,
            replaceLen, &hasErr, outLen);
        ret = const_cast<char *>(result);
    }

    if (hasErr) {
        SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
    }
    return ret;
}

extern DLLEXPORT const char *ReplaceStrStrWithoutRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ReplaceStrStrStrWithRep(contextPtr, str, strLen, searchStr, searchLen, "", 0, isNull, outLen);
}

// Cast numeric type to string
extern DLLEXPORT const char *CastIntToString(int64_t contextPtr, int32_t value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    string str = to_string(value);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastLongToString(int64_t contextPtr, int64_t value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    string str = to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str()));
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDoubleToString(int64_t contextPtr, double value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    int precision = 15;

    ostringstream oss;
    oss.precision(precision);
    oss << value;
    *outLen = static_cast<int32_t>(oss.str().size());
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
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal64ToString(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    string str = DecimalOperations::ScaleOfDecimal(to_string(x), scale);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal128ToString(int64_t contextPtr, int64_t high, uint64_t low, int32_t precision,
    int32_t scale, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    Decimal128 inputDecimal(high, low);
    string stringDecimal = DecimalOperations::ScaleOfDecimal(inputDecimal.ToString(), scale);
    *outLen = static_cast<int32_t>(stringDecimal.length());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, stringDecimal.c_str(), *outLen);
    if (res != EOK) {
        char message[] = "cast failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastStrWithDiffWidths(int64_t contextPtr, const char *srcStr, int32_t srcLen,
    int32_t srcWidth, bool isNull, int32_t dstWidth, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    bool hasErr = false;
    const char *ret = StringUtil::CastStrStr(&hasErr, srcStr, srcWidth, srcLen, outLen, dstWidth);
    if (hasErr) {
        ostringstream errMsg;
        errMsg << "cast varchar[" << srcWidth << "] to varchar[" << dstWidth << "] failed.";
        SetError(contextPtr, errMsg.str().c_str(), errMsg.str().length());
    }
    return ret;
}

// Cast string to numeric type
extern DLLEXPORT int32_t CastStringToInt(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    int32_t result;
    regex r("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value is not a number.";
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

extern DLLEXPORT int64_t CastStringToLong(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    int64_t result;
    regex r("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value is not a number.";
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

extern DLLEXPORT double CastStringToDouble(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    double result;
    regex r("[[:blank:]]*([+-])?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?[[:blank:]]*");
    string s = string(str, strLen);
    if (!regex_match(s, r)) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to DOUBLE. Value is not a number.";
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

extern DLLEXPORT int64_t CastStringToDecimal64(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t outPrecision, int32_t outScale)
{
    if (isNull) {
        return 0;
    }
    string s = string(str, strLen);
    int precision = 0;
    int scale = 0;
    int64_t result = 0;
    OpStatus status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale << ")";
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
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale <<
            "). Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern DLLEXPORT void CastStringToDecimal128(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (isNull) {
        return;
    }
    string s = string(str, strLen);
    Decimal128 result;
    int precision = 0;
    int scale = 0;
    OpStatus status = DecimalOperations::StringToDecimal128(s, result, scale, precision);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale << ")";
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
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale <<
            "). Value too large.";
        int len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern DLLEXPORT const char *ConcatStrStrRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, isNull, outLen);
}

extern DLLEXPORT const char *ConcatCharCharRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t aWidth,
    int32_t apLen, const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, isNull, outLen);
}

extern DLLEXPORT const char *ConcatCharStrRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t aWidth,
    int32_t apLen, const char *bp, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, isNull, outLen);
}

extern DLLEXPORT const char *ConcatStrCharRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, isNull, outLen);
}

extern DLLEXPORT int32_t CastStringToDateRetNull(bool *isNull, const char *str, int32_t strLen)
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

extern DLLEXPORT const char *CastIntToStringRetNull(int64_t contextPtr, bool *isNull, int32_t value, int32_t *outLen)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastLongToStringRetNull(int64_t contextPtr, bool *isNull, int64_t value, int32_t *outLen)
{
    string str = to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str()));
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDoubleToStringRetNull(int64_t contextPtr, bool *isNull, double value, int32_t *outLen)
{
    int precision = 15;

    ostringstream oss;
    oss.precision(precision);
    oss << value;
    *outLen = static_cast<int32_t>(oss.str().size());
    if (ceil(value) == floor(value)) {
        int appendLength = 2;
        *outLen = *outLen + appendLength;
        oss << ".0";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, (oss.str()).c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal64ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t x,
    int32_t precision, int32_t scale, int32_t *outLen)
{
    string str = DecimalOperations::ScaleOfDecimal(to_string(x), scale);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *CastDecimal128ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t high, uint64_t low,
    int32_t precision, int32_t scale, int32_t *outLen)
{
    Decimal128 inputDecimal(high, low);
    string stringDecimal = DecimalOperations::ScaleOfDecimal(inputDecimal.ToString(), scale);
    *outLen = static_cast<int32_t>(stringDecimal.length());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, stringDecimal.c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT int32_t CastStringToIntRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int32_t result;
    regex r("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*");
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

    return result;
}

extern DLLEXPORT int64_t CastStringToLongRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int64_t result;
    regex r("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*");
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

    return result;
}

extern DLLEXPORT double CastStringToDoubleRetNull(bool *isNull, const char *str, int32_t strLen)
{
    double result;
    std::regex r("[[:blank:]]*([+-])?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?[[:blank:]]*");
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
    return result;
}

extern DLLEXPORT int64_t CastStringToDecimal64RetNull(bool *isNull, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale)
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
    return result;
}

extern DLLEXPORT void CastStringToDecimal128RetNull(bool *isNull, const char *str, int32_t strLen, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
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
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT const char *CastStrWithDiffWidthsRetNull(int64_t contextPtr, bool *isNull, const char *srcStr,
    int32_t srcLen, int32_t srcWidth, int32_t dstWidth, int32_t *outLen)
{
    return StringUtil::CastStrStr(isNull, srcStr, srcWidth, srcLen, outLen, dstWidth);
}
