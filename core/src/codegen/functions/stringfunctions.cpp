/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include "stringfunctions.h"

namespace omniruntime::codegen::function {

extern "C" DLLEXPORT int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen)
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

extern "C" DLLEXPORT bool LikeStr(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen,
    bool isNull)
{
    if (isNull) {
        return false;
    }
    std::string s = std::string(str, strLen);
    std::string r = std::string(regexToMatch, regexLen);

    std::wregex re(StringUtil::ToWideString(r));
    return regex_match(StringUtil::ToWideString(s), re);
}

extern "C" DLLEXPORT bool LikeChar(const char *str, int32_t strWidth, int32_t strLen, const char *regexToMatch,
    int32_t regexLen, bool isNull)
{
    int32_t paddingCount = strWidth - omniruntime::Utf8Util::CountCodePoints(str, strLen);
    std::string originalStr;
    originalStr.reserve(strLen + paddingCount);
    originalStr.append(str, strLen);
    for (int i = 0; i < paddingCount; i++) {
        originalStr.append(" ");
    }
    std::string r = std::string(regexToMatch, regexLen);
    std::wregex re(StringUtil::ToWideString(r));
    return regex_match(StringUtil::ToWideString(originalStr), re);
}

extern "C" DLLEXPORT const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    const char *ret = StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG);
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    bool hasErr = false;
    const char *ret = StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG);
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    bool hasErr = false;
    const char *ret = StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG);
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    const char *ret = StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, &hasErr, outLen);
    if (hasErr) {
        SetError(contextPtr, CONCAT_ERR_MSG);
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT int32_t CastStringToDateNotAllowReducePrecison(int64_t contextPtr, const char *str, int32_t strLen,
    bool isNull)
{
    if (isNull) {
        return 0;
    }
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t result;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_dateRegex)) {
        SetError(contextPtr, "Only support cast date\'YYYY-MM-DD\' to integer");
        return -1;
    }
    if (Date32::StringToDate32(str, strLen, result) == -1) {
        SetError(contextPtr, "Value cannot be cast to date: " + std::string(str, strLen));
        return -1;
    }
    return result;
}

extern "C" DLLEXPORT int32_t CastStringToDateAllowReducePrecison(int64_t contextPtr, const char *str, int32_t strLen,
    bool isNull)
{
    if (isNull) {
        return 0;
    }
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t result;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (Date32::StringToDate32(str, strLen, result) == -1) {
        SetError(contextPtr, "Value cannot be cast to date: " + std::string(str, strLen));
        return -1;
    }
    return result;
}

extern "C" DLLEXPORT const char *ToUpperStr(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
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

extern "C" DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ToUpperStr(contextPtr, str, strLen, isNull, outLen);
}

extern "C" DLLEXPORT const char *ToLowerStr(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
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

extern "C" DLLEXPORT const char *ToLowerChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ToLowerStr(contextPtr, str, strLen, isNull, outLen);
}

extern "C" DLLEXPORT int64_t LengthChar(const char *str, int32_t width, int32_t strLen, bool isNull)
{
    return isNull ? 0 : width;
}

extern "C" DLLEXPORT int32_t LengthCharReturnInt32(const char *str, int32_t width, int32_t strLen, bool isNull)
{
    return isNull ? 0 : width;
}

extern "C" DLLEXPORT int32_t LengthStrReturnInt32(const char *str, int32_t strLen, bool isNull)
{
    return isNull ? 0 : omniruntime::Utf8Util::CountCodePoints(str, strLen);
}

extern "C" DLLEXPORT int64_t LengthStr(const char *str, int32_t strLen, bool isNull)
{
    return isNull ? 0 : omniruntime::Utf8Util::CountCodePoints(str, strLen);
}

extern "C" DLLEXPORT const char *ReplaceStrStrStrWithRepNotReplace(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    char *ret;
    if (searchLen == 0) {
        *outLen = strLen;
        ret = const_cast<char *>(str);
    } else {
        auto result = StringUtil::ReplaceWithSearchNotEmpty(contextPtr, str, strLen, searchStr, searchLen, replaceStr,
            replaceLen, &hasErr, outLen);
        ret = const_cast<char *>(result);
    }

    if (hasErr) {
        SetError(contextPtr, REPLACE_ERR_MSG);
    }
    return ret;
}

extern "C" DLLEXPORT const char *ReplaceStrStrStrWithRepReplace(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }

    bool hasErr = false;
    char *ret;
    if (searchLen == 0) {
        auto result =
            StringUtil::ReplaceWithSearchEmpty(contextPtr, str, strLen, replaceStr, replaceLen, &hasErr, outLen);
        ret = (const_cast<char *>(result));
    } else {
        auto result = StringUtil::ReplaceWithSearchNotEmpty(contextPtr, str, strLen, searchStr, searchLen, replaceStr,
            replaceLen, &hasErr, outLen);
        ret = const_cast<char *>(result);
    }

    if (hasErr) {
        SetError(contextPtr, REPLACE_ERR_MSG);
    }
    return ret;
}

extern "C" DLLEXPORT const char *ReplaceStrStrWithoutRepNotReplace(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ReplaceStrStrStrWithRepNotReplace(contextPtr, str, strLen, searchStr, searchLen, "", 0, isNull, outLen);
}

extern "C" DLLEXPORT const char *ReplaceStrStrWithoutRepReplace(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    return ReplaceStrStrStrWithRepReplace(contextPtr, str, strLen, searchStr, searchLen, "", 0, isNull, outLen);
}

// Cast numeric type to std::string
extern "C" DLLEXPORT const char *CastIntToString(int64_t contextPtr, int32_t value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    std::string str = std::to_string(value);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        SetError(contextPtr, "cast failed");
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastLongToString(int64_t contextPtr, int64_t value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    std::string str = std::to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str()));
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        SetError(contextPtr, "cast failed");
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastDoubleToString(int64_t contextPtr, double value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    int precision = std::numeric_limits<double>::max_digits10;

    std::ostringstream errorMessage;
    errorMessage.precision(precision);
    errorMessage << value;
    *outLen = static_cast<int32_t>(errorMessage.str().size());
    if (ceil(value) == floor(value)) {
        int appendLength = 2;
        *outLen = *outLen + appendLength;
        errorMessage << ".0";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, (errorMessage.str()).c_str(), *outLen);
    if (res != EOK) {
        SetError(contextPtr, "cast failed");
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastDecimal64ToString(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    std::string str = Decimal64(x).SetScale(scale).ToString();
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
    if (res != EOK) {
        SetError(contextPtr, "cast failed");
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastDecimal128ToString(int64_t contextPtr, int64_t high, uint64_t low,
    int32_t precision, int32_t scale, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    std::string stringDecimal = Decimal128Wrapper(high, low).SetScale(scale).ToString();
    *outLen = static_cast<int32_t>(stringDecimal.length());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, stringDecimal.c_str(), *outLen);
    if (res != EOK) {
        SetError(contextPtr, "cast failed");
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
        std::ostringstream errorMessage;
        errorMessage << "cast varchar[" << srcWidth << "] to varchar[" << dstWidth << "] failed.";
        SetError(contextPtr, errorMessage.str());
    }
    return ret;
}

// Cast std::string to numeric type
extern "C" DLLEXPORT int32_t CastStringToInt(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    int32_t result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_intRegex)) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    try {
        result = stoi(s);
    } catch (std::exception &e) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value too large.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    return result;
}

extern "C" DLLEXPORT int64_t CastStringToLong(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    int64_t result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_intRegex)) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    try {
        result = stol(s);
    } catch (std::exception &e) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value too large.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    return result;
}

extern "C" DLLEXPORT double CastStringToDouble(int64_t contextPtr, const char *str, int32_t strLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    double result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_doubleRegex)) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to DOUBLE. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    try {
        result = stod(s);
    } catch (std::exception &e) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to DOUBLE. Value too large.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastStringToDecimal64(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t outPrecision, int32_t outScale)
{
    if (isNull) {
        return 0;
    }
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_decimalRegex)) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale <<
            "). Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    Decimal64 result(s);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << std::string(str, strLen) << "' to DECIMAL(" << outPrecision <<
            ", " << outScale << "). Value too large.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void CastStringToDecimal128(int64_t contextPtr, const char *str, int32_t strLen, bool isNull,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (isNull) {
        return;
    }
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_decimalRegex)) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale <<
            "). Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return;
    }
    Decimal128Wrapper result(s.c_str());
    result.ReScale(outScale);
    OpStatus status = result.IsOverflow(outPrecision);
    if (status != OpStatus::SUCCESS) {
        SetError(contextPtr, CastErrorMessage(OMNI_VARCHAR, OMNI_DECIMAL128, std::string(str, strLen).c_str(), status,
            outPrecision, outScale));
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT const char *ConcatStrStrRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, isNull, outLen);
}

extern "C" DLLEXPORT const char *ConcatCharCharRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t aWidth,
    int32_t apLen, const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, isNull, outLen);
}

extern "C" DLLEXPORT const char *ConcatCharStrRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t aWidth,
    int32_t apLen, const char *bp, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatCharDiffWidths(contextPtr, ap, aWidth, apLen, bp, bpLen, isNull, outLen);
}

extern "C" DLLEXPORT const char *ConcatStrCharRetNull(int64_t contextPtr, bool *isNull, const char *ap, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    return StringUtil::ConcatStrDiffWidths(contextPtr, ap, apLen, bp, bpLen, isNull, outLen);
}

extern "C" DLLEXPORT int32_t CastStringToDateRetNullNotAllowReducePrecison(bool *isNull, const char *str,
    int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t result;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_dateRegex)) {
        *isNull = true;
        return -1;
    }
    if (Date32::StringToDate32(str, strLen, result) == -1) {
        *isNull = true;
        return -1;
    }
    return result;
}

extern "C" DLLEXPORT int32_t CastStringToDateRetNullAllowReducePrecison(bool *isNull, const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t result;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (Date32::StringToDate32(str, strLen, result) == -1) {
        *isNull = true;
        return -1;
    }
    return result;
}

extern "C" DLLEXPORT const char *CastIntToStringRetNull(int64_t contextPtr, bool *isNull, int32_t value,
    int32_t *outLen)
{
    std::string str = std::to_string(value);
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

extern "C" DLLEXPORT const char *CastLongToStringRetNull(int64_t contextPtr, bool *isNull, int64_t value,
    int32_t *outLen)
{
    std::string str = std::to_string(value);
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

extern "C" DLLEXPORT const char *CastDoubleToStringRetNull(int64_t contextPtr, bool *isNull, double value,
    int32_t *outLen)
{
    int precision = std::numeric_limits<double>::max_digits10;

    std::ostringstream errorMessage;
    errorMessage.precision(precision);
    errorMessage << value;
    *outLen = static_cast<int32_t>(errorMessage.str().size());
    if (ceil(value) == floor(value)) {
        int appendLength = 2;
        *outLen = *outLen + appendLength;
        errorMessage << ".0";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, (errorMessage.str()).c_str(), *outLen);
    if (res != EOK) {
        *isNull = true;
        *outLen = 0;
        return nullptr;
    }
    return ret;
}

extern "C" DLLEXPORT const char *CastDecimal64ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t x,
    int32_t precision, int32_t scale, int32_t *outLen)
{
    std::string str = Decimal64(x).SetScale(scale).ToString();
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

extern "C" DLLEXPORT const char *CastDecimal128ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t high,
    uint64_t low, int32_t precision, int32_t scale, int32_t *outLen)
{
    Decimal128Wrapper inputDecimal(high, low);
    std::string stringDecimal = inputDecimal.SetScale(scale).ToString();
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

extern "C" DLLEXPORT int32_t CastStringToIntRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int32_t result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_intRegex)) {
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

extern "C" DLLEXPORT int64_t CastStringToLongRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int64_t result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_intRegex)) {
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

extern "C" DLLEXPORT double CastStringToDoubleRetNull(bool *isNull, const char *str, int32_t strLen)
{
    double result;
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_doubleRegex)) {
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

extern "C" DLLEXPORT int64_t CastStringToDecimal64RetNull(bool *isNull, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale)
{
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_decimalRegex)) {
        *isNull = true;
        return 0;
    }
    Decimal64 result(std::string(str, strLen));
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void CastStringToDecimal128RetNull(bool *isNull, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_decimalRegex)) {
        *isNull = true;
        return;
    }
    Decimal128Wrapper result(s.c_str());
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
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
}
