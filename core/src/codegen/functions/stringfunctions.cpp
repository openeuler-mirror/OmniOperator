/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: registry  function  implementation
 */
#include "stringfunctions.h"
#include "md5.h"
#include "dtoa.h"
#include "type/string_Impl.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT int64_t CountChar(const char *str, int32_t strLen, const char *target, int32_t targetWidth, int32_t targetLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    char chr = target[0];
    int64_t count = std::count(str, str + strLen, chr);
    return count;
}

extern "C" DLLEXPORT const char* SplitIndexRetNull(const char *str, int32_t strLen, bool strIsNull, const char *target,
                                                   int32_t targetWidth, int32_t targetLen, bool targetIsNull, int32_t index,
                                                   bool indexIsNull, bool *outIsNull, int32_t *outLen)
{
    if (strIsNull || targetIsNull || indexIsNull) {
        *outIsNull = true;
        *outLen = 0;
        return nullptr;
    }
    size_t start = 0;
    size_t currentIndex = 0;

    for (size_t i = 0; i <= strLen; ++i) {
        if (i == strLen || str[i] == *target) {
            if (currentIndex == index) {
                *outIsNull = false;
                *outLen = i - start;
                return str + start;
            }
            start = i + 1;
            ++currentIndex;
        }
    }
    *outIsNull = true;
    *outLen = 0;
    return nullptr;
}

/**
 * This function is only called when apLen is equal to bpLen. When apLen and bpLen are different,
 * it will directly return false instead of calling StrEquals.
 */
extern "C" DLLEXPORT bool StrEquals(const char *ap, int32_t apLen, const char *bp, int32_t bpLen)
{
    for (int i = 0; i < apLen; ++i) {
        if (ap[i] != bp[i]) {
            return false;
        }
    }
    return true;
}

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

extern "C" DLLEXPORT const char* RegexpExtractRetNull(int64_t contextPtr, const char *str, int32_t strLen, bool strIsNull,
                                                      const char *regexToMatch, int32_t regexWidth, int32_t regexLen,
                                                      bool regexIsNull, int32_t group, bool groupIsNull, bool *outIsNull, int32_t *outLen)
{
    if (strIsNull || regexIsNull || groupIsNull) {
        *outIsNull = true;
        *outLen = 0;
        return nullptr;
    }
    std::string s = std::string(str, strLen);
    std::string r = std::string(regexToMatch, regexLen);

    std::wregex re(StringUtil::ToWideString(r));
    std::wstring ws = StringUtil::ToWideString(s);
    std::wsmatch match; // Wide string match results

    if (std::regex_search(ws, match, re) && match.size() > group) {
        int startIdx = match.position(group); // Get start position of group 2
        *outLen = match.length(group);
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        memcpy(ret, str + startIdx, *outLen + 1);
        return ret;
    } else {
        *outIsNull = true;
        *outLen = 0;
        return nullptr;
    }
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

extern "C" DLLEXPORT const char *ConcatWsStr(int64_t contextPtr, const char *separator, int32_t separatorLen,
    bool separatorIsNull, const char *ap, int32_t apLen, const char *bp, int32_t bpLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    if (separatorIsNull) {
        *outLen = 0;
        isNull = true;
        return nullptr;
    }

    bool hasErr = false;
    const char *ret = StringUtil::ConcatWsStrDiffWidths(contextPtr, separator, separatorLen, ap, apLen, bp, bpLen, &hasErr, outLen);
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
    int64_t result = 0;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_dateRegex)) {
        SetError(contextPtr, "Only support cast date\'YYYY-MM-DD\' to integer");
        return -1;
    }
    if (Date32::StringToDate32(str, strLen, result) != Status::CONVERT_SUCCESS) {
        SetError(contextPtr, "Value cannot be cast to date: " + std::string(str, strLen));
        return -1;
    }
    return static_cast<int32_t >(result);
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
    int64_t result = 0;
    if (Date32::StringToDate32(str, strLen, result) != Status::CONVERT_SUCCESS) {
        SetError(contextPtr, "Value cannot be cast to date: " + std::string(str, strLen));
        return -1;
    }
    return static_cast<int32_t >(result);
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
    memcpy(ret, str.c_str(), *outLen);
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
    memcpy(ret, str.c_str(), *outLen);
    return ret;
}

extern "C" DLLEXPORT const char *CastDoubleToString(int64_t contextPtr, double value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    auto ret = ArenaAllocatorMalloc(contextPtr, MAX_DATA_LENGTH);
    *outLen = static_cast<int32_t >(DoubleToString::DoubleToStringConverter(value, ret));
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
    memcpy(ret, str.c_str(), *outLen);
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
    memcpy(ret, stringDecimal.c_str(), *outLen);
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
    Status status = ConvertStringToInteger<int32_t, false>(result, str, strLen);
    if (status == Status::IS_NOT_A_NUMBER) {
        std::string s(str, strLen);
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    if (status == Status::CONVERT_OVERFLOW) {
        std::string s(str, strLen);
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to INTEGER. Value too large or too small.";
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
    Status status = ConvertStringToInteger<int64_t, false>(result, str, strLen);
    if (status == Status::IS_NOT_A_NUMBER) {
        std::string s = std::string(str, strLen);
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }

    if (status == Status::CONVERT_OVERFLOW) {
        std::string s = std::string(str, strLen);
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << s << "' to BIGINT. Value too large or too small.";
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
    Status status = ConvertStringToDouble(result, str, strLen);
    if (status == Status::IS_NOT_A_NUMBER) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << std::string(str, strLen) << "' to DOUBLE. Value is not a number.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    if (status == Status::CONVERT_OVERFLOW) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast '" << std::string(str, strLen) << "' to DOUBLE. Value is not a number.";
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

extern "C" DLLEXPORT int64_t CastStringToDecimal64RoundUp(int64_t contextPtr, const char *str, int32_t strLen,
    bool isNull, int32_t outPrecision, int32_t outScale)
{
    if (isNull) {
        return 0;
    }
    std::string s = std::string(str, strLen);
    Decimal64<true> result(s);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) == OpStatus::OP_OVERFLOW) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << std::string(str, strLen) << "' to DECIMAL(" << outPrecision <<
            ", " << outScale << "). Value too large.";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    if (result.IsOverflow(outPrecision) == OpStatus::FAIL) {
        std::ostringstream errorMessage;
        errorMessage << "Cannot cast VARCHAR '" << s << "' to DECIMAL(" << outPrecision << ", " << outScale <<
            "). Value is not a number.";
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

extern "C" DLLEXPORT void CastStringToDecimal128RoundUp(int64_t contextPtr, const char *str, int32_t strLen,
    bool isNull, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
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
    Decimal128Wrapper<true> result(s.c_str());
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
    int64_t result = 0;
    std::string s(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_dateRegex)) {
        *isNull = true;
        return -1;
    }
    if (Date32::StringToDate32(str, strLen, result) != Status::CONVERT_SUCCESS) {
        *isNull = true;
        return -1;
    }
    return static_cast<int32_t >(result);
}

extern "C" DLLEXPORT int32_t CastStringToDateRetNullAllowReducePrecison(bool *isNull, const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int64_t result = 0;
    if (Date32::StringToDate32(str, strLen, result) != Status::CONVERT_SUCCESS) {
        *isNull = true;
        return -1;
    }
    return static_cast<int32_t >(result);
}

extern "C" DLLEXPORT const char *CastIntToStringRetNull(int64_t contextPtr, bool *isNull, int32_t value,
    int32_t *outLen)
{
    std::string str = std::to_string(value);
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    memcpy(ret, str.c_str(), *outLen);
    return ret;
}

extern "C" DLLEXPORT const char *CastLongToStringRetNull(int64_t contextPtr, bool *isNull, int64_t value,
    int32_t *outLen)
{
    std::string str = std::to_string(value);
    *outLen = static_cast<int32_t>(strlen(str.c_str()));
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    memcpy(ret, str.c_str(), *outLen);
    return ret;
}

extern "C" DLLEXPORT const char *CastDoubleToStringRetNull(int64_t contextPtr, bool *isNull, double value,
    int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, MAX_DATA_LENGTH);
    *outLen = static_cast<int32_t >(DoubleToString::DoubleToStringConverter(value, ret));
    return ret;
}

extern "C" DLLEXPORT const char *CastDecimal64ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t x,
    int32_t precision, int32_t scale, int32_t *outLen)
{
    std::string str = Decimal64(x).SetScale(scale).ToString();
    *outLen = static_cast<int32_t>(str.size());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    memcpy(ret, str.c_str(), *outLen);
    return ret;
}

extern "C" DLLEXPORT const char *CastDecimal128ToStringRetNull(int64_t contextPtr, bool *isNull, int64_t high,
    uint64_t low, int32_t precision, int32_t scale, int32_t *outLen)
{
    Decimal128Wrapper inputDecimal(high, low);
    std::string stringDecimal = inputDecimal.SetScale(scale).ToString();
    *outLen = static_cast<int32_t>(stringDecimal.length());
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    memcpy(ret, stringDecimal.c_str(), *outLen);
    return ret;
}

extern "C" DLLEXPORT int32_t CastStringToIntRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int32_t result = 0;
    Status status = ConvertStringToInteger<int32_t>(result, str, strLen);
    *isNull = status != Status::CONVERT_SUCCESS;
    return result;
}

extern "C" DLLEXPORT int64_t CastStringToLongRetNull(bool *isNull, const char *str, int32_t strLen)
{
    int64_t result = 0;
    Status status = ConvertStringToInteger<int64_t>(result, str, strLen);
    *isNull = status != Status::CONVERT_SUCCESS;
    return result;
}

extern "C" DLLEXPORT double CastStringToDoubleRetNull(bool *isNull, const char *str, int32_t strLen)
{
    double result;
    Status status = ConvertStringToDouble(result, str, strLen);
    if (status != Status::CONVERT_SUCCESS) {
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

extern "C" DLLEXPORT int64_t CastStringToDecimal64RoundUpRetNull(bool *isNull, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale)
{
    std::string s = std::string(str, strLen);
    Decimal64<true> result(std::string(str, strLen));
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

extern "C" DLLEXPORT void CastStringToDecimal128RoundUpRetNull(bool *isNull, const char *str, int32_t strLen,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    std::string s = std::string(str, strLen);
    StringUtil::TrimString(s);
    if (!regex_match(s, g_decimalRegex)) {
        *isNull = true;
        return;
    }
    Decimal128Wrapper<true> result(s.c_str());
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

extern "C" DLLEXPORT int32_t InStr(const char *srcStr, int32_t srcLen, const char *subStr, int32_t subLen, bool isNull)
{
    // currently return 0 if not found that means 1-based
    if (isNull || subLen > srcLen) {
        return 0;
    }
    if (subLen == 0) {
        return 1;
    }

    int32_t tailPos = srcLen - subLen;
    int32_t cmpLen = subLen - 1;
    for (int32_t pos = 0; pos <= tailPos; ++pos) {
        if (srcStr[pos] == subStr[0] && memcmp(srcStr + pos + 1, subStr + 1, cmpLen) == 0) {
            auto result = omniruntime::Utf8Util::CountCodePoints(srcStr, pos);
            return (result + 1);
        }
    }
    return 0;
}

extern "C" DLLEXPORT bool StartsWithStr(const char *srcStr, int32_t srcLen, const char *matchStr, int32_t matchLen,
    bool isNull)
{
    if (isNull || matchLen > srcLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return memcmp(srcStr, matchStr, matchLen) == 0;
}

extern "C" DLLEXPORT bool EndsWithStr(const char *srcStr, int32_t srcLen, const char *matchStr, int32_t matchLen,
    bool isNull)
{
    if (isNull || matchLen > srcLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return memcmp(srcStr + srcLen - matchLen, matchStr, matchLen) == 0;
}

extern "C" DLLEXPORT bool RegexMatch(const char *srcStr, int32_t srcLen, const char *matchStr, int32_t matchLen,
    bool isNull)
{
    if (isNull) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    if (srcLen == 0) {
        return false;
    }
    for (int32_t i = 0; i < srcLen; i++) {
        char c = srcStr[i];
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

extern "C" DLLEXPORT const char *CastDateToStringRetNull(int64_t contextPtr, bool *isNull, int32_t value,
    int32_t *outLen)
{
    Date32 date(value);
    auto ret = ArenaAllocatorMalloc(contextPtr, MAX_DAY_ONLY_LENGTH);
    *outLen = static_cast<int32_t>(date.ToString(ret, MAX_DAY_ONLY_LENGTH));
    return ret;
}

extern "C" DLLEXPORT const char *CastDateToString(int64_t contextPtr, int32_t value, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    Date32 date(value);
    auto ret = ArenaAllocatorMalloc(contextPtr, MAX_DAY_ONLY_LENGTH);
    *outLen = static_cast<int32_t>(date.ToString(ret, MAX_DAY_ONLY_LENGTH));
    return ret;
}

extern "C" DLLEXPORT char *Md5Str(int64_t contextPtr, const char *str, int32_t len, bool isNull, int32_t *outLen)
{
    if (isNull) {
        return nullptr;
    }
    Md5Function md5(str, len);
    *outLen = 32;
    char *mdString = ArenaAllocatorMalloc(contextPtr, *outLen);
    md5.FinishHex(mdString);
    return mdString;
}

extern "C" DLLEXPORT bool ContainsStr(const char *srcStr, int32_t srcLen, const char *matchStr, int32_t matchLen,
    bool isNull)
{
    if (isNull || matchLen > srcLen) {
        return false;
    }
    if (matchLen == 0) {
        return true;
    }
    return StringUtil::StrContainsStr(srcStr, srcLen, matchStr, matchLen);
}

extern "C" DLLEXPORT const char *GreatestStr(const char *lValue, int32_t lLen, bool lIsNull, const char *rValue,
    int32_t rLen, bool rIsNull, bool *retIsNull, int32_t *outLen)
{
    if (lIsNull && rIsNull) {
        *retIsNull = true;
        *outLen = 0;
        return nullptr;
    }
    if (lIsNull) {
        *outLen = rLen;
        return rValue;
    }
    if (!rIsNull) {
        int32_t cmpRet = memcmp(lValue, rValue, std::min(lLen, rLen));
        if (cmpRet < 0 || (cmpRet == 0 && rLen > lLen)) {
            *outLen = rLen;
            return rValue;
        }
    }
    *outLen = lLen;
    return lValue;
}

extern "C" DLLEXPORT const char *EmptyToNull(const char *str, int32_t len, bool isNull, int32_t *outLen)
{
    if (len == 0 || isNull) {
        *outLen = 0;
        return nullptr;
    }

    *outLen = len;
    return str;
}

extern "C" DLLEXPORT const char *StaticInvokeVarcharTypeWriteSideCheck(int64_t contextPtr, const char *str, int32_t len,
    int32_t limit, bool isNull, int32_t *outLen)
{
    if (isNull) {
        *outLen = 0;
        return nullptr;
    }
    int32_t ssLen = StringUtil::NumChars(str, len);
    if (ssLen <= limit) {
        *outLen = len;
        return str;
    }
    int32_t numTailSpacesToTrim = ssLen - limit;
    int32_t endIdx = len - 1;
    int32_t trimTo = len - numTailSpacesToTrim;
    while (endIdx >= trimTo && str[endIdx] == 0x20) {
        endIdx--;
    }
    int32_t outByteNum = endIdx + 1;
    ssLen = StringUtil::NumChars(str, outByteNum);
    if (ssLen > limit) {
        std::ostringstream errorMessage;
        errorMessage << "Exceeds varchar type length limitation: " << limit;
        SetError(contextPtr, errorMessage.str());
        *outLen = 0;
        return nullptr;
    }

    auto padded = ArenaAllocatorMalloc(contextPtr, outByteNum);
    memcpy(padded, str, outByteNum);
    padded[outByteNum] = '\0';
    *outLen = outByteNum;
    return padded;
}

extern "C" DLLEXPORT const char *StaticInvokeCharReadPadding(int64_t contextPtr, const char *str, int32_t len,
    int32_t limit, bool isNull, int32_t *outLen)
{
    if (isNull) {
        *outLen = 0;
        return nullptr;
    } else if (len == 0) {
        *outLen = 0;
        return "";
    }
    int32_t ssLen = StringUtil::NumChars(str, len);
    if (ssLen >= limit) {
        *outLen = len;
        return str;
    }
    int32_t diff = limit - ssLen;
    int32_t outByteNum = len + diff + 1;
    auto padded = ArenaAllocatorMalloc(contextPtr, outByteNum);
    memcpy(padded, str, len);
    memset(padded + len, ' ', diff);
    padded[outByteNum] = '\0';
    *outLen = outByteNum - 1;
    return padded;
}

extern "C" DLLEXPORT const char *SubstringIndex(int64_t contextPtr, const char *str, int32_t strLen, const char *delim,
    int32_t delimLen, int32_t count, bool isNull, int32_t *outLen)
{
    if (count == 0 || isNull) {
        *outLen = 0;
        return nullptr;
    }

    int64_t index;
    if (count > 0) {
        index = stringImpl::StringPosition<true, true>(std::string_view(str, strLen), std::string_view(delim, delimLen),
            count);
    } else {
        index = stringImpl::StringPosition<true, false>(std::string_view(str, strLen),
            std::string_view(delim, delimLen), -count);
    }

    // If 'delim' is not found or found fewer than 'count' times,
    // return the input string directly.
    if (index == 0) {
        auto result = ArenaAllocatorMalloc(contextPtr, strLen);
        memcpy(result, str, strLen);
        *outLen = strLen;
        return result;
    }

    auto start = 0;
    auto length = strLen;
    const auto delimLength = delimLen;
    if (count > 0) {
        length = index - 1;
    } else {
        start = index + delimLength - 1;
        length -= start;
    }

    auto result = ArenaAllocatorMalloc(contextPtr, length);
    memcpy(result, str + start, length);
    *outLen = length;
    return result;
}
}

