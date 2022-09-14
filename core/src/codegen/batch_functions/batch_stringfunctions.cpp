/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch string functions implementation
 */

#include "batch_stringfunctions.h"
#include <iostream>
#include <string>
#include <cstring>
#include <regex>
#include <huawei_secure_c/include/securec.h>
#include <bits/basic_string.h>
#include "type/decimal128.h"
#include "type/decimal_operations.h"


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

extern DLLEXPORT void BatchLikeStr(uint8_t **str, int32_t *strLen, uint8_t **regexToMatch, int32_t *regexLen,
    bool *isAnyNull, bool *output, int32_t rowCnt)
{
    std::string s;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = false;
            continue;
        }
        s = std::string(reinterpret_cast<char *>(str[i]), strLen[i]);
        std::string r = std::string(reinterpret_cast<char *>(regexToMatch[i]), regexLen[i]);
        std::wregex re(ToWideString(r));
        output[i] = regex_match(ToWideString(s), re);
    }
}

extern DLLEXPORT void BatchLikeChar(uint8_t **str, int32_t strWidth, int32_t *strLen, uint8_t **regexToMatch,
    int32_t *regexLen, bool *isAnyNull, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = false;
            continue;
        }
        int32_t paddingCount =
            strWidth - omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
        std::string originalStr;
        originalStr.reserve(strLen[i] + paddingCount);
        originalStr.append(reinterpret_cast<char *>(str[i]), strLen[i]);
        for (int32_t count = 0; count < paddingCount; count++) {
            originalStr.append(" ");
        }
        std::string r = std::string(reinterpret_cast<char *>(regexToMatch[i]), regexLen[i]);
        std::wregex re(ToWideString(r));
        output[i] = regex_match(ToWideString(originalStr), re);
    }
}

extern DLLEXPORT void BatchConcatStrStr(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen,
    bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2;
    for (int i = 0; i < rowCnt; i++) {
        outLen[i] = apLen[i] + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        res1 = memcpy_s(ret, outLen[i] + 1, ap[i], apLen[i]);
        res2 = memcpy_s(ret + apLen[i], outLen[i] - apLen[i] + 1, bp[i], bpLen[i]);
        if (res1 != EOK || res2 != EOK) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatCharChar(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2, res3;
    for (int i = 0; i < rowCnt; i++) {
        int32_t aPaddingCount = bpLen[i] > 0 ?
            aWidth - omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(ap[i]), apLen[i]) :
            0;
        outLen[i] = apLen[i] + aPaddingCount + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }

        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        res1 = memcpy_s(ret, outLen[i] + 1, ap[i], apLen[i]);
        res2 = memset_s(ret + apLen[i], outLen[i] - apLen[i] + 1, ' ', aPaddingCount);
        res3 = memcpy_s(ret + apLen[i] + aPaddingCount, outLen[i] - (apLen[i] + aPaddingCount) + 1, bp[i], bpLen[i]);
        if (res1 != EOK || res2 != EOK || res3 != EOK) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatCharStr(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen, uint8_t **bp,
    int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2, res3;
    for (int i = 0; i < rowCnt; i++) {
        int32_t aPaddingCount = bpLen[i] > 0 ?
            aWidth - omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(ap[i]), apLen[i]) :
            0;
        outLen[i] = apLen[i] + aPaddingCount + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }

        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        res1 = memcpy_s(ret, outLen[i] + 1, ap[i], apLen[i]);
        res2 = memset_s(ret + apLen[i], outLen[i] - apLen[i] + 1, ' ', aPaddingCount);
        res3 = memcpy_s(ret + apLen[i] + aPaddingCount, outLen[i] - (apLen[i] + aPaddingCount) + 1, bp[i], bpLen[i]);
        if (res1 != EOK || res2 != EOK || res3 != EOK) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatStrChar(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t bWidth,
    int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2;
    for (int i = 0; i < rowCnt; i++) {
        outLen[i] = apLen[i] + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }

        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        res1 = memcpy_s(ret, outLen[i] + 1, ap[i], apLen[i]);
        res2 = memcpy_s(ret + apLen[i], outLen[i] - apLen[i] + 1, bp[i], bpLen[i]);
        if (res1 != EOK || res2 != EOK) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchCastStrWithDiffWidths(int64_t contextPtr, uint8_t **str, int32_t srcWidth, int32_t *strLen,
    bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
        }
        bool hasErr = false;
        const uint8_t *ret =
            CastStrStr(&hasErr, reinterpret_cast<const char *>(str[i]), srcWidth, strLen[i], outLen + i, dstWidth);
        if (hasErr) {
            ostringstream errMsg;
            errMsg << "cast varchar[" << srcWidth << "] to varchar[" << dstWidth << "] failed.";
            SetError(contextPtr, errMsg.str().c_str(), errMsg.str().length());
        }
        output[i] = const_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchLengthChar(uint8_t **str, const int32_t width, int32_t *strLen, bool *isAnyNull,
    int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = width;
    }
}


extern DLLEXPORT void BatchLengthCharReturnInt32(uint8_t **str, const int32_t width, int32_t *strLen, bool *isAnyNull,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        output[i] = width;
    }
}

extern DLLEXPORT void BatchLengthStrReturnInt32(uint8_t **str, int32_t *strLen, bool *isAnyNull, int32_t *output,
    int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        output[i] = omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
    }
}

extern DLLEXPORT void BatchLengthStr(uint8_t **str, int32_t *strLen, bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        output[i] = omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
    }
}

extern DLLEXPORT void BatchReplaceStrStrStrWithRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, uint8_t **replaceStr, int32_t *replaceLen, bool *isAnyNull,
    uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        EngineType engineType = EngineUtil::GetInstance().GetEngineType();
        if (searchLen[i] == 0 && engineType == EngineType::Spark) {
            outLen[i] = strLen[i];
            auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
            errno_t res = memcpy_s(ret, outLen[i] + 1, str[i], strLen[i]);
            if (res != EOK) {
                SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                outLen[i] = 0;
                output[i] = (uint8_t *)"";
                continue;
            }
            output[i] = reinterpret_cast<uint8_t *>(ret);
            continue;
        } else if (searchLen[i] == 0) {
            int32_t strCodePoints =
                omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
            outLen[i] = strLen[i] + (strCodePoints + 1) * replaceLen[i];
            auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
            int32_t indexBuffer = 0;
            errno_t res;
            for (int32_t index = 0; index < strLen[i];) {
                res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, replaceStr[i], replaceLen[i]);
                if (res != EOK) {
                    SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                    outLen[i] = 0;
                    output[i] = (uint8_t *)"";
                    continue;
                }
                indexBuffer += replaceLen[i];
                int32_t codePointLength =
                    omniruntime::Utf8Util::LengthOfCodePoint(static_cast<char>(*(str[i] + index)));
                res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, str[i] + index, codePointLength);
                if (res != EOK) {
                    SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                    outLen[i] = 0;
                    output[i] = (uint8_t *)"";
                    continue;
                }
                indexBuffer += codePointLength;
                index += codePointLength;
            }
            res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, replaceStr[i], replaceLen[i]);
            if (res != EOK) {
                SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                outLen[i] = 0;
                output[i] = (uint8_t *)"";
                continue;
            }
            output[i] = reinterpret_cast<uint8_t *>(ret);
            continue;
        }

        if (strLen[i] == 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        std::string s = std::string(reinterpret_cast<char *>(str[i]), strLen[i]);
        std::string search = std::string(reinterpret_cast<char *>(searchStr[i]), searchLen[i]);
        std::string replace = std::string(reinterpret_cast<char *>(replaceStr[i]), replaceLen[i]);
        std::string::size_type matchIndex = 0;
        if (replaceLen[i] == 0) {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s = s.substr(0, matchIndex) + s.substr(matchIndex + searchLen[i]);
            }
        } else {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s.replace(matchIndex, searchLen[i], replace);
                matchIndex += replaceLen[i];
            }
        }

        outLen[i] = s.length();
        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        error_t res = memcpy_s(ret, outLen[i] + 1, s.c_str(), s.length());
        if (res != EOK) {
            SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchReplaceStrStrWithoutRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    uint8_t *replaceStr[1] = { (uint8_t *)"" };
    int32_t replaceLen[1] = { 0 };

    for (int i = 0; i < rowCnt; i++) {
        EngineType engineType = EngineUtil::GetInstance().GetEngineType();
        if (searchLen[i] == 0 && engineType == EngineType::Spark) {
            outLen[i] = strLen[i];
            auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
            errno_t res = memcpy_s(ret, outLen[i] + 1, str[i], strLen[i]);
            if (res != EOK) {
                SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                outLen[i] = 0;
                output[i] = (uint8_t *)"";
                continue;
            }
            output[i] = reinterpret_cast<uint8_t *>(ret);
            continue;
        } else if (searchLen[i] == 0) {
            int32_t strCodePoints =
                omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
            outLen[i] = strLen[i] + (strCodePoints + 1) * replaceLen[0];
            auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
            int32_t indexBuffer = 0;
            errno_t res;
            for (int32_t index = 0; index < strLen[i];) {
                res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, replaceStr[0], replaceLen[0]);
                if (res != EOK) {
                    SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                    outLen[i] = 0;
                    output[i] = (uint8_t *)"";
                    continue;
                }
                indexBuffer += replaceLen[i];
                int32_t codePointLength =
                    omniruntime::Utf8Util::LengthOfCodePoint(static_cast<char>(*(str[i] + index)));
                res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, str[i] + index, codePointLength);
                if (res != EOK) {
                    SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                    outLen[i] = 0;
                    output[i] = (uint8_t *)"";
                    continue;
                }
                indexBuffer += codePointLength;
                index += codePointLength;
            }
            res = memcpy_s(ret + indexBuffer, outLen[i] - indexBuffer + 1, replaceStr[0], replaceLen[0]);
            if (res != EOK) {
                SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
                outLen[i] = 0;
                output[i] = (uint8_t *)"";
                continue;
            }
            output[i] = reinterpret_cast<uint8_t *>(ret);
            continue;
        }

        if (strLen[i] == 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        std::string s = std::string(reinterpret_cast<char *>(str[i]), strLen[i]);
        std::string search = std::string(reinterpret_cast<char *>(searchStr[i]), searchLen[i]);
        std::string replace = std::string(reinterpret_cast<char *>(replaceStr[0]), replaceLen[0]);
        std::string::size_type matchIndex = 0;
        if (replaceLen[0] == 0) {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s = s.substr(0, matchIndex) + s.substr(matchIndex + searchLen[i]);
            }
        } else {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s.replace(matchIndex, searchLen[i], replace);
                matchIndex += replaceLen[0];
            }
        }

        outLen[i] = s.length();
        auto ret = ArenaAllocatorMalloc(contextPtr, outLen[i] + 1);
        error_t res = memcpy_s(ret, outLen[i] + 1, s.c_str(), s.length());
        if (res != EOK) {
            SetError(contextPtr, REPLACE_ERR_MSG.c_str(), REPLACE_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatStrStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2;
    for (int i = 0; i < rowCnt; ++i) {
        outLen[i] = apLen[i] + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            isNull[i] = false;
            continue;
        }

        ret = ArenaAllocatorMalloc(contextPtr, outLen[i]);
        res1 = memcpy_s(ret, outLen[i], ap, apLen[i]);
        res2 = memcpy_s(ret + apLen[i], *outLen, bp, bpLen[i]);
        if (apLen[i] != 0 && bpLen[i] != 0 && (res1 != EOK || res2 != EOK)) {
            isNull[i] = true;
            char message[] = "Concat failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            output[i] = (uint8_t *)"";
            continue;
        }
        isNull[i] = false;
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatCharCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2, res3;
    int aPaddingCount;
    for (int i = 0; i < rowCnt; ++i) {
        aPaddingCount = bpLen[i] > 0 ? aWidth - apLen[i] : 0;
        outLen[i] = apLen[i] + aPaddingCount + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            isNull[i] = false;
            continue;
        }

        ret = ArenaAllocatorMalloc(contextPtr, outLen[i]);
        res1 = memcpy_s(ret, outLen[i], ap[i], apLen[i]);
        res2 = memset_s(ret + apLen[i], outLen[i] - apLen[i], ' ', aPaddingCount);
        res3 = memcpy_s(ret + apLen[i] + aPaddingCount, outLen[i] - (apLen[i] + aPaddingCount), bp[i], bpLen[i]);
        if (apLen[i] != 0 && aPaddingCount != 0 && bpLen[i] != 0 && (res1 != EOK || res2 != EOK || res3 != EOK)) {
            char message[] = "Concat failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            output[i] = (uint8_t *)"";
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatCharStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2, res3;
    int32_t aPaddingCount;
    for (int i = 0; i < rowCnt; ++i) {
        aPaddingCount = bpLen[i] > 0 ? aWidth - apLen[i] : 0;
        outLen[i] = apLen[i] + aPaddingCount + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            isNull[i] = false;
            continue;
        }

        ret = ArenaAllocatorMalloc(contextPtr, outLen[i]);
        res1 = memcpy_s(ret, outLen[i], ap[i], apLen[i]);
        res2 = memset_s(ret + apLen[i], outLen[i] - apLen[i], ' ', aPaddingCount);
        res3 = memcpy_s(ret + apLen[i] + aPaddingCount, outLen[i] - (apLen[i] + aPaddingCount), bp[i], bpLen[i]);
        if (apLen[i] != 0 && aPaddingCount != 0 && bpLen[i] != 0 && (res1 != EOK || res2 != EOK || res3 != EOK)) {
            char message[] = "Concat failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            output[i] = (uint8_t *)"";
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchConcatStrCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    char *ret;
    errno_t res1, res2;
    for (int i = 0; i < rowCnt; ++i) {
        outLen[i] = apLen[i] + bpLen[i];
        if (outLen[i] <= 0) {
            outLen[i] = 0;
            output[i] = (uint8_t *)"";
            isNull[i] = false;
            continue;
        }

        ret = ArenaAllocatorMalloc(contextPtr, outLen[i]);
        res1 = memcpy_s(ret, outLen[i], ap[i], apLen[i]);
        res2 = memcpy_s(ret + apLen[i], outLen[i] - apLen[i], bp[i], bpLen[i]);
        if (apLen[i] != 0 && bpLen[i] != 0 && (res1 != EOK || res2 != EOK)) {
            char message[] = "Concat failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            output[i] = (uint8_t *)"";
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchCastIntToStringRetNull(bool *isNull, int64_t contextPtr, int32_t *value, uint8_t **output,
    int32_t *outLen, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        string str = to_string(value[i]);
        outLen[i] = static_cast<int32_t>(str.size());
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
        errno_t res = memcpy_s(ret, *outLen, str.c_str(), *outLen);
        if (res != EOK) {
            char message[] = "cast failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        output[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern "C" DLLEXPORT void BatchCastStrWithDiffWidthsRetNull(bool *isNull, int64_t contextPtr, uint8_t **srcStr,
    int32_t srcWidth, int32_t *strLen, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = const_cast<uint8_t *>(CastStrStr(isNull + i, reinterpret_cast<const char *>(srcStr[i]), srcWidth,
            strLen[i], outLen + i, dstWidth));
    }
}