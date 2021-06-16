#ifndef __STRINGFUNCTIONS_H__
#define __STRINGFUNCTIONS_H__

#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <algorithm>
#include <regex>
// #include <re2/re2.h>
// #include <re2/stringpiece.h>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT bool strEqualsExt(int64_t Ap, int64_t Bp);
extern "C" DLLEXPORT bool strCompareExt(int64_t Ap, int64_t Bp);
extern "C" DLLEXPORT bool likeExt(int64_t str, int64_t regexToMatch);
extern "C" DLLEXPORT int64_t substrExt(int64_t str, int32_t startIdx, int32_t length);
extern "C" DLLEXPORT int64_t concatStrExt(int64_t Ap, int64_t Bp);
extern "C" DLLEXPORT int32_t cast_string(int64_t str);



#endif