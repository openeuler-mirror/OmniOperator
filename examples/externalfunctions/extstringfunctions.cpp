/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "extstringfunctions.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace std;


extern "C" DLLEXPORT int32_t LengthStr(int64_t s)
{
    auto tmps = s;
    void *sAddr = &s;
    auto cs = static_cast<uint8_t **>(sAddr);
    string str(*cs);
    return str.size();
}

extern "C" DLLEXPORT int32_t FirstCharStr(int64_t s)
{
    auto tmps = s;
    void *sAddr = &s;
    auto cs = static_cast<uint8_t **>(sAddr);
    return (*cs)[0];
}