/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Test Native Class
 */

#ifndef OMNI_RUNTIME_GLOBAL_LOG_H
#define OMNI_RUNTIME_GLOBAL_LOG_H

#define GLOBAL_LOG_BUF_SIZE 1024

#include <string>
#include <iostream>
#include <sstream>
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif

#define LogsInfoMacro(stringStream, formatStr) \
    stringStream << "[" << __FILE__ << "][" << __FUNCTION__ << "][" << __LINE__ << "]" << formatStr

#define LogsInfoVargMacro(logBuf, format, ...)                                                                   \
    snprintf_s(logBuf, GLOBAL_LOG_BUF_SIZE, GLOBAL_LOG_BUF_SIZE, "[%s][%s][%d]:" format, __FILE__, __FUNCTION__, \
        __LINE__, ##__VA_ARGS__)

enum class LogType {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3
};

void Log(const std::string &logStr, LogType logLev);

void FreeLog();

int GetLogLevel();
bool IsDebugEnable();

#ifdef __cplusplus
}
#endif
#endif // OMNI_RUNTIME_GLOBAL_LOG_H
