/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

#ifndef __DEBUG_H__
#define __DEBUG_H__

#include <chrono>
#include <stdexcept>
#include "../../config.h"
#include "util/global_log.h"

using Time = std::chrono::high_resolution_clock;
using ms = std::chrono::milliseconds;
using fsec = std::chrono::duration<float>;

// define time
#if defined(DEBUG_JNI) || defined(DEBUG_OPERATOR) || defined(DEBUG_LLVM)
#define START() Time::now()
#define END(t0)                                    \
    ({                                             \
        auto t1 = Time::now();                     \
        fsec fs = t1 - t0;                         \
        ms d = std::chrono::duration_cast<ms>(fs); \
        d.count();                                 \
    })
#else
#define START() 0
#define END(t0) 0
#endif

#ifdef DEBUG_JNI
#define JNI_DEBUG_LOG(format, ...) printf("[%s][%s][%d]: " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define JNI_DEBUG_LOG(format, ...)
#endif

#ifdef OP_DEBUG
#define OP_DEBUG_LOG(format, ...)                                                                  \
    do {                                                                                           \
        printf("[OPERATOR][%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define OP_DEBUG_LOG(format, ...)
#endif

#ifdef DEBUG_LLVM
#define LLVM_DEBUG_LOG(format, ...)                                                                 \
    do {                                                                                            \
        printf("[LLVM][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LLVM_DEBUG_LOG(format, ...)
#endif

#ifdef TRACE
#define LogTrace(format, ...)                                                                        \
    do {                                                                                             \
        printf("[TRACE][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LogTrace(format, ...)
#endif

#define LogDebug(format, ...)                                                                        \
    do {                                                                                             \
        char logBuf[GLOBAL_LOG_BUF_SIZE];                                                            \
        LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                            \
        std::string logString(logBuf);                                                               \
        Log(logString, LogType::LOG_DEBUG);                                                          \
    } while (0)

#define LogInfo(format, ...)                                                                        \
    do {                                                                                            \
        char logBuf[GLOBAL_LOG_BUF_SIZE];                                                           \
        LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                           \
        std::string logString(logBuf);                                                              \
        Log(logString, LogType::LOG_INFO);                                                          \
    } while (0)

#define LogWarn(format, ...)                                                                        \
    do {                                                                                            \
        char logBuf[GLOBAL_LOG_BUF_SIZE];                                                           \
        LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                           \
        std::string logString(logBuf);                                                              \
        Log(logString, LogType::LOG_WARN);                                                          \
    } while (0)

#define LogError(format, ...)                                                                       \
    do {                                                                                            \
        char logBuf[GLOBAL_LOG_BUF_SIZE];                                                           \
        LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                           \
        std::string logString(logBuf);                                                              \
        Log(logString, LogType::LOG_ERROR);                                                         \
    } while (0)

#if defined(DEBUG) || defined(TRACE)
#ifndef ASSERT
#define ASSERT(condition)                                          \
    do {                                                           \
        if (!(condition)) {                                        \
            throw std::runtime_error(#condition " is not match!"); \
        }                                                          \
    } while (0)
#endif
#else
#define ASSERT(condition)
#endif
#endif