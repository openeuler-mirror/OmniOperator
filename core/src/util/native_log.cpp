/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "native_log.h"
#include "global_log.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>

using namespace std;

inline jmethodID logWarnId;
inline jmethodID logErrorId;
inline jmethodID logInfoId;
inline jmethodID logDebugId;
inline JavaVM *localJVM;
inline jobject oplogObj;
inline int g_logLevel;
// Level the slf4j logger will actually publish. Kept apart from g_logLevel so that an override can
// lower the native threshold without the messages being dropped on the Java side.
inline int g_javaLogLevel;
inline bool g_isDebugEnable = false;

int GetLogLevel()
{
    return g_logLevel;
}

bool IsDebugEnable()
{
    return g_isDebugEnable;
}

void Log(const std::string &logStr, LogType logLev)
{
    if (oplogObj == nullptr) {
        std::cout << logStr << std::endl;
        return;
    }
    JNIEnv *tmpEnv = nullptr;
    localJVM->GetEnv(reinterpret_cast<void **>(&tmpEnv), JNI_VERSION_1_8);

    // With OMNI_LOG_LEVEL the native threshold can sit below what log4j accepts for this logger.
    // Publishing through the lowest enabled method keeps those messages visible without requiring
    // a log4j configuration change.
    if (static_cast<int>(logLev) < g_javaLogLevel && g_javaLogLevel <= static_cast<int>(LogType::LOG_ERROR)) {
        logLev = static_cast<LogType>(g_javaLogLevel);
    }

    switch (logLev) {
        case LogType::LOG_DEBUG: {
            tmpEnv->CallVoidMethod(oplogObj, logDebugId, tmpEnv->NewStringUTF(logStr.data()));
            break;
        }
        case LogType::LOG_INFO: {
            tmpEnv->CallVoidMethod(oplogObj, logInfoId, tmpEnv->NewStringUTF(logStr.data()));
            break;
        }
        case LogType::LOG_WARN: {
            tmpEnv->CallVoidMethod(oplogObj, logWarnId, tmpEnv->NewStringUTF(logStr.data()));
            break;
        }
        case LogType::LOG_ERROR: {
            tmpEnv->CallVoidMethod(oplogObj, logErrorId, tmpEnv->NewStringUTF(logStr.data()));
            break;
        }
        default:
            break;
    }
}

void FreeLog()
{
    JNIEnv *tmpEnv = nullptr;
    localJVM->GetEnv(reinterpret_cast<void **>(&tmpEnv), JNI_VERSION_1_8);
    tmpEnv->DeleteLocalRef(oplogObj);
}

// OMNI_LOG_LEVEL forces the native log threshold regardless of the slf4j configuration. It stays
// separate from g_isDebugEnable on purpose: that flag also turns on per-batch metrics collection in
// every operator, so raising it merely to read a log line would distort any timing measurement.
static bool ApplyLogLevelOverride()
{
    const char *raw = std::getenv("OMNI_LOG_LEVEL");
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string level(raw);
    std::transform(level.begin(), level.end(), level.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (level == "debug") {
        g_logLevel = static_cast<int>(LogType::LOG_DEBUG);
    } else if (level == "info") {
        g_logLevel = static_cast<int>(LogType::LOG_INFO);
    } else if (level == "warn" || level == "warning") {
        g_logLevel = static_cast<int>(LogType::LOG_WARN);
    } else if (level == "error") {
        g_logLevel = static_cast<int>(LogType::LOG_ERROR);
    } else {
        return false;
    }
    return true;
}

void InitLevel(JNIEnv *env)
{
    jclass logClass = env->FindClass("org/slf4j/Logger");
    jmethodID logDebugLevelId = env->GetMethodID(logClass, "isDebugEnabled", "()Z");
    jmethodID logInfoLevelId = env->GetMethodID(logClass, "isInfoEnabled", "()Z");
    jmethodID logWarnLevelId = env->GetMethodID(logClass, "isWarnEnabled", "()Z");
    jmethodID logErrorLevelId = env->GetMethodID(logClass, "isErrorEnabled", "()Z");
    g_isDebugEnable = env->CallBooleanMethod(oplogObj, logDebugLevelId);
    if (env->CallBooleanMethod(oplogObj, logDebugLevelId)) {
        g_logLevel = static_cast<int>(LogType::LOG_DEBUG);
    } else if (env->CallBooleanMethod(oplogObj, logInfoLevelId)) {
        g_logLevel = static_cast<int>(LogType::LOG_INFO);
    } else if (env->CallBooleanMethod(oplogObj, logWarnLevelId)) {
        g_logLevel = static_cast<int>(LogType::LOG_WARN);
    } else if (env->CallBooleanMethod(oplogObj, logErrorLevelId)) {
        g_logLevel = static_cast<int>(LogType::LOG_ERROR);
    } else {
        g_logLevel = static_cast<int>(LogType::LOG_ERROR) + 1;
    }
    g_javaLogLevel = g_logLevel;
    ApplyLogLevelOverride();
}

JNIEXPORT void Java_nova_hetu_omniruntime_utils_NativeLog_initLog(JNIEnv *env, jclass jclz)
{
    jfieldID logId = env->GetStaticFieldID(jclz, "logger", "Lorg/slf4j/Logger;");
    jclass logClass = env->FindClass("org/slf4j/Logger");
    oplogObj = env->NewGlobalRef(env->GetStaticObjectField(jclz, logId));

    // get warn method
    logWarnId = env->GetMethodID(logClass, "warn", "(Ljava/lang/String;)V");

    // get error method
    logErrorId = env->GetMethodID(logClass, "error", "(Ljava/lang/String;)V");

    // get info method
    logInfoId = env->GetMethodID(logClass, "info", "(Ljava/lang/String;)V");

    // get debug method
    logDebugId = env->GetMethodID(logClass, "debug", "(Ljava/lang/String;)V");

    InitLevel(env);

    env->GetJavaVM(&localJVM);
}