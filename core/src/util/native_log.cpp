/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Factory Source File
 */

#include "native_log.h"
#include "global_log.h"

using namespace std;

jmethodID logWarnId;
jmethodID logErrorId;
jmethodID logInfoId;
jmethodID logDebugId;
JavaVM *localJVM;
jobject oplogObj;
int g_logLevel;
bool g_isDebugEnable = false;

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