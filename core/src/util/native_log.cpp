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

    env->GetJavaVM(&localJVM);
}