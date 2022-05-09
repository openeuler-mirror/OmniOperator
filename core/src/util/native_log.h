/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Native Log Header
 */
#ifndef OMNI_RUNTIME_NATIVE_LOG_H
#define OMNI_RUNTIME_NATIVE_LOG_H

#include "jni.h"

#ifdef __cplusplus
extern "C" {
#endif

extern jmethodID logWarnId;
extern jmethodID logErrorId;
extern jmethodID logInfoId;
extern jmethodID logDebugId;
extern JavaVM *localJVM;
extern jobject oplogObj;
extern int g_logLevel;

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_utils_NativeLog_initLog(JNIEnv *env, jclass jclz);

#ifdef __cplusplus
}
#endif
#endif // OMNI_RUNTIME_NATIVE_LOG_H