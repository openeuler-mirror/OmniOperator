/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: JNI common functions
 */

#include "jni_common_def.h"

jclass bufCls;
jclass traceUtilCls;
jclass omniRuntimeExceptionClass;

jmethodID traceUtilStackMethodId;

static const jint JNI_VERSION = JNI_VERSION_1_6;

jclass CreateGlobalClassRef(JNIEnv *env, const char *className)
{
    jclass localClass = env->FindClass(className);
    jclass globalClass = (jclass)env->NewGlobalRef(localClass);
    env->DeleteLocalRef(localClass);
    return globalClass;
}

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }
    bufCls = CreateGlobalClassRef(env, "java/nio/ByteBuffer");
    traceUtilCls = CreateGlobalClassRef(env, "nova/hetu/omniruntime/utils/TraceUtil");
    traceUtilStackMethodId = env->GetStaticMethodID(traceUtilCls, "stack", "()Ljava/lang/String;");
    omniRuntimeExceptionClass = CreateGlobalClassRef(env, "nova/hetu/omniruntime/utils/OmniRuntimeException");
    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, const void *reserved)
{
    JNIEnv *env = nullptr;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
    env->DeleteGlobalRef(bufCls);
    env->DeleteGlobalRef(traceUtilCls);
    env->DeleteGlobalRef(omniRuntimeExceptionClass);
}