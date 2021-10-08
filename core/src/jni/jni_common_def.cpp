/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI common functions
 */

#include "jni_common_def.h"

jclass bufCls;
jclass vecBatchCls;
jclass omniResultsCls;
jmethodID vecBatchInitMethodId;
jmethodID omniResultsInitMethodId;
jint JNI_VERSION = JNI_VERSION_1_6;

jclass createGlobalClassRef(JNIEnv *env, const char *className)
{
    jclass local_class = env->FindClass(className);
    jclass global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    return global_class;
}

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }
    bufCls = createGlobalClassRef(env, "java/nio/ByteBuffer");
    vecBatchCls = createGlobalClassRef(env, "nova/hetu/omniruntime/vector/VecBatch");
    vecBatchInitMethodId = env->GetMethodID(vecBatchCls, "<init>", "(J[J[II)V");
    omniResultsCls = createGlobalClassRef(env, "nova/hetu/omniruntime/operator/OmniResults");
    omniResultsInitMethodId =
        env->GetMethodID(omniResultsCls, "<init>", "([Lnova/hetu/omniruntime/vector/VecBatch;I)V");
    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, const void *reserved)
{
    JNIEnv *env = nullptr;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
    env->DeleteGlobalRef(vecBatchCls);
    env->DeleteGlobalRef(bufCls);
}