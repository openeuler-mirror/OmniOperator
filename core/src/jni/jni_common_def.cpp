//
// Created by root on 5/27/21.
//

#include "jni_common_def.h"

jclass bufCls;
jclass vecBatchCls;
jclass omniResultsCls;
jmethodID vecBatchInitMethodId;
jmethodID omniResultsInitMethodId;
jint JNI_VERSION = JNI_VERSION_1_6;

jclass createGlobalClassRef(JNIEnv* env, const char *className) {
    jclass local_class = env->FindClass(className);
    jclass global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    return global_class;
}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }
    bufCls = createGlobalClassRef(env, "java/nio/ByteBuffer");
    vecBatchCls = createGlobalClassRef(env,"nova/hetu/omniruntime/vector/VecBatch");
    vecBatchInitMethodId = env->GetMethodID(vecBatchCls, "<init>", "([Ljava/nio/ByteBuffer;[II)V");
    omniResultsCls = createGlobalClassRef(env,"nova/hetu/omniruntime/operator/OmniResults");
    omniResultsInitMethodId = env->GetMethodID(omniResultsCls, "<init>", "([Lnova/hetu/omniruntime/vector/VecBatch;I)V");
    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, void *reserved) {
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
    env->DeleteGlobalRef(vecBatchCls);
    env->DeleteGlobalRef(bufCls);
}