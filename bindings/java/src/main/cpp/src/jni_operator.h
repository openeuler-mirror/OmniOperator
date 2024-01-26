/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: JNI Operator Operations Header
 */
#ifndef JNI_OPERATOR_H
#define JNI_OPERATOR_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    addInputNative
 * Signature: (JJIJI)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInputNative(JNIEnv *, jobject, jlong, jlong);
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutputNative(JNIEnv *, jobject, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    close
 * Signature: (J)[Lnova/hetu/omniruntime/operator/void;
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative(JNIEnv *, jobject, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getSpilledBytesNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getSpilledBytesNative(JNIEnv *, jobject,
    jlong);

#ifdef __cplusplus
}
#endif
#endif
