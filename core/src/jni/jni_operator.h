//
// Created by root on 5/26/21.
//
#include <jni.h>
#ifndef OMNI_RUNTIME_OPERATOR_H
#define OMNI_RUNTIME_OPERATOR_H
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    addInput
 * Signature: (JJIJI)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInput
        (JNIEnv *, jobject, jlong, jlong, jint, jlong, jint);
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutput
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutput
        (JNIEnv *, jobject, jlong);

/*
* Class:     nova_hetu_omniruntime_operator_OmniOperator
* Method:    close
* Signature: (J)[Lnova/hetu/omniruntime/operator/void;
*/
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative
        (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif //OMNI_RUNTIME_OPERATOR_H
