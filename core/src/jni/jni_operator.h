//
// Created by root on 5/26/21.
//
#include <jni.h>

#ifndef __JNI_OPERATOR_H__
#define __JNI_OPERATOR_H__
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
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInputNative
        (JNIEnv *, jobject, jlong, jlong);
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutputNative
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
#endif //__JNI_OPERATOR_H__
