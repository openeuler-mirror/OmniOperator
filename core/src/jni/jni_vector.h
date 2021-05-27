//
// Created by root on 5/26/21.
//
#include <jni.h>
#ifndef OMNI_RUNTIME_JNI_VECTOR_H
#define OMNI_RUNTIME_JNI_VECTOR_H
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_allocate
        (JNIEnv *, jclass, jint);

/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    release
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_release
(JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif //OMNI_RUNTIME_JNI_VECTOR_H
