//
// Created by root on 5/26/21.
//
#include <stdint.h>
#include "jni_vector.h"
#include "../memory/memory_pool.h"
/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_allocate
        (JNIEnv *env, jclass jcls, jint jsize)
{
    void* addr = omni_allocate(jsize);
    jobject jbuf = env->NewDirectByteBuffer(addr, jsize);
    return jbuf;
}

/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    release
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_release
(JNIEnv *env, jclass jcls, jlong jAddress)
{
    if (jAddress < 0) {
        std::cout << "release address is error:" << jAddress << std::endl;
        return;
    }
    omni_release(jAddress);
}
