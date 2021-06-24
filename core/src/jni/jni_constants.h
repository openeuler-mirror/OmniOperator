//
// Created by root on 5/31/21.
//

#include <jni.h>
#ifndef __JNI_CONSTANTS_H__
#define __JNI_CONSTANTS_H__

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_constants_Constant_loadConstants
        (JNIEnv *env, jclass ignore);

#ifdef __cplusplus
}
#endif

#endif //__JNI_CONSTANTS_H__
