/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: JNI Constants
 */
#ifndef JNI_CONSTANTS_H
#define JNI_CONSTANTS_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_OmniLibs
 * Method:    getVersion
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_OmniLibs_getVersion(JNIEnv *env, jclass ignore);

#ifdef __cplusplus
}
#endif
#endif
