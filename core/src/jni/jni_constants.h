/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Constants
 */
#ifndef JNI_CONSTANTS_H
#define JNI_CONSTANTS_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_constants_Constant_loadConstants(JNIEnv *env, jclass ignore);

#ifdef __cplusplus
}
#endif
#endif
