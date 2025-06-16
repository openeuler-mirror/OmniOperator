/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: JNI Constants
 */

#include "jni_constants.h"

JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_OmniLibs_getVersion(JNIEnv *env, jclass ignore)
{
    return (*env).NewStringUTF(
        "Product Name: Kunpeng BoostKit\nProduct Version: 25.0.0\nComponent Name: BoostKit-omniop\nComponent Version: 1.9.0");
}