/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI common functions
 */
#ifndef JNI_COMMON_DEF_H
#define JNI_COMMON_DEF_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

extern jclass bufCls;
extern jclass vecBatchCls;
extern jclass omniResultsCls;
extern jclass traceUtilCls;
extern jmethodID vecBatchInitMethodId;
extern jmethodID vecBatchInitMethodId;
extern jmethodID omniResultsInitMethodId;
extern jmethodID traceUtilStackMethodId;

#ifdef __cplusplus
}
#endif
#endif
