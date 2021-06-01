//
// Created by root on 5/27/21.
//
#include <jni.h>
#ifndef OMNI_RUNTIME_JNI_COMMON_DEF_H
#define OMNI_RUNTIME_JNI_COMMON_DEF_H
#ifdef __cplusplus
extern "C" {
#endif

extern jclass bufCls;
extern jclass vecBatchCls;
extern jclass omniResultsCls;
extern jmethodID vecBatchInitMethodId;
extern jmethodID omniResultsInitMethodId;

#ifdef __cplusplus
}
#endif

#endif //OMNI_RUNTIME_JNI_COMMON_DEF_H
