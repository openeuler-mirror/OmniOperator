//
// Created by root on 5/27/21.
//
#include <jni.h>
#ifndef __JNI_COMMON_DEF_H__
#define __JNI_COMMON_DEF_H__
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

#endif //__JNI_COMMON_DEF_H__
