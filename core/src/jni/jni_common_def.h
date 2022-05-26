/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI common functions
 */
#ifndef JNI_COMMON_DEF_H
#define JNI_COMMON_DEF_H

#include <jni.h>
#include "../util/omni_exception.h"

#define JNI_METHOD_START try {
// macro end

#define JNI_METHOD_END(fallBackExpr)                        \
    }                                                       \
    catch (const std::exception &e)                         \
    {                                                       \
        env->ThrowNew(omniRuntimeExceptionClass, e.what()); \
        return fallBackExpr;                                \
    }                                                       \
    // macro end

#define JNI_METHOD_END_WITH_EXPRS_RELEASE(fallBackExpr, toDeleteExprs) \
    }                                                              \
    catch (const std::exception &e)                                \
    {                                                              \
        Expr::DeleteExprs(toDeleteExprs);                          \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());        \
        return fallBackExpr;                                       \
    }                                                              \
    // macro end

#ifdef __cplusplus
extern "C" {
#endif

extern jclass bufCls;
extern jclass vecBatchCls;
extern jclass omniResultsCls;
extern jclass traceUtilCls;
extern jclass lazyVectorCls;
extern jclass omniRuntimeExceptionClass;
extern jmethodID vecBatchInitMethodId;
extern jmethodID omniResultsInitMethodId;
extern jmethodID traceUtilStackMethodId;
extern jmethodID lazyVectorLoaderMethodId;

#ifdef __cplusplus
}
#endif
#endif
