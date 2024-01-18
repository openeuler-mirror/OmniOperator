/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI common functions
 */
#ifndef JNI_COMMON_DEF_H
#define JNI_COMMON_DEF_H

#include <jni.h>
#include "util/omni_exception.h"

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
    }                                                                  \
    catch (const std::exception &e)                                    \
    {                                                                  \
        Expr::DeleteExprs(toDeleteExprs);                              \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());            \
        return fallBackExpr;                                           \
    }                                                                  \
    // macro end

#define JNI_METHOD_END_WITH_MULTI_EXPRS(fallBackExpr, toDeleteExprs1, toDeleteExprs2) \
    }                                                                                 \
    catch (const std::exception &e)                                                   \
    {                                                                                 \
        Expr::DeleteExprs(toDeleteExprs1);                                            \
        Expr::DeleteExprs(toDeleteExprs2);                                            \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());                           \
        return fallBackExpr;                                                          \
    }                                                                                 \
    // macro end

#define JNI_METHOD_END_WITH_THREE_EXPRS(fallBackExpr, toDeleteExprs1, toDeleteExprs2, toDeleteExprs3) \
    }                                                                                                 \
    catch (const std::exception &e)                                                                   \
    {                                                                                                 \
        Expr::DeleteExprs(toDeleteExprs1);                                                            \
        Expr::DeleteExprs(toDeleteExprs2);                                                            \
        Expr::DeleteExprs(toDeleteExprs3);                                                            \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());                                           \
        return fallBackExpr;                                                                          \
    }                                                                                                 \
    // macro end


#define JNI_METHOD_END_WITH_OVERFLOW(fallBackExpr, overflowConfig) \
    }                                                              \
    catch (const std::exception &e)                                \
    {                                                              \
        delete (overflowConfig);                                   \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());        \
        return fallBackExpr;                                       \
    }

#define JNI_METHOD_END_WITH_EXPRS_OVERFLOW(fallBackExpr, toDeleteExprs, overflowConfig) \
    }                                                                                   \
    catch (const std::exception &e)                                                     \
    {                                                                                   \
        Expr::DeleteExprs(toDeleteExprs);                                               \
        delete (overflowConfig);                                                        \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());                             \
        return fallBackExpr;                                                            \
    }


#define JNI_METHOD_END_WITH_MULTI_EXPRS_OVERFLOW(fallBackExpr, toDeleteExprs1, toDeleteExprs2, overflowConfig) \
    }                                                                                                          \
    catch (const std::exception &e)                                                                            \
    {                                                                                                          \
        Expr::DeleteExprs(toDeleteExprs1);                                                                     \
        Expr::DeleteExprs(toDeleteExprs2);                                                                     \
        delete (overflowConfig);                                                                               \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());                                                    \
        return fallBackExpr;                                                                                   \
    }

#define JNI_METHOD_END_WITH_VECBATCHES(fallBackExpr, toDeleteVecBatch) \
    }                                                                  \
    catch (const std::exception &e)                                    \
    {                                                                  \
        if (toDeleteVecBatch) {                                        \
            VectorHelper::FreeVecBatch(toDeleteVecBatch);              \
        }                                                              \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());            \
        return fallBackExpr;                                           \
    }

#ifdef __cplusplus
extern "C" {
#endif

extern jclass bufCls;
extern jclass vecBatchCls;
extern jclass omniResultsCls;
extern jclass traceUtilCls;
extern jclass omniRuntimeExceptionClass;
extern jmethodID vecBatchInitMethodId;
extern jmethodID omniResultsInitMethodId;
extern jmethodID traceUtilStackMethodId;

#ifdef __cplusplus
}
#endif
#endif
