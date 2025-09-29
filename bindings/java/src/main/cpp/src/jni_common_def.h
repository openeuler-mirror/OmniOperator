// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

#define JNI_METHOD_END_WITH_VECBATCH(fallBackExpr, toDeleteVecBatch) \
    }                                                                \
    catch (const std::exception &e)                                  \
    {                                                                \
        VectorHelper::FreeVecBatch(toDeleteVecBatch);                \
        env->ThrowNew(omniRuntimeExceptionClass, e.what());          \
        return fallBackExpr;                                         \
    }

#ifdef __cplusplus
extern "C" {
#endif

extern jclass bufCls;
extern jclass traceUtilCls;
extern jclass omniRuntimeExceptionClass;
extern jmethodID traceUtilStackMethodId;

jclass CreateGlobalClassRef(JNIEnv *env, const char *className);

#ifdef __cplusplus
}
#endif
#endif
