/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: java udf util.
 */
#ifndef OMNI_RUNTIME_JNI_UTIL_H
#define OMNI_RUNTIME_JNI_UTIL_H

#include <jni.h>
#include <string>
#include <vector>
#include "util/error_code.h"

#define RETURN_ERROR_CODE_IF_EXC(env)                                                            \
    do {                                                                                         \
        if ((env)->ExceptionCheck()) {                                                           \
            LogError("Get Jni object failed since : %s", JniUtil::GetExceptionMsg(env).c_str()); \
            return ErrorCode::JVM_FAILED;                                                        \
        }                                                                                        \
    } while (false)

class JniUtil {
public:
    static omniruntime::op::ErrorCode Init();
    static JNIEnv *GetJNIEnv();
    static omniruntime::op::ErrorCode GetGlobalClassRef(JNIEnv *env, const char *className, jclass *globalClassRef);
    static jclass GetHiveUdfExecutorCls();
    static jmethodID GetExecuteSingleMethod();
    static jmethodID GetExecuteBatchMethod();
    static jclass GetDataTypeIdCls();
    static jclass GetUdfUtilCls();
    static jmethodID GetThrowableToStringMethod();

    static jfieldID GetFieldId(int32_t id);
    static std::string GetExceptionMsg(JNIEnv *env);
    static void SetEnv(JNIEnv *env)
    {
        threadLocalEnv = env;
    }

private:
    static omniruntime::op::ErrorCode InitDataTypeIdAndFields(JNIEnv *env);

    static JNIEnv *GetNewJNIEnv();
    static __thread JNIEnv *threadLocalEnv;
};

class JniUtfChars {
public:
    JniUtfChars() : env(nullptr), jString(nullptr), utfChars(nullptr) {}

    ~JniUtfChars()
    {
        if (utfChars != nullptr) {
            env->ReleaseStringUTFChars(jString, utfChars);
        }
    }

    const char *GetUtfChars()
    {
        return utfChars;
    }

    static omniruntime::op::ErrorCode Create(JNIEnv *env, jstring jString, JniUtfChars *jniUtfChars);

private:
    JNIEnv *env;
    jstring jString;
    const char *utfChars;
};

#endif // OMNI_RUNTIME_JNI_UTIL_H
