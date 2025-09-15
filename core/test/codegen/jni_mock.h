/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: jni mock
 */

#ifndef OMNI_RUNTIME_JNI_MOCK_H
#define OMNI_RUNTIME_JNI_MOCK_H

#include <jni.h>
#include "gmock/gmock.h"

namespace omniruntime {
namespace mock {
class InAndOutputInfos {
public:
    bool operator == (const InAndOutputInfos &infos) const
    {
        return true;
    }
};

class JNIEnvMock : public JNIEnv {
public:
    JNIEnvMock();
    ~JNIEnvMock();

    MOCK_METHOD1(FindClass, jclass(const char *));
    MOCK_METHOD0(ExceptionOccurred, jthrowable());
    MOCK_METHOD0(ExceptionClear, void());

    MOCK_METHOD1(NewGlobalRef, jobject(jobject));
    MOCK_METHOD1(DeleteLocalRef, void(jobject));

    MOCK_METHOD3(GetStaticMethodID, jmethodID(jclass, const char *, const char *));

    MOCK_METHOD3(CallStaticObjectMethod, jobject(jclass, jmethodID, jthrowable));
    MOCK_METHOD3(CallStaticVoidMethodV, void(jclass, jmethodID, InAndOutputInfos));

    MOCK_METHOD3(GetStaticFieldID, jfieldID(jclass, const char *, const char *));
    MOCK_METHOD2(GetStaticObjectField, jobject(jclass, jfieldID));

    MOCK_METHOD1(NewStringUTF, jstring(const char *));
    MOCK_METHOD2(GetStringUTFChars, const char *(jstring, jboolean *));
    MOCK_METHOD2(ReleaseStringUTFChars, void(jstring, const char *));

    MOCK_METHOD3(NewObjectArray, jobjectArray(jsize, jclass, jobject));
    MOCK_METHOD3(SetObjectArrayElement, void(jobjectArray, jsize, jobject));

    MOCK_METHOD0(ExceptionCheck, jboolean());
};

class JavaVMMock : public JavaVM {
public:
    JavaVMMock();
    ~JavaVMMock();

    MOCK_METHOD2(GetEnv, jint(void **, jint));
    MOCK_METHOD2(AttachCurrentThread, jint(void **, void *));
};

JavaVMMock *CreateJavaVMMock();
void DestroyJavaVMMock(JavaVMMock *javaVM);
JNIEnvMock *CreateJNIEnvMock();
void DestroyJNIEnvMock(JNIEnvMock *env);
}
}

#endif // OMNI_RUNTIME_JNI_MOCK_H
