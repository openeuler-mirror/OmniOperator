/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_JNI_VECTOR_LOADER_H
#define OMNI_RUNTIME_JNI_VECTOR_LOADER_H

#include <jni.h>
#include "vector/lazy_vector.h"
#include "vector/loader/vector_loader.h"

namespace omniruntime {
namespace vec {
class JniVectorLoader : public VectorLoader {
public:
    JniVectorLoader(JNIEnv *jniEnv, jobject jLoader)
        : VectorLoader(), jniEnv(jniEnv), jLoader(jniEnv->NewGlobalRef(jLoader))
    {}

    ~JniVectorLoader() override
    {
        jniEnv->DeleteGlobalRef(jLoader);
    }

    Vector *Load() override;

    // Each thread has its own JNIEnv, construct thread may not the same as addInput,
    // so here need SetJNIEnv interface.
    void SetJNIEnv(JNIEnv *jniEnv)
    {
        this->jniEnv = jniEnv;
    }

private:
    JNIEnv *jniEnv;
    jobject jLoader;
};
}
}

#endif // OMNI_RUNTIME_JNI_VECTOR_LOADER_H
