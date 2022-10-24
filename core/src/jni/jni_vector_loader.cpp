/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "jni_vector_loader.h"
#include "jni_common_def.h"
#include "util/omni_exception.h"
#include "util/error_code.h"

namespace omniruntime {
namespace vec {
Vector *JniVectorLoader::Load()
{
    jobject nativeVector = jniEnv->CallStaticObjectMethod(lazyVectorCls, lazyVectorLoaderMethodId, jLoader);
    jthrowable nativeException = jniEnv->ExceptionOccurred();
    if (nativeException != nullptr) {
        jniEnv->ExceptionDescribe();
    }

    auto *loadedVector = reinterpret_cast<Vector *>(nativeVector);
    if (loadedVector) {
        return loadedVector;
    }
    throw OmniException(op::GetErrorCode(op::ErrorCode::LOAD_LAZY_VECTOR_FAILED),
        op::GetErrorMessage(op::ErrorCode::LOAD_LAZY_VECTOR_FAILED));
}
}
}
