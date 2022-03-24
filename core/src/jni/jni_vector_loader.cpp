/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "jni_vector_loader.h"
#include "jni_common_def.h"

namespace omniruntime {
namespace vec {
Vector *JniVectorLoader::Load()
{
    jobject nativeVector = jniEnv->CallStaticObjectMethod(lazyVectorCls, lazyVectorLoaderMethodId, jLoader);
    return reinterpret_cast<Vector *>(nativeVector);
}
}
}
