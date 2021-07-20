/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Operations Source File
 */

#include <vector>
#include <algorithm>
#include <src/vector/long_vector.h>
#include <src/vector/vector_batch.h>
#include "jni_operator.h"
#include "jni_common_def.h"
#include "../operator/operator_factory.h"
#include "../util/debug.h"

using namespace omniruntime::op;

jobjectArray transform(JNIEnv *env, std::vector<VectorBatch *> &result)
{
    jobjectArray res = env->NewObjectArray(result.size(), vecBatchCls, nullptr);
    int32_t idx = 0;
    for (auto vecBatch : result) {
        jobject obj = env->NewObject(vecBatchCls, vecBatchInitMethodId, (jlong)((int64_t)vecBatch), vecBatch->getRowCount());
        env->SetObjectArrayElement(res, idx++, obj);
    }
    return res;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInputNative
        (JNIEnv *env, jobject jObj, jlong jOperatorAddress, jlong jVecBatchAddress)
{
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    Operator *nativeOperator = (Operator *) jOperatorAddress;
    int32_t ret = nativeOperator->AddInput(vecBatch);
    return ret;
}

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutputNative
        (JNIEnv *env, jobject jObj, jlong jOperatorAddr)
{
    JNI_DEBUG_LOG("get output starting.");
    auto start = START();
    Operator *nativeOperator = (Operator *) jOperatorAddr;
    std::vector<VectorBatch *> outputPages;
    int32_t errNo = nativeOperator->GetOutput(outputPages);
    JNI_DEBUG_LOG("getOutput finished, elapsed time: %ld ms.", END(start));
    jobjectArray result = transform(env, outputPages);
    JNI_DEBUG_LOG("transform finished, elapsed time: %ld ms.", END(start));
    return env->NewObject(omniResultsCls, omniResultsInitMethodId, result, nativeOperator->GetStatus());
}

/*
* Class:     nova_hetu_omniruntime_operator_OmniOperator
* Method:    close
* Signature: (J)[Lnova/hetu/omniruntime/operator/void;
*/
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative
        (JNIEnv *env, jobject jObj, jlong jOperatorAddr) {
    JNI_DEBUG_LOG("close starting.");
    auto start = START();
    Operator *nativeOperator = (Operator *) jOperatorAddr;
    delete nativeOperator;
    JNI_DEBUG_LOG("close finished, elapsed time: %ld ms.", END(start));
}
