/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Operator Operations Source File
 */

#include <vector>
#include <algorithm>
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "jni_common_def.h"
#include "operator/operator_factory.h"
#include "jni_operator.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;

void RecordInputVectorsStack(VectorBatch *vectorBatch, JNIEnv *env)
{
#ifdef DEBUG_VECTOR
    jstring jstack = (jstring)env->CallStaticObjectMethod(traceUtilCls, traceUtilStackMethodId);
    auto stackChars = env->GetStringUTFChars(jstack, JNI_FALSE);
    std::string stack(stackChars);
    int32_t vecCount = vectorBatch->GetVectorCount();
    for (int i = 0; i < vecCount; ++i) {
        Vector *vector = vectorBatch->GetVector(i);
        vector->RecordStack(stack, VecOpType::JNI_ADD_INPUT);
    }
    env->ReleaseStringUTFChars(jstack, stackChars);
#endif
}

void RecordOutputVectorsStack(VectorBatch &outputVecBatch, JNIEnv *env)
{
#ifdef DEBUG_VECTOR
    jstring jstack = (jstring)env->CallStaticObjectMethod(traceUtilCls, traceUtilStackMethodId);
    auto stackChars = env->GetStringUTFChars(jstack, JNI_FALSE);
    std::string stack(stackChars);
    for (int j = 0; j < outputVecBatch.GetVectorCount(); ++j) {
        Vector *vector = outputVecBatch.GetVector(j);
        vector->RecordStack(stack, VecOpType::JNI_GET_OUTPUT);
    }
    env->ReleaseStringUTFChars(jstack, stackChars);
#endif
}

jobject transform(JNIEnv *env, VectorBatch &result)
{
    int32_t vecCount = result.GetVectorCount();
    long vecAddresses[vecCount];
    int32_t encodings[vecCount];
    int32_t dataTypeIds[vecCount];
    long valueBufAddrs[vecCount];
    long nullBufAddrs[vecCount];
    long offsetsBufAddrs[vecCount];
    for (int i = 0; i < vecCount; ++i) {
        BaseVector *vector = result.Get(i);
        vecAddresses[i] = reinterpret_cast<uintptr_t>(vector);
        dataTypeIds[i] = vector->GetTypeId();
        encodings[i] = vector->GetEncoding();
        // By default, all 3 buf arrays will have a value,
        // if not, it will be 0, which means a null pointer.
        valueBufAddrs[i] = reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetValues(vector));
        nullBufAddrs[i] = reinterpret_cast<uintptr_t>(omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vector));
        offsetsBufAddrs[i] = reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetOffsetsAddr(vector));
    }

    // set vector addresses parameter to vector batch construct.
    jlongArray jVecAddresses = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecAddresses, 0, vecCount, vecAddresses);

    // set vector encoding
    jintArray jVecEncodingIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jVecEncodingIds, 0, vecCount, encodings);

    // set vector type ids parameter to vector batch construct.
    jintArray jDataTypeIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jDataTypeIds, 0, vecCount, dataTypeIds);

    // set vector value buf address
    jlongArray jVecValueBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecValueBufAddrs, 0, vecCount, valueBufAddrs);

    // set vec null buf address
    jlongArray jVecNullBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecNullBufAddrs, 0, vecCount, nullBufAddrs);

    // set vec offsets buf address
    jlongArray jVecOffsetsBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecOffsetsBufAddrs, 0, vecCount, offsetsBufAddrs);

    // create vector batch java object.
    jobject obj = env->NewObject(vecBatchCls, vecBatchInitMethodId, (jlong)((int64_t)(&result)), jVecAddresses,
        jVecValueBufAddrs, jVecNullBufAddrs, jVecOffsetsBufAddrs, jVecEncodingIds, jDataTypeIds, result.GetRowCount());
    return obj;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInputNative(JNIEnv *env, jobject jObj,
    jlong jOperatorAddress, jlong jVecBatchAddress)
{
    int32_t errNo = 0;
    JNI_METHOD_START
    auto *vecBatch = (VectorBatch *)jVecBatchAddress;
    auto *nativeOperator = (Operator *)jOperatorAddress;
    RecordInputVectorsStack(vecBatch, env);
    errNo = nativeOperator->AddInput(vecBatch);
    JNI_METHOD_END(errNo)
    return errNo;
}

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutputNative(JNIEnv *env, jobject jObj,
    jlong jOperatorAddr)
{
    auto *nativeOperator = (Operator *)jOperatorAddr;
    VectorBatch *outputVecBatch = nullptr;
    JNI_METHOD_START
    nativeOperator->GetOutput(&outputVecBatch);
    JNI_METHOD_END_WITH_VECBATCHES(nullptr, outputVecBatch)
    jobject result = nullptr;
    if (outputVecBatch) {
        RecordOutputVectorsStack(*outputVecBatch, env);
        result = transform(env, *outputVecBatch);
    }
    return env->NewObject(omniResultsCls, omniResultsInitMethodId, result, nativeOperator->GetStatus());
}

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    close
 * Signature: (J)[Lnova/hetu/omniruntime/operator/void;
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative(JNIEnv *env, jobject jObj,
    jlong jOperatorAddr)
{
    auto *nativeOperator = (Operator *)jOperatorAddr;
    Operator::DeleteOperator(nativeOperator);
}
