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
    // set vector addresses parameter to vector batch construct.
    jlongArray jVecAddresses = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecAddresses, 0, vecCount, (const jlong *)result.GetVectors());
    long allocators[vecCount];
    int32_t capacityInBytes[vecCount];
    int32_t offsets[vecCount];
    int32_t encodings[vecCount];
    long valueBufAddrs[vecCount];
    long nullBufAddrs[vecCount];
    long offsetBufAddrs[vecCount];
    for (int i = 0; i < vecCount; ++i) {
        Vector *vector = result.GetVector(i);
        allocators[i] = (long)vector->GetAllocator();
        capacityInBytes[i] = vector->GetCapacityInBytes();
        offsets[i] = vector->GetPositionOffset();
        encodings[i] = vector->GetEncoding();
        // By default, all 3 buf arrays will have a value,
        // if not, it will be 0, which means a null pointer.
        valueBufAddrs[i] = reinterpret_cast<uintptr_t>(vector->GetValues());
        nullBufAddrs[i] = reinterpret_cast<uintptr_t>(vector->GetValueNulls());
        offsetBufAddrs[i] = reinterpret_cast<uintptr_t>(vector->GetValueOffsets());
    }
    // set vector allocators parameter to vector batch construct.
    jlongArray jVecAllocatorAddresses = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecAllocatorAddresses, 0, vecCount, allocators);

    // set vector capacityInBytes parameter to vector batch construct.
    jintArray jVecCapacityInBytes = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jVecCapacityInBytes, 0, vecCount, capacityInBytes);

    // set vector offsets ids parameter to vector batch construct.
    jintArray jVecOffsets = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jVecOffsets, 0, vecCount, offsets);

    // set vector encoding
    jintArray jVecEncodingIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jVecEncodingIds, 0, vecCount, encodings);

    // set vector type ids parameter to vector batch construct.
    jintArray jDataTypeIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jDataTypeIds, 0, vecCount, (const jint *)result.GetVectorTypeIds());

    // set vector value buf address
    jlongArray jVecValueBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecValueBufAddrs, 0, vecCount, valueBufAddrs);

    // set vec null buf address
    jlongArray jVecNullBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecNullBufAddrs, 0, vecCount, nullBufAddrs);

    // set vec offset buf address
    jlongArray jVecOffsetBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecOffsetBufAddrs, 0, vecCount, offsetBufAddrs);

    // create vector batch java object.
    jobject obj = env->NewObject(vecBatchCls, vecBatchInitMethodId, (jlong)((int64_t)(&result)), jVecAddresses,
        jVecValueBufAddrs, jVecNullBufAddrs, jVecOffsetBufAddrs, jVecAllocatorAddresses, jVecCapacityInBytes,
        jVecOffsets, jVecEncodingIds, jDataTypeIds, result.GetRowCount());
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
