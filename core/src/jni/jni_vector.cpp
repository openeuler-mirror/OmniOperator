/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Source File
 */
#include <stdint.h>
#include <src/vector/vector_common.h>
#include <src/vector/dictionary_vector.h>
#include "../util/debug.h"
#include "jni_vector.h"
#include "../memory/memory_pool.h"
#include "../vector/vector_type.h"
#include "../vector/vector_allocator.h"
#include "../vector/long_vector.h"
#include "../vector/varchar_vector.h"
#include "../vector/boolean_vector.h"
#include "../vector/container_vector.h"
#include "../vector/vector_allocator_manager.h"

using namespace omniruntime::vec;

Vector *TransformVector(long vectorAddr);

VectorAllocator *TransformAllocator(long allocatorAddr);

jobject transformBaseVectorToByteBuffer(JNIEnv *env, void *addr, int sizeInBytes);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative
        (JNIEnv *env, jclass jcls, jint jCapacityInBytes, jint jValueCount, jint jVectorType, jlong jAllocator)
{
    int64_t nativeVector = 0;
    switch (jVectorType) {
        case OMNI_VEC_TYPE_INT:
            nativeVector = reinterpret_cast<int64_t>(new IntVector(TransformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_LONG:
            nativeVector = reinterpret_cast<int64_t>(new LongVector(TransformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            nativeVector = reinterpret_cast<int64_t>(new DoubleVector(TransformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_BOOLEAN:
            nativeVector = reinterpret_cast<int64_t>(new BooleanVector(TransformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_SHORT:
            break;
        case OMNI_VEC_TYPE_VARCHAR:
            nativeVector = reinterpret_cast<int64_t>(new VarcharVector(TransformAllocator(jAllocator), jCapacityInBytes, jValueCount));
            break;
        default:
            break;
    }

    return nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jStartIndex, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->Slice(jStartIndex, jLength));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative
  (JNIEnv *env, jclass jcls, jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    return reinterpret_cast<int64_t>(nativeVector->CopyPositions(reinterpret_cast<int *>(positions), 0, jLength));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyRegionNative
  (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jPositionOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->CopyRegion(jPositionOffset, jLength));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeAllocator, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    if (nativeVector == nullptr) {
        std::cerr << "free vector native vector is null:" << jNativeVector << std::endl;
    }
    delete nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getAllocatorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->GetAllocator());
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getCapacityInBytesNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetReference()->GetCapacityInBytes();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getSizeNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetSize();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getOffsetNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetPositionOffset();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_setValueCountNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jSize)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    if (jSize < 0 || jSize > nativeVector->GetSize()) {
        std::cerr << "size is error, the range is[0," << nativeVector->GetSize() << "]" << std::endl;
        return jSize;
    }
    nativeVector->SetSize(jSize);
    return jSize;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getTypeNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetType();
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return transformBaseVectorToByteBuffer(env, nativeVector->GetValues(),
                                           nativeVector->GetReference()->GetCapacityInBytes());
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return transformBaseVectorToByteBuffer(env, nativeVector->GetValueNulls(),
                                           nativeVector->GetReference()->GetValueNullChunk()->GetSizeInBytes());
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getPositionNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    ContainerVector* containerVec = reinterpret_cast<ContainerVector*>(jNativeVector);
    return containerVec->getPositionCount();
}

JNIEXPORT jintArray JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getVecTypesNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    ContainerVector* containerVec = reinterpret_cast<ContainerVector*>(jNativeVector);
    auto vecTypes = containerVec->getVecTypes();
    jintArray res = env->NewIntArray(vecTypes.size());
    env->SetIntArrayRegion(res, 0, vecTypes.size(), (int32_t*)vecTypes.data());
    return res;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_appendVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVectorDest, jint jOffSet, jlong jNativeVectorSrc, jint jLength) {
    Vector *nativeVectorSrc = TransformVector(jNativeVectorSrc);
    Vector *nativeVectorDest = TransformVector(jNativeVectorDest);
    nativeVectorDest->Append(nativeVectorSrc, (int32_t) jOffSet, (int32_t) jLength);
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return transformBaseVectorToByteBuffer(env, nativeVector->GetReference()->GetValueOffsetsAddress(),
                                           nativeVector->GetReference()->GetValueOffsetChunk()->GetSizeInBytes());
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_newAllocatorNative
        (JNIEnv *env, jclass jcls, jstring jScopeId)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    return reinterpret_cast<int64_t>(manager.GetOrCreateAllocator(GLOBAL_SCOPE_NAME));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_freeAllocatorNative
        (JNIEnv *env, jclass jcls, jlong jAllocator)
{
    VectorAllocator *allocator = TransformAllocator(jAllocator);
    allocator->FreeAllVectors();
    return 0;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getGlobalAllocatorNative
        (JNIEnv *env, jclass jcls)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    return reinterpret_cast<int64_t>(manager.GetOrCreateAllocator(GLOBAL_SCOPE_NAME));
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative
        (JNIEnv *env, jclass jcls, jint jVecCount)
{
    VectorBatch *vecBatch = new VectorBatch(jVecCount);
    return (int64_t) vecBatch;
}

void Java_nova_hetu_omniruntime_vector_VecBatch_freeVectorBatchNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress)
{
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    delete vecBatch;
}

jint Java_nova_hetu_omniruntime_vector_VecBatch_getVectorCountNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress)
{
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    return vecBatch->GetVectorCount();
}

void Java_nova_hetu_omniruntime_vector_VecBatch_setVectorNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress, jint jVecIndex, jlong jVecAddress)
{
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    vecBatch->SetVector(jVecIndex, (Vector *) jVecAddress);
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_getVectorNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress, jint jVecIndex)
{
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    return (int64_t) vecBatch->GetVector(jVecIndex);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getDictionaryNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    Vector *dictionary = reinterpret_cast<DictionaryVector *>(nativeVector)->GetDictionary();
    return reinterpret_cast<jlong>(dictionary);
}

JNIEXPORT jintArray JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getIdsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    DictionaryVector *dictionaryVector = reinterpret_cast<DictionaryVector *>(nativeVector);
    int32_t *ids = dictionaryVector->GetIds();
    int32_t idsCount = dictionaryVector->GetIdsCount();

    jintArray jResult = env->NewIntArray(idsCount);
    jint *result = env->GetIntArrayElements(jResult, nullptr);
    for (int32_t i = 0; i < idsCount; i++) {
        result[i] = ids[i];
    }
    env->ReleaseIntArrayElements(jResult, result, 0);
    return jResult;
}

Vector *TransformVector(long vectorAddr)
{
    Vector *nativeVector = reinterpret_cast<Vector *>(vectorAddr);
    ASSERT(nativeVector != nullptr);
    return nativeVector;
}

VectorAllocator *TransformAllocator(long allocatorAddr)
{
    VectorAllocator *nativeAllocator = reinterpret_cast<VectorAllocator *>(allocatorAddr);
    ASSERT(nativeAllocator != nullptr);
    return nativeAllocator;
}

jobject transformBaseVectorToByteBuffer(JNIEnv *env, void *addr, int sizeInBytes)
{
    return env->NewDirectByteBuffer(addr, sizeInBytes);
}
