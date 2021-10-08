/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Source File
 */
#include "jni_vector.h"
#include <stdint.h>
#include "../util/debug.h"
#include "../memory/memory_pool.h"
#include "../vector/vector_common.h"
#include "../vector/vector_type_serializer.h"
#include "../vector/vector_helper.h"

using namespace omniruntime::vec;

Vector *TransformVector(long vectorAddr);

VectorAllocator *TransformAllocator(long allocatorAddr);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative(JNIEnv *env, jclass jcls,
    jlong jAllocator, jint jCapacityInBytes, jint jValueCount, jint jVectorTypeId)
{
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(
        VectorHelper::CreateVector(TransformAllocator(jAllocator), jVectorTypeId, jCapacityInBytes, jValueCount)));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newDictionaryVectorNative(JNIEnv *env, jclass jcls,
    jlong jDictionary, jintArray jIds)
{
    Vector *dictionary = TransformVector(jDictionary);
    jint *ids = env->GetIntArrayElements(jIds, JNI_FALSE);
    jsize idsCount = env->GetArrayLength(jIds);
    DictionaryVector *dictionaryVector = std::make_unique<DictionaryVector>(dictionary, ids, idsCount).release();
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(dictionaryVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jStartIndex, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->Slice(jStartIndex, jLength)));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    return reinterpret_cast<uintptr_t>(
        reinterpret_cast<void *>(nativeVector->CopyPositions(reinterpret_cast<int *>(positions), 0, jLength)));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyRegionNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jPositionOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->CopyRegion(jPositionOffset, jLength)));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    if (nativeVector == nullptr) {
        std::cerr << "free vector native vector is null:" << jNativeVector << std::endl;
    }
    delete nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getAllocatorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->GetAllocator()));
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getCapacityInBytesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetCapacityInBytes();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getSizeNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetSize();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_setSizeNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jSize)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    if (jSize < 0 || jSize > nativeVector->GetSize()) {
        std::cerr << "size is error, the range is[0," << nativeVector->GetSize() << "]" << std::endl;
        return jSize;
    }
    nativeVector->SetSize(jSize);
    return jSize;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getOffsetNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetPositionOffset();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getTypeIdNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetTypeId();
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->GetValues()));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->GetValueNulls()));
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getPositionNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    ContainerVector *containerVec = reinterpret_cast<ContainerVector *>(jNativeVector);
    return containerVec->getPositionCount();
}

JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getVecTypesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    ContainerVector *containerVec = reinterpret_cast<ContainerVector *>(jNativeVector);
    auto vecTypes = containerVec->getVecTypes();
    return env->NewStringUTF(Serialize(vecTypes).data());
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_appendVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVectorDest, jint jOffSet, jlong jNativeVectorSrc, jint jLength)
{
    Vector *nativeVectorSrc = TransformVector(jNativeVectorSrc);
    Vector *nativeVectorDest = TransformVector(jNativeVectorDest);
    nativeVectorDest->Append(nativeVectorSrc, (int32_t)jOffSet, (int32_t)jLength);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative(JNIEnv *env,
    jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->GetValueOffsets()));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_newAllocatorNative(JNIEnv *env, jclass jcls,
    jstring jScopeId)
{
    auto scope = env->GetStringUTFChars(jScopeId, JNI_FALSE);
    VectorAllocator *vectorAllocator = VectorAllocatorFactory::GetOrCreateAllocator(scope);
    env->ReleaseStringUTFChars(jScopeId, scope);
    return reinterpret_cast<uintptr_t>(vectorAllocator);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_freeAllocatorNative(JNIEnv *env, jclass jcls,
    jlong jAllocator)
{
    VectorAllocator *allocator = TransformAllocator(jAllocator);
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    return 0;
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative(JNIEnv *env, jclass jcls,
    jlongArray jVectorAddresses, jint rRowCount)
{
    jlong *vecAddresses = env->GetLongArrayElements(jVectorAddresses, JNI_FALSE);
    jsize vecCount = env->GetArrayLength(jVectorAddresses);
    VectorBatch *vecBatch = new VectorBatch(vecCount, rRowCount);
    for (int i = 0; i < vecCount; ++i) {
        vecBatch->SetVector(i, (Vector *)vecAddresses[i]);
    }
    env->ReleaseLongArrayElements(jVectorAddresses, vecAddresses, JNI_ABORT);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vecBatch));
}

void Java_nova_hetu_omniruntime_vector_VecBatch_freeVectorBatchNative(JNIEnv *env, jclass jcls, jlong jVecBatchAddress)
{
    VectorBatch *vecBatch = (VectorBatch *)jVecBatchAddress;
    delete vecBatch;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getDictionaryNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(
        reinterpret_cast<void *>(reinterpret_cast<DictionaryVector *>(nativeVector)->GetDictionary()));
}

JNIEXPORT jintArray JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getIdsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
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
    return nativeVector;
}

VectorAllocator *TransformAllocator(long allocatorAddr)
{
    VectorAllocator *nativeAllocator = reinterpret_cast<VectorAllocator *>(allocatorAddr);
    return nativeAllocator;
}
