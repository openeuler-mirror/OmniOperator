/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Source File
 */
#include "jni_vector.h"
#include <cstdint>
#include "memory/memory_pool.h"
#include "vector/vector_batch.h"
#include "vector/unsafe_vector.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include "jni_common_def.h"
#include "operator/aggregation/container_vector.h"
#include "type/data_type_serializer.h"
#include "memory/thread_memory_manager.h"

using namespace omniruntime::vec;

static BaseVector *TransformVector(long vectorAddr)
{
    BaseVector *nativeVector = reinterpret_cast<BaseVector *>(vectorAddr);
    return nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative(JNIEnv *env, jclass jcls,
    jint jValueCount, jint jVectorEncodingId, jint jVectorTypeId, jint jCapacityInBytes)
{
    BaseVector *vector;
    JNI_METHOD_START
    vector = VectorHelper::CreateVector(jVectorEncodingId, jVectorTypeId, jValueCount, jCapacityInBytes).release();
    JNI_METHOD_END(0)
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newDictionaryVectorNative(JNIEnv *env, jclass jcls,
    jlong jDictionaryNativeVector, jintArray jIds, jint size, jint dataTypeId)
{
    BaseVector *dictionaryVector = TransformVector(jDictionaryNativeVector);
    jint idsArray[size];
    env->GetIntArrayRegion(jIds, 0, size, idsArray);
    jint *ids = idsArray;
    BaseVector *vector;
    JNI_METHOD_START
    vector = VectorHelper::CreateDictionaryVector(ids, size, dictionaryVector, dataTypeId).release();
    JNI_METHOD_END(0)
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint dataTypeId, jint jStartIndex, jint jLength)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    BaseVector *sliceVector;
    JNI_METHOD_START
    sliceVector = VectorHelper::SliceVector(nativeVector, dataTypeId, jStartIndex, jLength).release();
    JNI_METHOD_END(0)
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(sliceVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength, jint dataTypeId)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    BaseVector *copyVector;
    JNI_METHOD_START
    copyVector =
        VectorHelper::CopyPositionsVector(nativeVector, reinterpret_cast<int *>(positions), 0, jLength, dataTypeId)
            .release();
    JNI_METHOD_END(0)
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(copyVector));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    if (nativeVector == nullptr) {
        std::cerr << "free vector native vector is null:" << jNativeVector << std::endl;
    }
    delete nativeVector;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getCapacityInBytesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    if (nativeVector ->GetStringEncoding() != OMNI_LARGE_STRING) {
        // TODO: return the real capacity for fixed width vector
        return 0;
    }
    auto *varCharVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(
            jNativeVector);
    return UnsafeStringVector::GetContainer(varCharVector)->GetCapacityInBytes();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getSizeNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetSize();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_setSizeNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jSize)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    if (jSize < 0 || jSize > nativeVector->GetSize()) {
        std::cerr << "size is error, the range is[0," << nativeVector->GetSize() << "]" << std::endl;
        return jSize;
    }
    UnsafeBaseVector::SetSize(nativeVector, jSize);
    return jSize;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative(JNIEnv *env, jclass jlcls,
    jlong jNativeVector, jint dataTypeId)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::GetValues(nativeVector, dataTypeId));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(UnsafeBaseVector::GetNulls(nativeVector)));
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getPositionNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    ContainerVector *containerVec = reinterpret_cast<ContainerVector *>(jNativeVector);
    return containerVec->GetSize();
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_setDataTypesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jstring dataTypes)
{
    ContainerVector *containerVec = reinterpret_cast<ContainerVector *>(jNativeVector);
    auto dataTypeString = env->GetStringUTFChars(dataTypes, JNI_FALSE);
    containerVec->SetDataTypes(Deserialize(dataTypeString).Get());
    env->ReleaseStringUTFChars(dataTypes, dataTypeString);
}

JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getDataTypesNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    ContainerVector *containerVec = reinterpret_cast<ContainerVector *>(jNativeVector);
    auto &DataTypes = containerVec->GetDataTypes();
    return env->NewStringUTF(Serialize(DataTypes).data());
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_appendVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVectorDest, jint jOffSet, jlong jNativeVectorSrc, jint jLength, jint dataTypeId)
{
    BaseVector *nativeVectorSrc = TransformVector(jNativeVectorSrc);
    BaseVector *nativeVectorDest = TransformVector(jNativeVectorDest);
    JNI_METHOD_START
    VectorHelper::AppendVector(nativeVectorDest, (int32_t)jOffSet, nativeVectorSrc, (int32_t)jLength, dataTypeId);
    JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative(JNIEnv *env,
    jclass jcls, jlong jNativeVector, jint jVectorTypeId)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::GetOffsetsAddr(nativeVector, jVectorTypeId));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_memory_MemoryManager_setGlobalMemoryLimitNative(JNIEnv *env,
    jclass jcls, jlong jLimit)
{
    omniruntime::mem::MemoryManager::SetGlobalMemoryLimit(jLimit);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_memory_MemoryManager_getAllocatedMemoryNative(JNIEnv *env,
    jclass jcls)
{
    auto threadMemoryManager = omniruntime::mem::ThreadMemoryManager::GetThreadMemoryManager();
    int64_t accountedMemory = threadMemoryManager->GetThreadAccountedMemory();
    int64_t untrackedMemory = threadMemoryManager->GetUntrackedMemory();
    return accountedMemory + untrackedMemory;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_memory_MemoryManager_memoryClearNative(JNIEnv *env, jclass jcls)
{
    auto threadMemoryManager = omniruntime::mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative(JNIEnv *env, jclass jcls,
    jlongArray jVectorAddresses, jint rRowCount)
{
    jlong *vecAddresses = env->GetLongArrayElements(jVectorAddresses, JNI_FALSE);
    jsize vecCount = env->GetArrayLength(jVectorAddresses);
    VectorBatch *vecBatch = new VectorBatch(rRowCount);
    for (int i = 0; i < vecCount; ++i) {
        vecBatch->Append(reinterpret_cast<BaseVector *>(vecAddresses[i]));
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
    jlong jNativeVector, jint dataTypeId)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::GetDictionary(nativeVector, dataTypeId));
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getVecEncodingNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetEncoding();
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VarcharVec_expandDataCapacity(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jToCapacityInBytes)
{
    auto nativeVector =
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(jNativeVector);
    char *newBuffAddress = UnsafeStringVector::ExpandStringBuffer(nativeVector, jToCapacityInBytes);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(newBuffAddress));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_setNullFlagNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jboolean jHasNull)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    nativeVector->SetNullFlag(jHasNull);
}

JNIEXPORT jboolean JNICALL Java_nova_hetu_omniruntime_vector_Vec_hasNullNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->HasNull();
}
