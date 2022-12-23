/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Source File
 */
#include "jni_vector.h"
#include <cstdint>
#include "util/debug.h"
#include "memory/memory_pool.h"
#include "vector/vector_common.h"
#include "type/data_type_serializer.h"
#include "vector/vector_helper.h"
#include "jni_common_def.h"
#include "jni_vector_loader.h"
#include "memory/base_allocator.h"

using namespace omniruntime::vec;
using namespace omniruntime::mem;

static Vector *TransformVector(long vectorAddr)
{
    Vector *nativeVector = reinterpret_cast<Vector *>(vectorAddr);
    return nativeVector;
}

static VectorAllocator *TransformAllocator(long allocatorAddr)
{
    VectorAllocator *nativeAllocator = reinterpret_cast<VectorAllocator *>(allocatorAddr);
    return nativeAllocator;
}

void RecordVectorStack(Vector *vector, VecOpType opType, JNIEnv *env)
{
#ifdef DEBUG_VECTOR
    jstring jstack = (jstring)env->CallStaticObjectMethod(traceUtilCls, traceUtilStackMethodId);
    auto stackChars = env->GetStringUTFChars(jstack, JNI_FALSE);
    std::string stack(stackChars);
    vector->RecordStack(stack, opType);
    env->ReleaseStringUTFChars(jstack, stackChars);
#endif
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative(JNIEnv *env, jclass jcls,
    jlong jAllocator, jint jCapacityInBytes, jint jValueCount, jint jVectorEncodingId, jint jVectorTypeId)
{
    Vector *vector;
    JNI_METHOD_START
    vector = VectorHelper::CreateVector(TransformAllocator(jAllocator), jVectorEncodingId, jVectorTypeId,
        jCapacityInBytes, jValueCount);
    RecordVectorStack(vector, VecOpType::JNI_NEW, env);
    JNI_METHOD_END(0)
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jStartIndex, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    RecordVectorStack(nativeVector, VecOpType::JNI_SLICE_SRC, env);
    Vector *sliceVector;
    JNI_METHOD_START
    sliceVector = nativeVector->Slice(jStartIndex, jLength);
    JNI_METHOD_END(0)
    RecordVectorStack(sliceVector, VecOpType::JNI_SLICE, env);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(sliceVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    RecordVectorStack(nativeVector, VecOpType::JNI_COPY_POSITIONS_SRC, env);
    Vector *copyVector;
    JNI_METHOD_START
    copyVector = nativeVector->CopyPositions(reinterpret_cast<int *>(positions), 0, jLength);
    JNI_METHOD_END(0)
    RecordVectorStack(copyVector, VecOpType::JNI_COPY_POSITIONS, env);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(copyVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyRegionNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jPositionOffset, jint jLength)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    RecordVectorStack(nativeVector, VecOpType::JNI_COPY_REGION_SRC, env);
    Vector *copyVector;
    JNI_METHOD_START
    copyVector = nativeVector->CopyRegion(jPositionOffset, jLength);
    JNI_METHOD_END(0)
    RecordVectorStack(copyVector, VecOpType::JNI_COPY_REGION, env);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(copyVector));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    if (nativeVector == nullptr) {
        std::cerr << "free vector native vector is null:" << jNativeVector << std::endl;
    }
    RecordVectorStack(nativeVector, VecOpType::JNI_FREE, env);
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
    return containerVec->GetPositionCount();
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
    jlong jNativeVectorDest, jint jOffSet, jlong jNativeVectorSrc, jint jLength)
{
    Vector *nativeVectorSrc = TransformVector(jNativeVectorSrc);
    Vector *nativeVectorDest = TransformVector(jNativeVectorDest);
    JNI_METHOD_START
    nativeVectorDest->Append(nativeVectorSrc, (int32_t)jOffSet, (int32_t)jLength);
    JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative(JNIEnv *env,
    jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(nativeVector->GetValueOffsets()));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_newChildAllocatorNative(JNIEnv *env, jclass jcls,
    jlong jNativeParent, jstring jScopeId, jlong jLimit, jlong jReservation)
{
    auto scope = env->GetStringUTFChars(jScopeId, JNI_FALSE);
    auto *parent = TransformAllocator(jNativeParent);
    VectorAllocator *child;
    JNI_METHOD_START
    child = static_cast<VectorAllocator *>(parent->NewChildAllocator(scope, jLimit, jReservation));
    JNI_METHOD_END(0)
    env->ReleaseStringUTFChars(jScopeId, scope);
    return reinterpret_cast<uintptr_t>(child);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_freeAllocatorNative(JNIEnv *env, jclass jcls,
    jlong jAllocator)
{
    VectorAllocator *allocator = TransformAllocator(jAllocator);
    delete allocator;
    return 0;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_setLimitNative(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator, jlong jLimit)
{
    TransformAllocator(jNativeAllocator)->SetLimit(jLimit);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getLimitNative(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator)
{
    return TransformAllocator(jNativeAllocator)->GetLimit();
}

JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getScopeNative(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator)
{
    std::string nativeScope = TransformAllocator(jNativeAllocator)->GetScope();
    jstring scope = env->NewStringUTF(nativeScope.c_str());
    return scope;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_setRootAllocatorLimitNative(JNIEnv *env,
    jclass jcls, jlong jLimit)
{
    omniruntime::mem::SetRootAllocatorLimit(jLimit);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getAllocatedMemoryNative(JNIEnv *env,
    jclass jcls, jlong jNativeAllocator)
{
    return TransformAllocator(jNativeAllocator)->GetAllocatedMemory();
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getParentAllocator(JNIEnv *env, jclass jcls,
    jlong jNativeAllocator)
{
    return reinterpret_cast<uintptr_t>(TransformAllocator(jNativeAllocator)->GetParentAllocator());
}

JNIEXPORT jlongArray JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getChildAllocatorsNative(JNIEnv *env,
    jclass jcls, jlong jNativeParent)
{
    std::vector<BaseAllocator *> childAllocators = TransformAllocator(jNativeParent)->GetChildAllocators();
    auto length = static_cast<int32_t>(childAllocators.size());
    jlongArray nativeChilds = (env)->NewLongArray(length);
    long childAddrs[length];
    for (int32_t i = 0; i < length; i++) {
        childAddrs[i] = reinterpret_cast<uintptr_t>(childAllocators[i]);
    }
    env->SetLongArrayRegion(nativeChilds, 0, length, childAddrs);
    return nativeChilds;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getGlobalVectorAllocator(JNIEnv *env,
    jclass jcls)
{
    int64_t globalVectorAllocatorAddr = 0;
    JNI_METHOD_START
    globalVectorAllocatorAddr = reinterpret_cast<uintptr_t>(omniruntime::vec::GetProcessGlobalVecAllocator());
    JNI_METHOD_END(globalVectorAllocatorAddr)
    return globalVectorAllocatorAddr;
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

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_setDictionaryNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jlong jNativeDictionary)
{
    DictionaryVector *nativeVector = reinterpret_cast<DictionaryVector *>(jNativeVector);
    Vector *nativeDictionary = TransformVector(jNativeDictionary);
    JNI_METHOD_START
    nativeVector->SetDictionary(nativeDictionary->Slice(0, nativeDictionary->GetSize()));
    JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_LazyVec_setLazyLoaderNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jobject jLazyLoader)
{
    LazyVector *lazyVector = reinterpret_cast<LazyVector *>(jNativeVector);
    JniVectorLoader *loader = new JniVectorLoader(env, jLazyLoader);
    lazyVector->SetLoader(loader);
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getVecEncodingNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetEncoding();
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VarcharVec_expandDataCapacity(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jToCapacityInBytes)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    int64_t valueAddr = 0;
    JNI_METHOD_START
    valueAddr = nativeVector->ExpandDataCapacity(jToCapacityInBytes);
    JNI_METHOD_END(valueAddr)
    return valueAddr;
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_setNullFlagNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jboolean jHasNull)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    nativeVector->SetNullFlag(jHasNull);
}

JNIEXPORT jboolean JNICALL Java_nova_hetu_omniruntime_vector_Vec_mayHaveNullNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->MayHaveNull();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getNullCountNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    Vector *nativeVector = TransformVector(jNativeVector);
    return nativeVector->GetNullCount();
}
