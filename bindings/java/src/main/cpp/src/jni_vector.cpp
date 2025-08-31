/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
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
#include "util/data_type_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::mem;

static ALWAYS_INLINE BaseVector *TransformVector(long vectorAddr)
{
    return reinterpret_cast<BaseVector *>(vectorAddr);
}

#ifdef TRACE
static void RecordStack(BaseVector *vector, JNIEnv *env)
{
    jstring jstack = (jstring)env->CallStaticObjectMethod(traceUtilCls, traceUtilStackMethodId);
    auto stackChars = env->GetStringUTFChars(jstack, JNI_FALSE);
    std::string stack(stackChars);
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    // replace c++ stack with java stack after vector is created.
    threadMemoryTrace->ReplaceVectorTracedLog(reinterpret_cast<uintptr_t>(vector), stack);
    env->ReleaseStringUTFChars(jstack, stackChars);
}
#endif

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative(JNIEnv *env, jclass jcls,
    jint jValueCount, jint jVectorEncodingId, jint jVectorTypeId, jint jCapacityInBytes)
{
    BaseVector *vector = nullptr;
    JNI_METHOD_START
    vector = VectorHelper::CreateVector(jVectorEncodingId, jVectorTypeId, jValueCount, jCapacityInBytes);
    if (UNLIKELY(vector == nullptr)) {
        throw omniruntime::exception::OmniException("CREATE_FLAT_VECTOR_FAILED",
            "return a null pointer when creating flat vector");
    }
    JNI_METHOD_END(0)
#ifdef TRACE
    RecordStack(vector, env);
#endif
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newDictionaryVectorNative(JNIEnv *env, jclass jcls,
    jlong jDictionaryNativeVector, jintArray jIds, jint size, jint dataTypeId)
{
    BaseVector *dictionaryVector = TransformVector(jDictionaryNativeVector);
    jint idsArray[size];
    env->GetIntArrayRegion(jIds, 0, size, idsArray);
    jint *ids = idsArray;
    BaseVector *vector = nullptr;
    JNI_METHOD_START
    vector = VectorHelper::CreateDictionaryVector(ids, size, dictionaryVector, dataTypeId);
    if (UNLIKELY(vector == nullptr)) {
        throw omniruntime::exception::OmniException("CREATE_DICTIONARY_VECTOR_FAILED",
            "return a null pointer when creating dictionary vector");
    }
    JNI_METHOD_END(0)
#ifdef TRACE
    RecordStack(vector, env);
#endif
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jint jStartIndex, jint jLength)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    BaseVector *sliceVector = nullptr;
    JNI_METHOD_START
    sliceVector = VectorHelper::SliceVector(nativeVector, jStartIndex, jLength);
    JNI_METHOD_END(0)
#ifdef TRACE
    RecordStack(sliceVector, env);
#endif
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(sliceVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    BaseVector *copyVector = nullptr;
    JNI_METHOD_START
    copyVector = VectorHelper::CopyPositionsVector(nativeVector, reinterpret_cast<int *>(positions), 0, jLength);
    JNI_METHOD_END(0)
#ifdef TRACE
    RecordStack(copyVector, env);
#endif
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
    DataTypeId typeId = nativeVector->GetTypeId();
    if (typeId != omniruntime::type::OMNI_VARCHAR && typeId != omniruntime::type::OMNI_CHAR) {
        throw omniruntime::exception::OmniException("vector type is no supported",
            "the interface only supports varchar/char vector.");
    }
    auto *varCharVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(nativeVector);
    return omniruntime::vec::unsafe::UnsafeStringVector::GetContainer(varCharVector)->GetCapacityInBytes();
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
    omniruntime::vec::unsafe::UnsafeBaseVector::SetSize(nativeVector, jSize);
    return jSize;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative(JNIEnv *env, jclass jlcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetValues(nativeVector));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    return reinterpret_cast<uintptr_t>(omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(nativeVector));
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
    containerVec->SetDataTypes(omniruntime::type::Deserialize(dataTypeString).Get());
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
    BaseVector *nativeVectorSrc = TransformVector(jNativeVectorSrc);
    BaseVector *nativeVectorDest = TransformVector(jNativeVectorDest);
    JNI_METHOD_START
    VectorHelper::AppendVector(nativeVectorDest, (int32_t)jOffSet, nativeVectorSrc, (int32_t)jLength);
    JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative(JNIEnv *env,
    jclass jcls, jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    auto offsetsAddr = VectorHelper::UnsafeGetOffsetsAddr(nativeVector);
    if (UNLIKELY(offsetsAddr == nullptr)) {
        throw omniruntime::exception::OmniException("GET_OFFSETS_FAILED",
            "return a null pointer when getting offsets address");
    }
    return reinterpret_cast<uintptr_t>(offsetsAddr);
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

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_memory_MemoryManager_memoryReclamationNative(JNIEnv *env, jclass jcls)
{
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    if (threadMemoryTrace->HasMemoryLeak()) {
        threadMemoryTrace->FreeLeakedMemory();
    }
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative(JNIEnv *env, jclass jcls,
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

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_VecBatch_freeVectorBatchNative(JNIEnv *env, jclass jcls,
    jlong jVecBatchAddress)
{
    VectorBatch *vecBatch = reinterpret_cast<VectorBatch *>(jVecBatchAddress);
    vecBatch->ClearVectors();
    delete vecBatch;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getDictionaryNative(JNIEnv *env, jclass jcls,
    jlong jNativeVector)
{
    BaseVector *nativeVector = TransformVector(jNativeVector);
    auto dictionaryAddr = VectorHelper::UnsafeGetDictionary(nativeVector);
    if (UNLIKELY(dictionaryAddr == nullptr)) {
        throw omniruntime::exception::OmniException("GET_DICTIONARY_NATIVE_FAILED",
            "return a null pointer when getting dictionary address");
    }
    return reinterpret_cast<uintptr_t>(dictionaryAddr);
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
    auto nativeVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(jNativeVector);
    char *newBuffAddress =
        omniruntime::vec::unsafe::UnsafeStringVector::ExpandStringBuffer(nativeVector, jToCapacityInBytes);
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


JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_ComplexVec_getComplexCapacityNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jVecEncoding)
{

}


JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_ComplexVec_newComplexVectorNative
        (JNIEnv *env, jclass jcls, jint jSize, jint jVectorEncodingId, jobjectArray jDataTypes)
{
    BaseVector *vector = nullptr;
    // TODO
    DataType* dataType = nullptr;
    JNI_METHOD_START
        jsize len = env->GetArrayLength(jDataTypes);
        if (len == 0) {
            throw omniruntime::exception::OmniException("INVALID_ARGUMENT", "DataType array is empty");
        }

        std::vector<std::shared_ptr<DataType>> children = DataTypeUtil::ConvertJavaDataTypesToCpp(env, jDataTypes);
        if (jVectorEncodingId == OMNI_ENCODING_MAP) {
            dataType = new MapType(children[0],children[1]);
        } else if (jVectorEncodingId == OMNI_ENCODING_STRUCT) {
            dataType = new RowType(children);
        } else {
            std::string omniExceptionInfo =
                "In function CreateVector, no such encoding type " + std::to_string(jVectorEncodingId);
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }

        vector = VectorHelper::CreateComplexVector(dataType, jSize);
        if (UNLIKELY(vector == nullptr)) {
            throw omniruntime::exception::OmniException("CREATE_COMPLEX_VECTOR_FAILED",
                                                        "return a null pointer when creating complex vector");
        }
    JNI_METHOD_END(0)
#ifdef TRACE
    RecordStack(vector, env);
#endif
    return reinterpret_cast<uintptr_t>(reinterpret_cast<void *>(vector));
}

extern "C"
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_MapVec_getValuesAddrNative
    (JNIEnv *env, jclass jlcls, jlong jNativeVector)
{
    MapVector *nativeVector = reinterpret_cast<MapVector *>(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetValues(nativeVector->GetValueVector().get()));
}

extern "C"
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_MapVec_getKeysAddrNative
    (JNIEnv *env, jclass jlcls, jlong jNativeVector)
{
    MapVector *nativeVector = reinterpret_cast<MapVector *>(jNativeVector);
    return reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetValues(nativeVector->GetKeyVector().get()));
}

extern "C"
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_MapVec_getOffsetNative
    (JNIEnv *env, jclass jlcls, jlong jNativeVector, jlong rowId)
{
    MapVector *nativeVector = reinterpret_cast<MapVector *>(jNativeVector);

    return nativeVector->GetOffset(rowId);
}

extern "C"
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_MapVec_getSizeNative
    (JNIEnv *env, jclass jlcls, jlong jNativeVector, jlong rowId)
{
    MapVector *nativeVector = reinterpret_cast<MapVector *>(jNativeVector);
    return nativeVector->GetSize(rowId);
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_StructVec_addVecNative
  (JNIEnv *env, jclass jcls, jlong rowVecAddr, jint index, jlong addedVecAddr)
{
    auto rowVec = reinterpret_cast<RowVector *>(rowVecAddr);
    auto addedVec =  reinterpret_cast<BaseVector *>(addedVecAddr);
    rowVec->Add(index, addedVec);
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_MapVec_AddKeysNative
(JNIEnv *env, jclass jcls, jlong mapVecAddr, jlong keysAddr) {
    auto mapVec = reinterpret_cast<MapVector *>(mapVecAddr);
    auto keys =  reinterpret_cast<BaseVector *>(keysAddr);
    mapVec->AddKeys(keys);
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_MapVec_AddValuesNative
  (JNIEnv *env, jclass jcls, jlong mapVecAddr, jlong valuesAddr) {
    auto mapVec = reinterpret_cast<MapVector *>(mapVecAddr);
    auto values =  reinterpret_cast<BaseVector *>(valuesAddr);
    mapVec->AddValues(values);
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_MapVec_AddOffsetsNative
  (JNIEnv *env, jclass jcls, jlong mapVecAddr, jintArray offsetsAddr) {
    auto mapVec = reinterpret_cast<MapVector *>(mapVecAddr);
    jsize length = env->GetArrayLength(offsetsAddr);
    jint* elements = env->GetIntArrayElements(offsetsAddr, nullptr);
    for (jsize i = 0; i < length; i++) {
        mapVec->SetOffset(i, elements[i]);
    }
}