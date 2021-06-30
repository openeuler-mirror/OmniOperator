//
// Created by root on 5/26/21.
//
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
#include "../vector/vector_allocator_manager.h"

Vector *transformVector(long vectorAddr);

VectorAllocator *transformAllocator(long allocatorAddr);

jobject transformBaseVectorToByteBuffer(JNIEnv *env, void *addr, int sizeInBytes);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative
        (JNIEnv *env, jclass jcls, jint jCapacityInBytes, jint jValueCount, jint jVectorType, jlong jAllocator) {
    int64_t nativeVector = 0;
    switch (jVectorType) {
        case OMNI_VEC_TYPE_INT:
            nativeVector = reinterpret_cast<int64_t>(new IntVector(transformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_LONG:
            nativeVector = reinterpret_cast<int64_t>(new LongVector(transformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            nativeVector = reinterpret_cast<int64_t>(new DoubleVector(transformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_BOOLEAN:
            nativeVector = reinterpret_cast<int64_t>(new BooleanVector(transformAllocator(jAllocator), jValueCount));
            break;
        case OMNI_VEC_TYPE_SHORT:
            break;
        case OMNI_VEC_TYPE_VARCHAR:
            nativeVector = reinterpret_cast<int64_t>(new VarcharVector(transformAllocator(jAllocator), jCapacityInBytes, jValueCount));
            break;
        default:
            break;
    }

    return nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jStartIndex, jint jLength) {
    Vector *nativeVector = transformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->slice(jStartIndex, jLength));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative
  (JNIEnv *env, jclass jcls, jlong jNativeVector, jintArray jPositions, jint jOffset, jint jLength)
{
    Vector *nativeVector = transformVector(jNativeVector);
    jint positionArray[jLength];
    env->GetIntArrayRegion(jPositions, jOffset, jLength, positionArray);
    jint *positions = positionArray;
    return reinterpret_cast<int64_t>(nativeVector->copyPositions(reinterpret_cast<int *>(positions), 0, jLength));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyRegionNative
  (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jPositionOffset, jint jLength)
{
    Vector *nativeVector = transformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->copyRegion(jPositionOffset, jLength));
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeAllocator, jlong jNativeVector) {
    std::cout << "from jni native vector is:" << jNativeVector << std::endl;
    Vector *nativeVector = transformVector(jNativeVector);
    std::cout << "affter transform native vector is:" << jNativeVector << std::endl;
    if (nativeVector == nullptr) {
        std::cerr << "free vector native vector is null:" << jNativeVector << std::endl;
    }
    delete nativeVector;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getAllocatorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return reinterpret_cast<int64_t>(nativeVector->getAllocator());
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getCapacityInBytesNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return nativeVector->getReference()->getCapacityInBytes();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getSizeNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return nativeVector->getSize();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getOffsetNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return nativeVector->getPositionOffset();
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_setValueCountNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector, jint jSize) {
    Vector *nativeVector = transformVector(jNativeVector);
    if (jSize < 0 || jSize > nativeVector->getSize()) {
        std::cerr << "size is error, the range is[0," << nativeVector->getSize() << "]" << std::endl;
        return jSize;
    }
    nativeVector->setSize(jSize);
    return jSize;
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getTypeNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return nativeVector->getType();
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return transformBaseVectorToByteBuffer(env, nativeVector->getReference()->getValuesAddress(),
                                           nativeVector->getReference()->getCapacityInBytes());
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector); 
    return transformBaseVectorToByteBuffer(env, nativeVector->getValueNulls(),
                                           nativeVector->getReference()->getValueNullChunk()->getSizeInBytes());
}

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_appendVectorNative
        (JNIEnv *env, jclass jcls, jlong jNativeVectorDest, jint jOffSet, jlong jNativeVectorSrc, jint jLength) {
    Vector *nativeVectorSrc = transformVector(jNativeVectorSrc);
    Vector *nativeVectorDest = transformVector(jNativeVectorDest);
    nativeVectorDest->append(nativeVectorSrc, (int32_t) jOffSet, (int32_t) jLength);
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector) {
    Vector *nativeVector = transformVector(jNativeVector);
    return transformBaseVectorToByteBuffer(env, nativeVector->getReference()->getValueOffsetsAddress(),
                                           nativeVector->getReference()->getValueOffsetChunk()->getSizeInBytes());
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_newAllocatorNative
        (JNIEnv *env, jclass jcls, jstring jScopeId) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    return reinterpret_cast<int64_t>(manager.getOrCreateAllocator(GLOBAL_SCOPE_NAME));
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_freeAllocatorNative
        (JNIEnv *env, jclass jcls, jlong jAllocator) {
    VectorAllocator *allocator = transformAllocator(jAllocator);
    allocator->freeAllVectors();
    return 0;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_getGlobalAllocatorNative
        (JNIEnv *env, jclass jcls) {
    VectorAllocatorManager manager = VectorAllocatorManager::getInstance();
    return reinterpret_cast<int64_t>(manager.getOrCreateAllocator(GLOBAL_SCOPE_NAME));
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative
        (JNIEnv *env, jclass jcls, jint jVecCount) {
    VectorBatch *vecBatch = new VectorBatch(jVecCount);
    return (int64_t) vecBatch;
}

void Java_nova_hetu_omniruntime_vector_VecBatch_freeVectorBatchNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress) {
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    delete vecBatch;
}

jint Java_nova_hetu_omniruntime_vector_VecBatch_getVectorCountNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress) {
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    return vecBatch->getVectorCount();
}

void Java_nova_hetu_omniruntime_vector_VecBatch_setVectorNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress, jint jVecIndex, jlong jVecAddress) {
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    vecBatch->setVector(jVecIndex, (Vector *) jVecAddress);
}

jlong Java_nova_hetu_omniruntime_vector_VecBatch_getVectorNative
        (JNIEnv *env, jclass jcls, jlong jVecBatchAddress, jint jVecIndex) {
    VectorBatch *vecBatch = (VectorBatch *) jVecBatchAddress;
    return (int64_t) vecBatch->getVector(jVecIndex);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getDictionaryNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = transformVector(jNativeVector);
    Vector *dictionary = reinterpret_cast<DictionaryVector *>(nativeVector)->getDictionary();
    return reinterpret_cast<jlong>(dictionary);
}

JNIEXPORT jintArray JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getIdsNative
        (JNIEnv *env, jclass jcls, jlong jNativeVector)
{
    Vector *nativeVector = transformVector(jNativeVector);
    DictionaryVector *dictionaryVector = reinterpret_cast<DictionaryVector *>(nativeVector);
    int32_t *ids = dictionaryVector->getIds();
    int32_t idsCount = dictionaryVector->getIdsCount();

    jintArray jResult = env->NewIntArray(idsCount);
    jint *result = env->GetIntArrayElements(jResult, nullptr);
    for (int32_t i = 0; i < idsCount; i++) {
        result[i] = ids[i];
    }
    env->ReleaseIntArrayElements(jResult, result, 0);
    return jResult;
}

Vector *transformVector(long vectorAddr) {
    Vector *nativeVector = reinterpret_cast<Vector *>(vectorAddr);
    ASSERT(nativeVector != nullptr);
    return nativeVector;
}

VectorAllocator *transformAllocator(long allocatorAddr) {
    VectorAllocator *nativeAllocator = reinterpret_cast<VectorAllocator *>(allocatorAddr);
    ASSERT(nativeAllocator != nullptr);
    return nativeAllocator;
}

jobject transformBaseVectorToByteBuffer(JNIEnv *env, void *addr, int sizeInBytes) {
    return env->NewDirectByteBuffer(addr, sizeInBytes);
}
