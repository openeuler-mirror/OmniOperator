/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef JNI_VECTOR_H
#define JNI_VECTOR_H
#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    newVectorNative
 * Signature: (IIIJ)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_newVectorNative(JNIEnv *, jclass, jlong, jint, jint,
    jint);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    sliceVectorNative
 * Signature: (JII)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_sliceVectorNative(JNIEnv *, jclass, jlong, jint, jint);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    copyPositionsNative
 * Signature: (J[III)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyPositionsNative(JNIEnv *, jclass, jlong, jintArray,
    jint, jint);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    copyRegionNative
 * Signature: (JII)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_copyRegionNative(JNIEnv *, jclass, jlong, jint, jint);
/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    freeVectorNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_freeVectorNative(JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getAllocatorNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getAllocatorNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getCapacityInBytesNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getCapacityInBytesNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getSizeNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getSizeNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    setSizeNative
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_setSizeNative(JNIEnv *, jclass, jlong, jint);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getOffsetNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getOffsetNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getTypeIdNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_Vec_getTypeIdNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getValuesNative
 * Signature: (J)J;
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValuesNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    getValueNullsNative
 * Signature: (J)J;
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_Vec_getValueNullsNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_Vec
 * Method:    appendVectorNative
 * Signature: (JIJI)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_Vec_appendVectorNative(JNIEnv *, jclass, jlong, jint, jlong,
    jint);

/*
 * Class:     nova_hetu_omniruntime_vector_ContainerVec
 * Method:    getPositionNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getPositionNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_ContainerVec
 * Method:    getVecTypesNative
 * Signature: (J)[I;
 */
JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_vector_ContainerVec_getVecTypesNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_VariableWidthVec
 * Method:    getValueOffsetsNative
 * Signature: (J)J;
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VariableWidthVec_getValueOffsetsNative(JNIEnv *, jclass,
    jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_DictionaryVec
 * Method:    setDictionaryNative
 * Signature: (J)[I
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_setDictionaryNative(JNIEnv *, jclass,
    jlong, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_VecAllocator
 * Method:    newAllocatorNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_newAllocatorNative(JNIEnv *, jclass, jstring);

/*
 * Class:     nova_hetu_omniruntime_vector_VecAllocator
 * Method:    freeAllocatorNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecAllocator_freeAllocatorNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_VecBatch
 * Method:    newVectorBatchNative
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_VecBatch_newVectorBatchNative(JNIEnv *, jclass, jlongArray,
    jint);

/*
 * Class:     nova_hetu_omniruntime_vector_VecBatch
 * Method:    freeVectorBatchNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_VecBatch_freeVectorBatchNative(JNIEnv *, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_vector_DictionaryVec
 * Method:    getDictionaryNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_DictionaryVec_getDictionaryNative(JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif
