/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: JNI Operator Operations Header
 */
#ifndef JNI_OPERATOR_H
#define JNI_OPERATOR_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    addInputNative
 * Signature: (JJIJI)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInputNative(JNIEnv *, jobject, jlong, jlong);
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutputNative
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */

JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutputNative(JNIEnv *, jobject, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    close
 * Signature: (J)[Lnova/hetu/omniruntime/operator/void;
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative(JNIEnv *, jobject, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getSpilledBytesNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getSpilledBytesNative(JNIEnv *, jobject,
    jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getMetricsInfoNative
 * Signature: (J)J
 */
JNIEXPORT jlongArray JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getMetricsInfoNative(JNIEnv *, jobject,
    jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    alignSchemaNative
 * Signature: (JJ)[Lnova/hetu/omniruntime/vector/VecBatch
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_alignSchemaNative(JNIEnv *, jobject, jlong,
    jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getHashMapUniqueKeysNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getHashMapUniqueKeysNative(JNIEnv *, jobject,
    jlong);


JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_RowBatch_freeRowBatchNative(JNIEnv *env, jclass jcls,
    jlong jVecBatchAddress);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_RowBatch_transFromVectorBatch(JNIEnv *env, jclass jcls,
    jlong vectorBatch);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_RowBatch_newRowBatchNative(JNIEnv *env, jclass jcls,
    jobjectArray rows, jint rowCount);

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_vector_serialize_OmniRowDeserializer_newOmniRowDeserializer(
    JNIEnv *env, jclass jcls, jintArray typeArray, jlongArray vecs);

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_serialize_OmniRowDeserializer_freeOmniRowDeserializer(
    JNIEnv *env, jclass jcls, jlong parserAddr);

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_serialize_OmniRowDeserializer_parseOneRow(JNIEnv *env,
    jclass jcls, jlong parserAddr, jbyteArray bytes, jint rowIndex);

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_serialize_OmniRowDeserializer_parseOneRowByAddr(JNIEnv *env,
    jclass jcls, jlong parserAddr, jlong rowAddr, jint rowIndex);

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_serialize_OmniRowDeserializer_parseAllRow(JNIEnv *env,
    jclass jcls, jlong parserAddr, jlong rowBatchAddr);

#ifdef __cplusplus
}
#endif
#endif
