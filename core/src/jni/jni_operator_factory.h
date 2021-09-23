/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Operator Factory Header
 */
#ifndef JNI_OPERATOR_FACTORY_H
#define JNI_OPERATOR_FACTORY_H
#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperatorNative
 * Signature: (J)JJ
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperatorNative(JNIEnv *, jobject,
    jlong, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *, jclass, jstring, jintArray, jobjectArray, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory
 * Method:    createHashAggregationOperatorFactory
 * Signature: ([Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *, jclass, jobjectArray, jstring, jobjectArray, jstring, jintArray, jstring, jboolean, jboolean);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory
 * Method:    createAggregationOperatorFactory
 * Signature: ([I[I[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(JNIEnv *,
    jobject, jstring, jintArray, jboolean, jboolean);

/*
 * Class:     nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory
 * Method:    createFilterAndProjectOperatorFactory
 * Signature: ([IILjava/lang/String;[II)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *, jobject, jstring, jint, jstring, jintArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory
 * Method:    createProjectOperatorFactory
 * Signature: ([II[Ljava/lang/Object;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *, jobject,
    jstring, jint, jobjectArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory
 * Method:    CreateWindowOperatorFactory
 * Signature: ([I[I[I[I[I[I[I[III[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *, jobject,
    jstring, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jint, jint, jintArray,
    jstring);

/*
 * Class:     nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory
 * Method:    createTopNOperatorFactory
 * Signature: (Ljava/lang/String;I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory
 * Method:    createHashBuilderOperatorFactory
 * Signature: (Ljava/lang/String;[Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *,
    jclass, jstring, jobjectArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory
 * Method:    createLookupJoinOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[ILjava/lang/String;IJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(JNIEnv *, jclass,
    jstring, jintArray, jobjectArray, jintArray, jstring, jint, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_partitionedoutput_PartitionedOutputOperatorFactory
 * Method:    CreatePartitionedOutputOperatorFactory
 * Signature: ([IZI[II[IZ[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_partitionedoutput_OmniPartitionedOutPutOperatorFactory_createPartitionedOutputOperatorFactory
(JNIEnv *env, jobject, jstring, jboolean, jint, jintArray, jint, jintArray, jboolean, jstring, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_union_UnionOperatorFactory
 * Method:    CreateUnionOperatorFactory
 * Signature: ([IZI[II[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
    JNIEnv *env, jobject, jstring, jboolean);

#ifdef __cplusplus
}
#endif
#endif