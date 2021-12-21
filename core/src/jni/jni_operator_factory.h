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
 * Method:    createSortJitContext
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortJitContext(JNIEnv *,
    jclass, jstring, jintArray, jobjectArray, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory(
    JNIEnv *, jclass, jstring, jintArray, jobjectArray, jintArray, jintArray, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory
 * Method:    createHashAggregationJitContext
 * Signature: ([Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationJitContext(
    JNIEnv *, jclass, jobjectArray, jstring, jobjectArray, jstring, jintArray, jstring, jboolean, jboolean);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory
 * Method:    createHashAggregationOperatorFactory
 * Signature: ([Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory(
    JNIEnv *, jclass, jobjectArray, jstring, jobjectArray, jstring, jintArray, jstring, jboolean, jboolean, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory
 * Method:    createAggregationJitContext
 * Signature: ([I[I[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationJitContext(JNIEnv *,
    jobject, jstring, jintArray, jstring, jboolean, jboolean);


/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory
 * Method:    createAggregationOperatorFactory
 * Signature: ([I[I[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory(JNIEnv *,
    jobject, jstring, jintArray, jstring, jboolean, jboolean, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory
 * Method:    createFilterAndProjectOperatorFactory
 * Signature: ([IILjava/lang/String;[II)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory(
    JNIEnv *, jobject, jstring, jint, jstring, jobjectArray, jint, jlong, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory
 * Method:    createProjectOperatorFactory
 * Signature: ([II[Ljava/lang/Object;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_project_OmniProjectOperatorFactory_createProjectOperatorFactory(JNIEnv *, jobject,
    jstring, jint, jobjectArray, jint, jlong, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory
 * Method:    CreateWindowJitContext
 * Signature: ([I[I[I[I[I[I[I[III[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowJitContext(JNIEnv *, jobject, jstring,
    jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jint, jint, jintArray, jstring);

/*
 * Class:     nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory
 * Method:    CreateWindowOperatorFactory
 * Signature: ([I[I[I[I[I[I[I[III[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory(JNIEnv *, jobject,
    jstring, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jint, jint, jintArray,
    jstring, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory
 * Method:    createTopNOperatorFactory
 * Signature: (Ljava/lang/String;I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNOperatorFactory(
    JNIEnv *env, jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc,
    jintArray jSortNullFirsts, jlong jitContext);

/*
 * Class:     nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory
 * Method:    createTopNJitContext
 * Signature: (Ljava/lang/String;I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNOperatorFactory_createTopNJitContext(JNIEnv *env, jclass jObj,
    jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory
 * Method:    createHashBuilderOperatorFactory
 * Signature: (Ljava/lang/String;[ILjava/lang/String;I[Ljava/lang/String;IJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory(JNIEnv *,
    jclass, jstring, jintArray, jstring, jint, jobjectArray, jint, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory
 * Method:    createHashBuilderJitContext
 * Signature: (Ljava/lang/String;[ILjava/lang/String;I[Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderJitContext(JNIEnv *, jclass,
    jstring, jintArray, jstring, jint, jobjectArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory
 * Method:    createLookupJoinOperatorFactory
 * Signature: (Ljava/lang/String;[I[I[ILjava/lang/String;IJJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory(JNIEnv *, jclass,
    jstring, jintArray, jintArray, jintArray, jstring, jint, jlong, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory
 * Method:    createLookupJoinJitContext
 * Signature: (Ljava/lang/String;[I[I[ILjava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinJitContext(JNIEnv *, jclass,
    jstring, jintArray, jintArray, jintArray, jstring, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_partitionedoutput_PartitionedOutputOperatorFactory
 * Method:    CreatePartitionedOutputOperatorFactory
 * Signature: ([IZI[II[IZ[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_partitionedoutput_OmniPartitionedOutPutOperatorFactory_createPartitionedOutputOperatorFactory(
    JNIEnv *env, jobject, jstring, jboolean, jint, jintArray, jint, jintArray, jboolean, jstring, jintArray, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_union_UnionOperatorFactory
 * Method:    CreateUnionOperatorFactory
 * Signature: ([IZI[II[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_union_OmniUnionOperatorFactory_createUnionOperatorFactory(
    JNIEnv *env, jobject, jstring, jboolean, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory
 * Method:    createSortWithExprJitContext
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprJitContext(JNIEnv *, jclass,
    jstring, jintArray, jobjectArray, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory
 * Method:    createSortWithExprOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[I[IJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_sort_OmniSortWithExprOperatorFactory_createSortWithExprOperatorFactory(JNIEnv *,
    jclass, jstring, jintArray, jobjectArray, jintArray, jintArray, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory
 * Method:    createHashBuilderWithExprJitContext
 * Signature: (Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprJitContext(
    JNIEnv *, jclass, jstring, jobjectArray, jstring, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory
 * Method:    createHashBuilderWithExprOperatorFactory
 * Signature: (Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;IJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderWithExprOperatorFactory_createHashBuilderWithExprOperatorFactory(
    JNIEnv *, jclass, jstring, jobjectArray, jstring, jint, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory
 * Method:    createLookupJoinWithExprJitContext
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[ILjava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprJitContext(
    JNIEnv *, jclass, jstring, jintArray, jobjectArray, jintArray, jstring, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory
 * Method:    createLookupJoinWithExprOperatorFactory
 * Signature: (Ljava/lang/String;[I[Ljava/lang/String;[ILjava/lang/String;IJJ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinWithExprOperatorFactory_createLookupJoinWithExprOperatorFactory(
    JNIEnv *, jclass, jstring, jintArray, jobjectArray, jintArray, jstring, jint, jlong, jlong);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprJitContext(JNIEnv *,
    jobject, jstring, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jint, jint,
    jobjectArray, jstring);

JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_window_OmniWindowWithExprOperatorFactory_createWindowWithExprOperatorFactory(
    JNIEnv *, jobject, jstring, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray, jint, jint,
    jobjectArray, jstring, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory
 * Method:    createHashAggregationWithExprJitContext
 * Signature: ([Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprJitContext(
    JNIEnv *, jclass, jobjectArray, jobjectArray, jstring, jintArray, jstring, jboolean, jboolean);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory
 * Method:    createHashAggregationWithExprOperatorFactory
 * Signature: ([Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationWithExprOperatorFactory_createHashAggregationWithExprOperatorFactory(
    JNIEnv *, jclass, jobjectArray, jobjectArray, jstring, jintArray, jstring, jboolean, jboolean, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory
 * Method:    createTopNWithExprOperatorFactory
 * Signature: (Ljava/lang/String;I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprOperatorFactory(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts,
    jlong jitContext);

/*
 * Class:     nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory
 * Method:    createTopNWithExprJitContext
 * Signature: (Ljava/lang/String;I[Ljava/lang/String;[I[I)J
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_topn_OmniTopNWithExprOperatorFactory_createTopNWithExprJitContext(JNIEnv *env,
    jclass jObj, jstring jSourceTypes, jint jN, jobjectArray jSortCols, jintArray jSortAsc, jintArray jSortNullFirsts);

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    closeNativeOperatorFactory
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_closeNativeOperatorFactory(JNIEnv *,
    jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_limit_OmniLimitOperatorFactory
 * Method:    CreateLimitOperatorFactory
 * Signature: (L)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_limit_OmniLimitOperatorFactory_createLimitOperatorFactory(
    JNIEnv *env, jclass, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_distinctlimit_OmniDistinctLimitOperatorFactory
 * Method:    CreateDistinctLimitOperatorFactory
 * Signature: ([Ljava/lang/String;[Ijava/lang/Int;IL)J
 * Note: out put seq as below:
 *                           1. distinct cols
 *                           2. normal cols
 *                           3. hash col(jHashChannel)
 * Note: put jHashChannel to -1 if no precomputed hash value for distinct cols
 */
JNIEXPORT jlong JNICALL
Java_nova_hetu_omniruntime_operator_limit_OmniDistinctLimitOperatorFactory_createDistinctLimitOperatorFactory(
    JNIEnv *env, jclass, jstring, jintArray, jint, jlong);

#ifdef __cplusplus
}
#endif
#endif