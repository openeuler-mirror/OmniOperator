//
// Created by root on 5/26/21.
//
#include <jni.h>
#ifndef OMNI_RUNTIME_OPERATOR_FACTORY_H
#define OMNI_RUNTIME_OPERATOR_FACTORY_H
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperatorFactory
 * Method:    createOperator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_OmniOperatorFactory_createOperator
        (JNIEnv *, jobject, jlong);

/*
 * Class:     nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory
 * Method:    createSortOperatorFactory
 * Signature: ([I[I[I[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_sort_OmniSortOperatorFactory_createSortOperatorFactory
        (JNIEnv *, jobject, jintArray, jintArray, jintArray, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory
 * Method:    createHashAggregationOperatorFactory
 * Signature: ([I[I[I[I[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniHashAggregationOperatorFactory_createHashAggregationOperatorFactory
        (JNIEnv *, jobject, jintArray, jintArray, jintArray, jintArray, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory
 * Method:    createAggregationOperatorFactory
 * Signature: ([I[I[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_aggregator_OmniAggregationOperatorFactory_createAggregationOperatorFactory
        (JNIEnv *, jobject, jintArray, jintArray);

/*
 * Class:     nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory
 * Method:    createFilterAndProjectOperatorFactory
 * Signature: ([IILjava/lang/String;[II)J
*/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_filter_OmniFilterAndProjectOperatorFactory_createFilterAndProjectOperatorFactory
        (JNIEnv *, jobject, jintArray, jint, jstring, jintArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory
 * Method:    createWindowOperatorFactory
 * Signature:
*/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_window_OmniWindowOperatorFactory_createWindowOperatorFactory
        (JNIEnv *, jobject, jintArray,jintArray,jintArray,jintArray,jintArray,jintArray,jintArray,jintArray,jint,jint,jintArray,jintArray);
/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory
 * Method:    createHashBuilderOperatorFactory
 * Signature: ([I[I[II)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniHashBuilderOperatorFactory_createHashBuilderOperatorFactory
  (JNIEnv *, jobject, jintArray, jintArray, jintArray, jint);

/*
 * Class:     nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory
 * Method:    createLookupJoinOperatorFactory
 * Signature: ([I[I[I[I[IJ)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_join_OmniLookupJoinOperatorFactory_createLookupJoinOperatorFactory
  (JNIEnv *, jobject, jintArray, jintArray, jintArray, jintArray, jintArray, jlong);

#ifdef __cplusplus
}
#endif
#endif //OMNI_RUNTIME_OPERATOR_FACTORY_H
