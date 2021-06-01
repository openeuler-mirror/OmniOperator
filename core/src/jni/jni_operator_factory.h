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

#ifdef __cplusplus
}
#endif
#endif //OMNI_RUNTIME_OPERATOR_FACTORY_H
