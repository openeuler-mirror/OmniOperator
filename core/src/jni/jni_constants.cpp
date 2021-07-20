/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Constants
 */
#include "jni_constants.h"
#include "../operator/status.h"
#include "../vector/vector_type.h"
#include "src/operator/aggregation/aggregator.h"

#define DEFINE_CONSTANT(_value_name) do { \
    jfieldID field = env->GetStaticFieldID(cls, #_value_name, fieldName); \
    if (field != nullptr) { \
        jmethodID methodId = env->GetMethodID(cls, "<init>", "(I)V"); \
        jobject obj = env->NewObject(cls, methodId, _value_name); \
        env->SetStaticObjectField(cls, field, obj); \
    } \
} while(0)

JNIEXPORT void JNICALL
Java_nova_hetu_omniruntime_constants_Constant_loadConstants(JNIEnv *env, jclass ignore) {
    jclass cls = env->FindClass("nova/hetu/omniruntime/constants/Status");
    const char *fieldName = "Lnova/hetu/omniruntime/constants/Status;";

    DEFINE_CONSTANT(OMNI_STATUS_NORMAL);
    DEFINE_CONSTANT(OMNI_STATUS_ERROR);
    DEFINE_CONSTANT(OMNI_STATUS_FINISHED);

    cls = env->FindClass("nova/hetu/omniruntime/constants/VecType");
    fieldName = "Lnova/hetu/omniruntime/constants/VecType;";
    DEFINE_CONSTANT(OMNI_VEC_TYPE_INT);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_LONG);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_DOUBLE);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_BOOLEAN);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_SHORT);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_CONTAINER);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_128_DECIMAL);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_256_DECIMAL);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_VARCHAR);
    DEFINE_CONSTANT(OMNI_VEC_TYPE_DICTIONARY);

    cls = env->FindClass("nova/hetu/omniruntime/constants/AggType");
    fieldName = "Lnova/hetu/omniruntime/constants/AggType;";
    using namespace omniruntime::op;
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_SUM);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_COUNT);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_AVG);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_MAX);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_MIN);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_DNV);
}