/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: JNI Constants
 */

#include "jni_constants.h"
#include "operator/status.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "operator/join/lookup_join.h"
#include "operator/window/window_function.h"

using namespace omniruntime::vec;

#define DEFINE_CONSTANT(_value_name)                                          \
    do {                                                                      \
        jfieldID field = env->GetStaticFieldID(cls, #_value_name, fieldName); \
        if (field != nullptr) {                                               \
            jmethodID methodId = env->GetMethodID(cls, "<init>", "(I)V");     \
            jobject obj = env->NewObject(cls, methodId, _value_name);         \
            env->SetStaticObjectField(cls, field, obj);                       \
        }                                                                     \
    } while (0)

JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_constants_Constant_loadConstants(JNIEnv *env, jclass ignore)
{
    jclass cls = env->FindClass("nova/hetu/omniruntime/constants/Status");
    const char *fieldName = "Lnova/hetu/omniruntime/constants/Status;";

    DEFINE_CONSTANT(OMNI_STATUS_NORMAL);
    DEFINE_CONSTANT(OMNI_STATUS_ERROR);
    DEFINE_CONSTANT(OMNI_STATUS_FINISHED);

    cls = env->FindClass("nova/hetu/omniruntime/constants/FunctionType");
    fieldName = "Lnova/hetu/omniruntime/constants/FunctionType;";
    using namespace omniruntime::op;
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_SUM);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_COUNT_COLUMN);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_COUNT_ALL);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_AVG);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_MAX);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_MIN);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_DNV);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);
    DEFINE_CONSTANT(OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL);
    DEFINE_CONSTANT(OMNI_WINDOW_TYPE_ROW_NUMBER);
    DEFINE_CONSTANT(OMNI_WINDOW_TYPE_RANK);

    cls = env->FindClass("nova/hetu/omniruntime/constants/JoinType");
    fieldName = "Lnova/hetu/omniruntime/constants/JoinType;";
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_INNER);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_LEFT);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_RIGHT);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_FULL);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_LEFT_SEMI);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_LEFT_ANTI);

    cls = env->FindClass("nova/hetu/omniruntime/constants/OmniWindowFrameType");
    fieldName = "Lnova/hetu/omniruntime/constants/OmniWindowFrameType;";
    DEFINE_CONSTANT(OMNI_FRAME_TYPE_RANGE);
    DEFINE_CONSTANT(OMNI_FRAME_TYPE_ROWS);

    cls = env->FindClass("nova/hetu/omniruntime/constants/OmniWindowFrameBoundType");
    fieldName = "Lnova/hetu/omniruntime/constants/OmniWindowFrameBoundType;";
    DEFINE_CONSTANT(OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING);
    DEFINE_CONSTANT(OMNI_FRAME_BOUND_PRECEDING);
    DEFINE_CONSTANT(OMNI_FRAME_BOUND_CURRENT_ROW);
    DEFINE_CONSTANT(OMNI_FRAME_BOUND_FOLLOWING);
    DEFINE_CONSTANT(OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING);

    cls = env->FindClass("nova/hetu/omniruntime/constants/OperatorType");
    fieldName = "Lnova/hetu/omniruntime/constants/OperatorType;";
    DEFINE_CONSTANT(OMNI_FILTER_AND_PROJECT);
    DEFINE_CONSTANT(OMNI_PROJECT);
    DEFINE_CONSTANT(OMNI_LIMIT);
    DEFINE_CONSTANT(OMNI_DISTINCT_LIMIT);
    DEFINE_CONSTANT(OMNI_SORT);
    DEFINE_CONSTANT(OMNI_TOPN);
    DEFINE_CONSTANT(OMNI_AGGREGATION);
    DEFINE_CONSTANT(OMNI_HASH_AGGREGATION);
    DEFINE_CONSTANT(OMNI_WINDOW);
    DEFINE_CONSTANT(OMNI_HASH_BUILDER);
    DEFINE_CONSTANT(OMNI_LOOKUP_JOIN);
    DEFINE_CONSTANT(OMNI_LOOKUP_OUTER_JOIN);
    DEFINE_CONSTANT(OMNI_SMJ_BUFFER);
    DEFINE_CONSTANT(OMNI_SMJ_STREAM);
    DEFINE_CONSTANT(OMNI_PARTITIONED_OUTPUT);
    DEFINE_CONSTANT(OMNI_UNION);
    DEFINE_CONSTANT(OMNI_FUSION);
}

JNIEXPORT jstring JNICALL Java_nova_hetu_omniruntime_OmniLibs_getVersion(JNIEnv *env, jclass ignore)
{
    return (*env).NewStringUTF(
        "Software-Title: boostkit-omnioperatorjit\nSoftware-Version: 1.1.0\nSoftware-Vendor: Kunpeng BoostKit");
}