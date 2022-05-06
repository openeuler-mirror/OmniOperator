/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
    DEFINE_CONSTANT(OMNI_WINDOW_TYPE_ROW_NUMBER);
    DEFINE_CONSTANT(OMNI_WINDOW_TYPE_RANK);

    cls = env->FindClass("nova/hetu/omniruntime/constants/JoinType");
    fieldName = "Lnova/hetu/omniruntime/constants/JoinType;";
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_INNER);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_LEFT);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_RIGHT);
    DEFINE_CONSTANT(OMNI_JOIN_TYPE_FULL);

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
}