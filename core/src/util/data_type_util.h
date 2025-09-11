/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: JNI Vector Operations Source File
 */
#ifndef OMNI_RUNTIME_DATA_TYPE_UTIL_H
#define OMNI_RUNTIME_DATA_TYPE_UTIL_H

#include <jni.h>
#include <memory>
#include <vector>
#include "type/data_type.h"
#include "util/omni_exception.h"

using namespace omniruntime::type;

class DataTypeUtil {
public:

    static jint GetDataTypeId(JNIEnv *env, jobject jDataType)
    {
        jclass dataTypeClass = env->GetObjectClass(jDataType);
        jmethodID getIdValueMethod = env->GetMethodID(dataTypeClass, "getIdValue", "()I");
        return env->CallIntMethod(jDataType, getIdValueMethod);
    }

    static void GetDecimalPrecisionAndScale(JNIEnv *env, jobject jDataType, jint *precision, jint *scale)
    {
        jclass decimalTypeClass = env->GetObjectClass(jDataType);
        jmethodID getPrecisionMethod = env->GetMethodID(decimalTypeClass, "getPrecision", "()I");
        jmethodID getScaleMethod = env->GetMethodID(decimalTypeClass, "getScale", "()I");
        *precision = env->CallIntMethod(jDataType, getPrecisionMethod);
        *scale = env->CallIntMethod(jDataType, getScaleMethod);
    }

    static jint GetVarcharWidth(JNIEnv *env, jobject jDataType)
    {
        jclass varcharTypeClass = env->GetObjectClass(jDataType);
        jmethodID getWidthMethod = env->GetMethodID(varcharTypeClass, "getWidth", "()I");
        return env->CallIntMethod(jDataType, getWidthMethod);
    }

    static jobjectArray GetContainerFieldTypes(JNIEnv *env, jobject jDataType)
    {
        jclass containerTypeClass = env->GetObjectClass(jDataType);
        jmethodID getFieldTypesMethod = env->GetMethodID(containerTypeClass, "getFieldTypes", "()[Lnova/hetu/omniruntime/type/DataType;");
        return (jobjectArray) env->CallObjectMethod(jDataType, getFieldTypesMethod);
    }

    static void GetArrayElementType(JNIEnv *env, jobject jDataType, jobject *elementType)
    {
        jclass arrayTypeClass = env->GetObjectClass(jDataType);
        jmethodID getElementTypeMethod = env->GetMethodID(arrayTypeClass, "getElementType", "()Lnova/hetu/omniruntime/type/DataType;");
        *elementType = env->CallObjectMethod(jDataType, getElementTypeMethod);
    }

    static void GetMapKeyValueTypes(JNIEnv *env, jobject jDataType, jobject *keyType, jobject *valueType)
    {
        jclass mapTypeClass = env->GetObjectClass(jDataType);
        jmethodID getKeyTypeMethod = env->GetMethodID(mapTypeClass, "getKeyType", "()Lnova/hetu/omniruntime/type/DataType;");
        jmethodID getValueTypeMethod = env->GetMethodID(mapTypeClass, "getValueType", "()Lnova/hetu/omniruntime/type/DataType;");
        *keyType = env->CallObjectMethod(jDataType, getKeyTypeMethod);
        *valueType = env->CallObjectMethod(jDataType, getValueTypeMethod);
    }

    static DataTypePtr ConvertJavaDataTypeToCpp(JNIEnv *env, jobject jDataType)
    {
        jint id = GetDataTypeId(env, jDataType);
        DataTypeId dataTypeId = static_cast<DataTypeId>(id);

        switch (dataTypeId) {
            case OMNI_INT:
                return std::make_shared<IntDataType>();
            case OMNI_LONG:
                return std::make_shared<LongDataType>();
            case OMNI_DOUBLE:
                return std::make_shared<DoubleDataType>();
            case OMNI_BOOLEAN:
                return std::make_shared<BooleanDataType>();
            case OMNI_SHORT:
                return std::make_shared<ShortDataType>();
            case OMNI_DECIMAL64: {
                jint precision;
                jint scale;
                GetDecimalPrecisionAndScale(env, jDataType, &precision, &scale);
                return std::make_shared<Decimal64DataType>(precision, scale);
            }
            case OMNI_DECIMAL128: {
                jint precision;
                jint scale;
                GetDecimalPrecisionAndScale(env, jDataType, &precision, &scale);
                return std::make_shared<Decimal128DataType>(precision, scale);
            }
            case OMNI_VARCHAR: {
                jint width = GetVarcharWidth(env, jDataType);
                return std::make_shared<VarcharDataType>(width);
            }
            case OMNI_CHAR: {
                jint width = GetVarcharWidth(env, jDataType);
                return std::make_shared<CharDataType>(width);
            }
            case OMNI_CONTAINER: {
                jobjectArray jFieldTypes = GetContainerFieldTypes(env, jDataType);
                jsize length = env->GetArrayLength(jFieldTypes);
                std::vector<DataTypePtr> fieldTypes;
                for (jsize i = 0; i < length; i++) {
                    jobject jFieldType = env->GetObjectArrayElement(jFieldTypes, i);
                    DataTypePtr fieldType = ConvertJavaDataTypeToCpp(env, jFieldType);
                    fieldTypes.push_back(fieldType);
                    env->DeleteLocalRef(jFieldType);
                }
                return std::make_shared<ContainerDataType>(fieldTypes);
            }
            case OMNI_ARRAY: {
                jobject jElementType;
                GetArrayElementType(env, jDataType, &jElementType);
                DataTypePtr elementType = ConvertJavaDataTypeToCpp(env, jElementType);
                env->DeleteLocalRef(jElementType);
                return std::make_shared<ArrayType>(elementType);
            }
            case OMNI_MAP: {
                jobject jKeyType;
                jobject jValueType;
                GetMapKeyValueTypes(env, jDataType, &jKeyType, &jValueType);
                DataTypePtr keyType = ConvertJavaDataTypeToCpp(env, jKeyType);
                DataTypePtr valueType = ConvertJavaDataTypeToCpp(env, jValueType);
                env->DeleteLocalRef(jKeyType);
                env->DeleteLocalRef(jValueType);
                return std::make_shared<MapType>(keyType, valueType);
            }
            case OMNI_ROW: {
                jobjectArray jFieldTypes = GetContainerFieldTypes(env, jDataType);
                jsize length = env->GetArrayLength(jFieldTypes);
                std::vector<DataTypePtr> fieldTypes;
                for (jsize i = 0; i < length; i++) {
                    jobject jFieldType = env->GetObjectArrayElement(jFieldTypes, i);
                    DataTypePtr fieldType = ConvertJavaDataTypeToCpp(env, jFieldType);
                    fieldTypes.push_back(fieldType);
                    env->DeleteLocalRef(jFieldType);
                }
                return std::make_shared<RowType>(fieldTypes);
            }
            default:
                return std::make_shared<DataType>(dataTypeId);
        }
    }

    static std::vector<DataTypePtr> ConvertJavaDataTypesToCpp(JNIEnv *env, jobjectArray jDataTypes)
    {
        std::vector<DataTypePtr> cppDataTypes;
        jsize length = env->GetArrayLength(jDataTypes);
        cppDataTypes.reserve(length);

        for (jsize i = 0; i < length; i++) {
            jobject jDataType = env->GetObjectArrayElement(jDataTypes, i);
            if (jDataType != nullptr) {
                try {
                    DataTypePtr cppDataType = ConvertJavaDataTypeToCpp(env, jDataType);
                    cppDataTypes.push_back(cppDataType);
                    env->DeleteLocalRef(jDataType);
                } catch (const omniruntime::exception::OmniException &e) {
                    env->DeleteLocalRef(jDataType);
                    throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                                                                "Failed to convert DataType at index " +
                                                                std::to_string(i) + ": " + e.what());
                } catch (const std::exception &e) {
                    env->DeleteLocalRef(jDataType);
                    throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                                                                "Std exception occurred at index " + std::to_string(i) +
                                                                ": " + e.what());
                }
            } else {
                throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                                                            "Null DataType object at index " + std::to_string(i));
            }
        }

        return cppDataTypes;
    }
};
#endif // OMNI_RUNTIME_DATA_TYPE_UTIL_H
