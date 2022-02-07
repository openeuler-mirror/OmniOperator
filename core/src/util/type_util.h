/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef OMNI_RUNTIME_TYPE_INFER_H
#define OMNI_RUNTIME_TYPE_INFER_H

#include <stdint.h>
#include "../vector/vector_type.h"

using namespace omniruntime::vec;
class TypeUtil {
public:
    static int32_t GetVarByteSize(uint32_t type) {
        switch (type) {
            case  omniruntime::vec::OMNI_VEC_TYPE_INT: {
                return sizeof(int32_t);
            }
            case omniruntime::vec::OMNI_VEC_TYPE_LONG: {
                return sizeof(int64_t);
            }
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                return sizeof(double);
            }
            default:
                break;
        }
        return 0;
    }
    static omniruntime::vec::VecTypeId StringToType(std::string dt);
    // Helper function for debugging DataType
    static std::string TypeToString(omniruntime::vec::VecTypeId id);

    static bool IsStringType(omniruntime::vec::VecTypeId id);

    static bool IsDecimalType(omniruntime::vec::VecTypeId type);

    static const std::map<std::string, omniruntime::vec::VecTypeId> stringDataTypeMap;

};
#endif  // OMNI_RUNTIME_TYPE_INFER_H