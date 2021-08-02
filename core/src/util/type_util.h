/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef __TYPE_INFER_H__
#define __TYPE_INFER_H__

#include <stdint.h>
#include "../vector/vector_type.h"

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
};

#endif