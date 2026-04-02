/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#ifndef OMNI_RUNTIME_REGR_NUMERIC_H
#define OMNI_RUNTIME_REGR_NUMERIC_H

#include <cstdint>
#include "vector/vector.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
namespace unsafe = omniruntime::vec::unsafe;
}
}

#include "operator/aggregation/vector_getter.h"

namespace omniruntime::op {

inline double RegrGetDoubleAt(vec::BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == vec::OMNI_ENCODING_CONST) {
        switch (vec->GetTypeId()) {
            case type::OMNI_BOOLEAN:
                return static_cast<vec::ConstVector<bool> *>(vec)->GetConstValue() ? 1.0 : 0.0;
            case type::OMNI_BYTE:
                return static_cast<double>(static_cast<vec::ConstVector<int8_t> *>(vec)->GetConstValue());
            case type::OMNI_SHORT:
                return static_cast<double>(static_cast<vec::ConstVector<int16_t> *>(vec)->GetConstValue());
            case type::OMNI_INT:
                return static_cast<double>(static_cast<vec::ConstVector<int32_t> *>(vec)->GetConstValue());
            case type::OMNI_LONG:
                return static_cast<double>(static_cast<vec::ConstVector<int64_t> *>(vec)->GetConstValue());
            case type::OMNI_FLOAT:
                return static_cast<double>(static_cast<vec::ConstVector<float> *>(vec)->GetConstValue());
            case type::OMNI_DOUBLE:
                return static_cast<vec::ConstVector<double> *>(vec)->GetConstValue();
            default: return 0.0;
        }
    }
    switch (vec->GetTypeId()) {
        case type::OMNI_BOOLEAN: {
            auto *p = reinterpret_cast<bool *>(GetValuesFromVector<type::OMNI_BOOLEAN>(vec));
            return p[row] ? 1.0 : 0.0;
        }
        case type::OMNI_BYTE: {
            auto *p = reinterpret_cast<int8_t *>(GetValuesFromVector<type::OMNI_BYTE>(vec));
            return static_cast<double>(p[row]);
        }
        case type::OMNI_SHORT: {
            auto *p = reinterpret_cast<int16_t *>(GetValuesFromVector<type::OMNI_SHORT>(vec));
            return static_cast<double>(p[row]);
        }
        case type::OMNI_INT: {
            auto *p = reinterpret_cast<int32_t *>(GetValuesFromVector<type::OMNI_INT>(vec));
            return static_cast<double>(p[row]);
        }
        case type::OMNI_LONG: {
            auto *p = reinterpret_cast<int64_t *>(GetValuesFromVector<type::OMNI_LONG>(vec));
            return static_cast<double>(p[row]);
        }
        case type::OMNI_FLOAT: {
            auto *p = reinterpret_cast<float *>(GetValuesFromVector<type::OMNI_FLOAT>(vec));
            return static_cast<double>(p[row]);
        }
        case type::OMNI_DOUBLE: {
            auto *p = reinterpret_cast<double *>(GetValuesFromVector<type::OMNI_DOUBLE>(vec));
            return p[row];
        }
        default:
            return 0.0;
    }
}

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_REGR_NUMERIC_H
