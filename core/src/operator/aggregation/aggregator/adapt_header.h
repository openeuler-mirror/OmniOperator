/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_ADAPT_HEADER_H
#define OMNI_RUNTIME_ADAPT_HEADER_H
#include "operator/omni_id_type_vector_traits.h"
namespace omniruntime {
namespace op {
using BooleanVector = NativeAndVectorType<type::OMNI_BOOLEAN>::vector;
using LongVector = NativeAndVectorType<type::OMNI_LONG>::vector;
using ShortVector = NativeAndVectorType<type::OMNI_SHORT>::vector;
using DoubleVector = NativeAndVectorType<type::OMNI_DOUBLE>::vector;
using IntVector = NativeAndVectorType<type::OMNI_INT>::vector;
using Decimal128Vector = NativeAndVectorType<type::DataTypeId::OMNI_DECIMAL128>::vector;
using VarcharVector = NativeAndVectorType<type::DataTypeId::OMNI_VARCHAR>::vector;
}
}
#endif // OMNI_RUNTIME_ADAPT_HEADER_H
