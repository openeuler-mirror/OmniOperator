/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_MARSHALLER_H
#define OMNI_RUNTIME_VECTOR_MARSHALLER_H

#include <cstddef>
#include "vector/vector_batch.h"
#include "vector/dictionary_container.h"
#include "memory/simple_arena_allocator.h"
#include "type/string_ref.h"
#include "type/data_type.h"

namespace omniruntime {
using namespace type;
using namespace vec;
namespace op {
/**
 * deserialize not null value
 */
using VectorDeSerializer = const char *(*)(BaseVector *baseVector, int32_t rowIdx, const char *&begin);

using VectorSerializer = type::StringRef (*)(BaseVector *baseVector, int32_t rowIdx,
    mem::SimpleArenaAllocator &arenaAllocator, const char *&begin);

template <type::DataTypeId id>
const char *DeserializeFromPointer(BaseVector *baseVector, int32_t rowIdx, const char *&begin);

extern std::vector<VectorSerializer> vectorSerializerCenter;
extern std::vector<VectorSerializer> dicVectorSerializerCenter;
extern std::vector<VectorDeSerializer> vectorDeSerializerCenter;
}
}
#endif // OMNI_RUNTIME_VECTOR_MARSHALLER_H
