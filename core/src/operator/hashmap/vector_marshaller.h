/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
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

using VectorSerializerIgnoreNull = bool (*)(BaseVector *baseVector, int32_t rowIdx,
    mem::SimpleArenaAllocator &arenaAllocator, type::StringRef &result);

using VectorSerializer = void (*)(BaseVector *baseVector, int32_t rowIdx, mem::SimpleArenaAllocator &arenaAllocator,
    type::StringRef &result);

using FixedKeyVectorSerializerIgnoreNull = bool (*)(BaseVector *baseVector, int32_t rowIdx, StringRef &result,
    size_t &pos);

using FixedKeyVectorSerializerIgnoreNullSimd = bool (*)(BaseVector *baseVector, int32_t rowIdx,
    std::vector<StringRef> &result, size_t &pos, int32_t joinRownum);

template <type::DataTypeId id> char *DeserializeFromPointer(BaseVector *baseVector, int32_t rowIdx, const char *&begin);

extern std::vector<VectorSerializer> vectorSerializerCenter;
extern std::vector<VectorSerializer> dicVectorSerializerCenter;
extern std::vector<VectorDeSerializer> vectorDeSerializerCenter;

extern std::vector<VectorSerializerIgnoreNull> vectorSerializerIgnoreNullCenter;
extern std::vector<VectorSerializerIgnoreNull> dicVectorSerializerIgnoreNullCenter;

extern std::vector<FixedKeyVectorSerializerIgnoreNull> vectorSerializerFixedKeysIgnoreNullCenter;
extern std::vector<FixedKeyVectorSerializerIgnoreNull> dicVectorSerializerFixedKeysIgnoreNullCenter;

extern std::vector<FixedKeyVectorSerializerIgnoreNullSimd> vectorSerializerFixedKeysIgnoreNullCenterSimd;
extern std::vector<FixedKeyVectorSerializerIgnoreNullSimd> dicVectorSerializerFixedKeysIgnoreNullCenterSimd;
}
}
#endif // OMNI_RUNTIME_VECTOR_MARSHALLER_H
