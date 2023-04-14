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

template <type::DataTypeId id>
type::StringRef NullVariableTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, const char *&begin);

template <type::DataTypeId id>
type::StringRef NullFixedLenTypeSerializer(mem::SimpleArenaAllocator &arenaAllocator, const char *&begin);

template <type::DataTypeId id>
type::StringRef FixedLenTypeSerializer(void *inValuePtr, mem::SimpleArenaAllocator &arenaAllocator,
                                       const char *&begin);

template <type::DataTypeId id>
const char *FixedLenTypeDeserializer(BaseVector *baseVector, size_t rowId, const char *pos);

template <type::DataTypeId id>
type::StringRef VariableTypeSerializer(void *inValuePtr, mem::SimpleArenaAllocator &arenaAllocator,
                                       const char *&begin);

template <type::DataTypeId id>
const char *VariableTypeDeserializer(BaseVector *baseVector, size_t rowId, const char *pos);


template <type::DataTypeId id>
omniruntime::type::StringRef SerializeValueIntoExecutionContext(std::shared_ptr<BaseVector> baseVector,
    int rowId, mem::SimpleArenaAllocator &arenaAllocator, const char *&begin);
/**
 * only serialize null value
 */
using VectorNullSerializer =
    std::function<type::StringRef(mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)>;

/**
 * serialize not null value
 */
using VectorValuedSerializer = std::function<type::StringRef(void *value,
    mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)>;

/**
 * deserialize not null value
 */
using VectorDeSerializer =
    std::function<const char *(BaseVector* baseVector, int rowId, const char *&begin)>;


using VectorSerializer = std::function<type::StringRef(BaseVector* baseVector, int rowId,
    mem::SimpleArenaAllocator &arenaAllocator, const char *&begin)>;


template <type::DataTypeId id>
const char *DeserializeFromPointer(BaseVector* baseVector, int rowId, const char *&begin);

extern std::vector<VectorSerializer> vectorSerializerCenter;
extern std::vector<VectorSerializer> dicVectorSerializerCenter;
extern std::vector<VectorDeSerializer> vectorDeSerializerCenter;
}
}
#endif // OMNI_RUNTIME_VECTOR_MARSHALLER_H
