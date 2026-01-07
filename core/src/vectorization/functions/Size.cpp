/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "Size.h"

#include <vector>
#include "vectorization/SimpleFunction.h"
#include "./vectorization/VectorReaders.h"
#include "type/data_type.h"
#include "../SimpleFunctionMetadata.h"
#include "util/compiler_util.h"
#include "../registration/SimpleFunctionRegistry.h"
#include "vectorization/ComplexViewTypes.h"

namespace omniruntime::vectorization {
namespace {
template <typename TExecParams>
struct Size {
    template <typename TInput>
    void initialize(const std::vector<DataTypeId> & /*inputTypes*/, const config::QueryConfig & /*config*/,
        const TInput * /*input*/, const bool *legacySizeOfNull)
    {
        if (legacySizeOfNull == nullptr) {
            OMNI_THROW("Size error:", "Constant legacySizeOfNull is expected.");
        }
        legacySizeOfNull_ = *legacySizeOfNull;
    }

    template <typename TInput>
    ALWAYS_INLINE bool callNullable(int32_t &out, const TInput *input, const bool * /*legacySizeOfNull*/)
    {
        if (input == nullptr) {
            if (legacySizeOfNull_) {
                out = -1;
                return true;
            }
            return false;
        }
        out = input->size();
        return true;
    }

private:
    // If true, returns -1 for null input. Otherwise, returns null.
    bool legacySizeOfNull_ = false;
};
} // namespace

void registerSize(const std::string &prefix)
{
    RegisterFunction<Size, int32_t, ArrayView<true, int8_t>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<Size, int32_t, ArrayView<true, int16_t>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<Size, int32_t, ArrayView<true, int32_t>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<Size, int32_t, ArrayView<true, int64_t>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<Size, int32_t, ArrayView<true, double>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<Size, int32_t, ArrayView<true, Decimal128>, bool>(prefix, {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT);
}
} // namespace facebook::velox::functions::sparksql
