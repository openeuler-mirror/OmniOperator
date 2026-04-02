/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: In expression implementation for SimpleFunction framework
 */

#include "vectorization/SimpleFunction.h"
#include "./vectorization/VectorReaders.h"
#include "type/data_type.h"
#include "../SimpleFunctionMetadata.h"
#include "../registration/SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
namespace {
template <typename TInput>
struct InFunctionOuter {
    template <typename TExecCtx>
    struct InFunctionInner {
        ALWAYS_INLINE void initialize(const std::vector<type::DataTypeId> & /*inputTypes*/, const config::QueryConfig &,
            const TInput * /*searchTerm*/, const ArrayView<true, TInput> *searchElements)
        {
            if (searchElements == nullptr) {
                return;
            }

            for (const auto &entry : *searchElements) {
                if (!entry.has_value()) {
                    hasNull_ = true;
                    continue;
                }
                elements_.insert(entry.value());
            }
        }

        ALWAYS_INLINE bool callNullable(bool &result, const TInput *searchTerm, const ArrayView<true, TInput> * /*array*/)
        {
            if (searchTerm == nullptr) {
                return false;
            }

            result = elements_.find(*searchTerm) != elements_.end();
            if (hasNull_ && !result) {
                return false;
            }
            return true;
        }

    private:
        std::set<TInput> elements_;
        bool hasNull_{false};
    };

    template <typename T>
    using Inner = typename InFunctionOuter<TInput>::template InFunctionInner<T>;
};

template <typename T>
void registerInFn(const std::string &prefix, const DataTypeId &typeId)
{
    RegisterFunction<InFunctionOuter<T>::template Inner, bool, T, ArrayView<true, T>>(prefix + "in",
        {typeId, OMNI_ARRAY}, OMNI_BOOLEAN);
}
} // namespace

void registerIn(const std::string &prefix)
{
    registerInFn<bool>(prefix, OMNI_BOOLEAN);
    registerInFn<int8_t>(prefix, OMNI_BYTE);
    registerInFn<int16_t>(prefix, OMNI_SHORT);
    registerInFn<int32_t>(prefix, OMNI_INT);
    registerInFn<int32_t>(prefix, OMNI_DATE32);
    registerInFn<int64_t>(prefix, OMNI_LONG);
    registerInFn<int64_t>(prefix, OMNI_TIMESTAMP);
    registerInFn<float>(prefix, OMNI_FLOAT);
    registerInFn<double>(prefix, OMNI_DOUBLE);
    registerInFn<std::string_view>(prefix, OMNI_VARCHAR);
    registerInFn<Decimal128>(prefix, OMNI_DECIMAL128);
}
}
