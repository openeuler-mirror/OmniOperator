/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: In expression implementation for SimpleFunction framework (无空值版)
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
            const TInput * /*searchTerm*/, const Array<TInput> *searchElements)
        {
            if (searchElements == nullptr) {
                return;
            }
            elements_.reserve(searchElements->size());

            for (const auto &entry : *searchElements) {
                if (!entry.has_value()) {
                    hasNull_ = true;
                    continue;
                }
                elements_.insert(entry.value());
            }
        }

        ALWAYS_INLINE bool callNullable(bool &result, const TInput *searchTerm, const Array<TInput> * /*array*/)
        {
            if (searchTerm == nullptr) {
                return false;
            }

            result = elements_.find(*searchTerm);
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
void registerInFn(const std::string &prefix)
{
    DataTypeId typeId = TYPE_ID<T>;
    RegisterFunction<InFunctionOuter<T>::template Inner, bool, T, ArrayView<true, T>>(prefix,
        {typeId, OMNI_ARRAY}, OMNI_BOOLEAN);
}
} // namespace

void registerIn(const std::string &prefix)
{
    registerInFn<int8_t>(prefix);
    registerInFn<int16_t>(prefix);
    registerInFn<int32_t>(prefix);
    registerInFn<int64_t>(prefix);
    registerInFn<float>(prefix);
    registerInFn<double>(prefix);
    registerInFn<bool>(prefix);
}
}
