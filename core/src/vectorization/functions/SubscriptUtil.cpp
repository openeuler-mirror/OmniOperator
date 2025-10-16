/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "SubscriptUtil.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// Decode arguments and transform result into a dictionaryVector where the
/// dictionary maintains a mapping from a given row to the index of the input
/// map value vector. This allows us to ensure that element_at is zero-copy.
template <DataTypeId kind>
VectorPtr ApplyMapTyped(const SelectivityVector &rows, const VectorPtr mapArg, const VectorPtr indexArg,
    ExecutionContext *context)
{
    using KeyType = typename NativeType<kind>::type;
    auto rowSize = mapArg->GetSize();
    int32_t dicIndex[rowSize];
    memset(dicIndex, -1, sizeof(dicIndex));

    auto mapVector = reinterpret_cast<MapVector *>(mapArg);
    if (indexArg->GetEncoding() == OMNI_ENCODING_CONST) {
        auto offset = mapVector->GetOffsets();
        int32_t index = 0;
        if constexpr (kind == OMNI_VARCHAR || kind == OMNI_CHAR) {
            auto constKeyVector = dynamic_cast<ConstVector<std::string> *>(indexArg);
            auto key = constKeyVector->GetConstValue();
            auto keyVector = dynamic_cast<Vector<LargeStringContainer<KeyType>> *>(mapVector->GetKeyVector().get());
            int32_t i = 0;
            while (i < keyVector->GetSize()) {
                if (keyVector->GetValue(i) == key) {
                    dicIndex[index] = i;
                    i = offset[index + 1];
                    ++index;
                    continue;
                }
                ++i;
                if (i >= offset[index + 1]) {
                    ++index;
                }
            }
            // auto rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(mapVector->
            //     GetValueVector().get())->Slice(0, rowSize);
            auto rawVector = VectorHelper::SliceVector(mapVector->GetValueVector().get(), 0, rowSize);
            auto result = VectorHelper::CreateDictionaryVector(dicIndex, rowSize, rawVector, rawVector->GetTypeId());
            return result;
        } else {
            auto keyVector = dynamic_cast<Vector<KeyType> *>(reinterpret_cast<MapVector *>(mapArg)->GetKeyVector().
                get());
        }
    } else {
        OMNI_THROW("ApplyMapTyped Error:", "Not support Key type!");
    }
}

VectorPtr SubscriptImpl::applyMap(const SelectivityVector &rows, std::vector<VectorPtr> &args,
    ExecutionContext *context) const
{
    auto mapArg = args[1];
    auto indexArg = args[0];

    // Ensure map key type and second argument are the same.
    // VELOX_CHECK(mapArg->type()->childAt(0)->equivalent(*indexArg->type()));

    // bool triggerCaching = shouldTriggerCaching(mapArg);
    if (indexArg->GetTypeId() != OMNI_ARRAY) {
        return DYNAMIC_TYPE_DISPATCH(ApplyMapTyped, indexArg->GetTypeId(), rows, mapArg, indexArg, context);
    } else {
        // We use applyMapComplexType when the key type is complex, but also when it
        // provides custom comparison operators because the main difference between
        // applyMapComplexType and applyTyped is that applyMapComplexType calls the
        // Vector's equalValueAt method, which calls the Types custom comparison
        // operator internally.
        OMNI_THROW("ApplyMapTyped Error:", "Not support Key type!");
    }
}
}
