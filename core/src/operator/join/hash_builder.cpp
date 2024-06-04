/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: hash builder implementations
 */
#include "hash_builder.h"
#include <vector>
#include <memory>

namespace omniruntime {
namespace op {
HashBuilderOperatorFactory::HashBuilderOperatorFactory(JoinType joinType, const DataTypes &buildTypes,
    const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount)
    : buildTypes(buildTypes),
      buildHashCols(std::vector<int32_t>(buildHashCols, buildHashCols + buildHashColsCount)),
      operatorIndex(0)
{
    if (operatorCount <= 0 || operatorCount > 10000) {
        throw OmniException("OPERATOR_RUNTIME_ERROR", "operatorCount is not in the acceptable range [1, 10000].");
    }
    if (joinType != OMNI_JOIN_TYPE_FULL) {
        hashTablesVariants = InitVariant<RowRefList>(buildHashColsCount, operatorCount, joinType);
    } else {
        hashTablesVariants = InitVariant<RowRefListWithFlags>(buildHashColsCount, operatorCount, joinType);
    }
}

template <class RowRefListType>
HashTableVariants *HashBuilderOperatorFactory::InitVariant(int32_t buildHashColsCount, int32_t operatorCount,
    JoinType joinType)
{
    if (buildHashColsCount == 1) {
        auto type = buildTypes.GetIds()[buildHashCols[0]];
        switch (type) {
            case OMNI_BOOLEAN:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<int8_t, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
            case OMNI_INT:
            case OMNI_DATE32:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<int32_t, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
            case OMNI_LONG:
            case OMNI_DECIMAL64:
            case OMNI_DOUBLE:
            case OMNI_DATE64:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<int64_t, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
            case OMNI_SHORT:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<int16_t, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
            case OMNI_DECIMAL128:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<Decimal128, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
            default:
                return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<StringRef, RowRefListType>>,
                    operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
        }
    } else {
        return new HashTableVariants{ std::in_place_type<JoinHashTableVariants<StringRef, RowRefListType>>,
            operatorCount, &(this->buildTypes), this->buildHashCols, joinType };
    }
}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(JoinType joinType,
    const DataTypes &buildTypes, const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount)
{
    return new HashBuilderOperatorFactory(joinType, buildTypes, buildHashCols, buildHashColsCount, operatorCount);
}

Operator *HashBuilderOperatorFactory::CreateOperator()
{
    int32_t partitionIndex =
        operatorIndex++ % std::visit([&](auto &&arg) { return arg.GetHashTableCount(); }, *hashTablesVariants);
    return new HashBuilderOperator(this->buildTypes, hashTablesVariants, partitionIndex);
}

HashBuilderOperator::HashBuilderOperator(const DataTypes &buildTypes, HashTableVariants *hashTables,
    int32_t partitionIndex)
    : buildTypes(buildTypes), partitionIndex(partitionIndex), hashTablesVariants(hashTables)
{
    SetOperatorName(metricsNameHashBuilder);
}

int32_t HashBuilderOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    auto rowCount = vecBatch->GetRowCount();
    if (rowCount <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        return 0;
    }
    UpdateAddInputInfo(rowCount);
    std::visit([&](auto &&arg) { arg.AddVecBatch(partitionIndex, vecBatch); }, *hashTablesVariants);
    return 0;
}

int32_t HashBuilderOperator::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    std::visit(
        [&](auto &&arg) {
            arg.Prepare(partitionIndex);
            arg.BuildHashTable(partitionIndex);
        },
        *hashTablesVariants);
    if (UNLIKELY(IsDebugEnable())) {
        int32_t hashTableSize = 0;
        auto hasgTableType =
            std::visit([&](auto &&arg) { return arg.GetHashTableTypes(partitionIndex); }, *hashTablesVariants);
        if (hasgTableType == HashTableImplementationType::NORMAL_HASH_TABLE) {
            hashTableSize = std::visit([&](auto &&arg) { return arg.GetHashTable(partitionIndex)->GetElementsSize(); },
                *hashTablesVariants);
        } else {
            hashTableSize = std::visit([&](auto &&arg) { return arg.GetArrayTable(partitionIndex)->GetElementsSize(); },
                *hashTablesVariants);
        }
        UpdateGetOutputInfo(hashTableSize);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashBuilderOperator::Close()
{
    UpdateCloseInfo();
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime