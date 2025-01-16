/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "selectivity_vector.h"
namespace omniruntime::vec {
// only for UT
template void omniruntime::vec::SelectivityVector::GetSelectivityVectorFromVectorBatch<
    (omniruntime::vec::GetSelectivityVectorMethod)0>(omniruntime::vec::VectorBatch *,
    omniruntime::vec::SelectivityVector &);
template void omniruntime::vec::SelectivityVector::GetSelectivityVectorFromVectorBatch<
    (omniruntime::vec::GetSelectivityVectorMethod)0>(omniruntime::vec::VectorBatch *,
    const std::vector<unsigned long, std::allocator<unsigned long>> &, omniruntime::vec::SelectivityVector &);
template void omniruntime::vec::SelectivityVector::GetSelectivityVectorFromVectorBatch<
    (omniruntime::vec::GetSelectivityVectorMethod)1>(omniruntime::vec::VectorBatch *,
    omniruntime::vec::SelectivityVector &);
template void omniruntime::vec::SelectivityVector::GetSelectivityVectorFromVectorBatch<
    (omniruntime::vec::GetSelectivityVectorMethod)1>(omniruntime::vec::VectorBatch *,
    const std::vector<unsigned long, std::allocator<unsigned long>> &, omniruntime::vec::SelectivityVector &);

SelectivityVector SelectivityVector::GetSelectivityVectorFromBaseVector(omniruntime::vec::BaseVector *baseVector)
{
    auto rowNum = baseVector->GetSize();
    SelectivityVector result(rowNum, true);
    if (baseVector->HasNull()) {
        for (int i = 0; i < rowNum; i++) {
            if (baseVector->IsNull(i)) {
                result.SetBit(i, false);
            }
        }
    }
    return result;
}


bool SelectivityVector::GetFlatBaseVectorsFromSelectivityVector(std::vector<BaseVector *> &baseVectors,
    SelectivityVector &selectivityVector, std::vector<BaseVector *> &result)
{
    if (selectivityVector.IsAllSelected()) {
        // all selected, filtering is not required.
        return false;
    }
    if (UNLIKELY(baseVectors.empty())) {
        return false;
    }
    size_t resultSize = selectivityVector.CountSelected();
    size_t rowCount = baseVectors[0]->GetSize();
    size_t encodingType = baseVectors[0]->GetEncoding();
    if (UNLIKELY(encodingType == OMNI_DICTIONARY)) {
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "OMNI_DICTIONARY is unsupported.");
    }
    result.resize(resultSize, nullptr);
    for (size_t i = 0; i < baseVectors.size(); i++) {
        auto baseVector = baseVectors[i];
        auto dataType = baseVector->GetTypeId();
        auto selectedBaseVector = VectorHelper::CreateVector(OMNI_FLAT, dataType, static_cast<int32_t>(resultSize));
        size_t index = 0;
        for (auto j = 0; j < rowCount; j++) {
            if (selectivityVector.IsValid(j)) {
                switch (dataType) {
                    case OMNI_INT:
                    case OMNI_DATE32: {
                        using FlatVector = Vector<int32_t>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_SHORT: {
                        using FlatVector = Vector<int16_t>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_LONG:
                    case OMNI_TIMESTAMP:
                    case OMNI_DECIMAL64: {
                        using FlatVector = Vector<int64_t>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_DOUBLE: {
                        using FlatVector = Vector<double>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_BOOLEAN: {
                        using FlatVector = Vector<bool>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_DECIMAL128: {
                        using FlatVector = Vector<Decimal128>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    case OMNI_VARCHAR:
                    case OMNI_CHAR: {
                        using FlatVector = Vector<LargeStringContainer<std::string_view>>;
                        auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                        static_cast<FlatVector *>(selectedBaseVector)->SetValue(index, value);
                        break;
                    }
                    default: {
                        LogError("No such %d type support", dataType);
                        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                            "unsupported selectivity type: " + std::to_string(static_cast<int>(dataType)));
                    }
                }
                index++;
            }
        }
        result[i] = selectedBaseVector;
    }
    return true;
}

template <GetSelectivityVectorMethod type>
void SelectivityVector::GetSelectivityVectorFromVectorBatch(omniruntime::vec::VectorBatch *vectorBatch,
    const std::vector<size_t> &selectedColumns, SelectivityVector &selectivityVector)
{
    auto rowNum = vectorBatch->GetRowCount();
    SelectivityVector singleColumnSelectivityVector(rowNum, true);
    if constexpr (type == GetSelectivityVectorMethod::AND) {
        for (unsigned long selectedColumn : selectedColumns) {
            auto currentColumn = vectorBatch->Get(selectedColumn);
            for (int i = 0; i < rowNum; i++) {
                if (currentColumn->IsNull(i)) {
                    singleColumnSelectivityVector.SetBit(i, false);
                }
            }
            selectivityVector.And(singleColumnSelectivityVector);
            singleColumnSelectivityVector.SetAll();
        }
    } else if constexpr (type == GetSelectivityVectorMethod::OR) {
        for (unsigned long selectedColumn : selectedColumns) {
            auto currentColumn = vectorBatch->Get(selectedColumn);
            for (int i = 0; i < rowNum; i++) {
                if (currentColumn->IsNull(i)) {
                    singleColumnSelectivityVector.SetBit(i, false);
                }
            }
            selectivityVector.Or(singleColumnSelectivityVector);
            singleColumnSelectivityVector.SetAll();
        }
    } else {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
            "unsupported selectivity type: " + std::to_string(static_cast<int>(type)));
    }
}

template <GetSelectivityVectorMethod type>
void SelectivityVector::GetSelectivityVectorFromVectorBatch(omniruntime::vec::VectorBatch *vectorBatch,
    SelectivityVector &selectivityVector)
{
    auto vectorBatchSize = vectorBatch->GetVectorCount();
    auto rowNum = vectorBatch->GetRowCount();
    if constexpr (type == GetSelectivityVectorMethod::AND) {
        SelectivityVector singleColumnSelectivityVector(rowNum, true);
        for (size_t i = 0; i < vectorBatchSize; i++) {
            auto currentColumn = vectorBatch->Get(i);
            for (int j = 0; j < rowNum; j++) {
                if (currentColumn->IsNull(j)) {
                    singleColumnSelectivityVector.SetBit(j, false);
                }
            }
            selectivityVector.And(singleColumnSelectivityVector);
            singleColumnSelectivityVector.SetAll();
        }
    } else if constexpr (type == GetSelectivityVectorMethod::OR) {
        SelectivityVector singleColumnSelectivityVector(rowNum, false);
        for (size_t i = 0; i < vectorBatchSize; i++) {
            auto currentColumn = vectorBatch->Get(i);
            for (int j = 0; j < rowNum; j++) {
                if (currentColumn->IsNull(j)) {
                    singleColumnSelectivityVector.SetBit(j, false);
                }
            }
            selectivityVector.Or(singleColumnSelectivityVector);
            singleColumnSelectivityVector.SetAll();
        }
    } else {
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
            "unsupported selectivity type: " + std::to_string(static_cast<int>(type)));
    }
}
}
