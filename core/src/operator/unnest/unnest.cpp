/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "unnest.h"

namespace omniruntime {
namespace op {

UnnestOperatorFactory* UnnestOperatorFactory::CreateUnnestOperatorFactory(
    std::shared_ptr<const UnnestNode> planNode)
{
    return new UnnestOperatorFactory(planNode);
}

Operator* UnnestOperatorFactory::CreateOperator()
{
    UnnestOperator *unnestOperator = new UnnestOperator(planNode_);
    return unnestOperator;
}

UnnestOperator::UnnestOperator(std::shared_ptr<const UnnestNode> planNode) : outputVecBatch(nullptr)
{
    const auto& inputType = planNode->Sources()[0]->OutputType();
    const auto& unnestVariables = planNode->unnestVariables();
    for (const auto& variable : unnestVariables) {
        auto fieldExpr = dynamic_cast<FieldExpr *>(variable);
        if (!fieldExpr->FieldIsArray()) {
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "Unnest operator supports only ARRAY and MAP types");
        }
        unnestChannels_.push_back(fieldExpr->colVal);
    }

    uint32_t outputChannel = 0;
    for (const auto& variable : planNode->replicateVariables()) {
        auto fieldExpr = dynamic_cast<FieldExpr *>(variable);
        identityProjections_.emplace_back(fieldExpr->colVal, outputChannel++);
    }
    outputTypeSize_ = planNode->OutputType()->GetSize();
}


int32_t UnnestOperator::AddInput(omniruntime::vec::VectorBatch* vecBatch)
{
    if (vecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        return 0;
    }
    int32_t numElements = 0;
    rawMaxSizes_.resize(vecBatch->Get(0)->GetSize(), 1);
    for (size_t channel = 0; channel < unnestChannels_.size(); ++channel) {
        const auto& unnestVector = vecBatch->Get(unnestChannels_[channel]);

        if (auto inputVector = dynamic_cast<omniruntime::vec::ArrayVector*>(unnestVector)) {
            for (auto row = 0; row < unnestVector->GetSize(); ++row) {
                auto rowSize = inputVector->GetSize(row);
                rawMaxSizes_[row] = (rowSize > rawMaxSizes_[row]) ? rowSize : rawMaxSizes_[row];
            }
        }
    }

    numElements = std::max(std::accumulate(rawMaxSizes_.begin(), rawMaxSizes_.end(), 0), vecBatch->Get(0)->GetSize());
    generateOutput(numElements, vecBatch);
}


int32_t UnnestOperator::GetOutput(omniruntime::vec::VectorBatch** resultVecBatch)
{
    if (outputVecBatch != nullptr) {
        *resultVecBatch = outputVecBatch;
        outputVecBatch = nullptr;
        return 0;
    }

    if (noMoreInput_) {
        SetStatus(OMNI_STATUS_FINISHED);
    }
    return 0;
}

void UnnestOperator::generateOutput(int32_t numElements, omniruntime::vec::VectorBatch *vecBatch)
{
    auto result = std::make_unique<VectorBatch>(numElements);
    result->ResizeVectorCount(outputTypeSize_);
    int32_t vecBatchSize = vecBatch->GetRowCount();
    generateRepeatedColumns(numElements, vecBatch, result.get());
    generateUnrepeatedColumns(numElements, vecBatch, result.get());

    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    outputVecBatch = result.release();
}

template<typename VectorType>
void UnnestOperator::generateRepeatedValues(VectorType* inputVector, VectorType* outputVector)
{
    int32_t index = 0;
    int32_t inputSize = inputVector->GetSize();
    for (auto i = 0; i < inputSize; ++i) {
        if (inputVector->IsNull(i)) {
            for (auto j = 0; j < rawMaxSizes_[i]; ++j) {
                outputVector->SetNull(index++);
            }
        } else {
            auto value = inputVector->GetValue(i);
            for (auto j = 0; j < rawMaxSizes_[i]; ++j) {
                outputVector->SetValue(index++, value);
            }
        }
    }
}

void UnnestOperator::generateArrayRepeatedValues(omniruntime::vec::BaseVector* inputVector,
                                                 omniruntime::vec::BaseVector* outputVector)
{
    auto inputArrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector);
    auto outputArrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(outputVector);
    auto inputElementVector = inputArrayVector->GetElementVector().get();
    auto outputElementVector = outputArrayVector->GetElementVector().get();
    auto typeId = inputElementVector->GetTypeId();
    auto inputSize = inputVector->GetSize();

    switch (typeId) {
        case OMNI_INT:
        case OMNI_DATE32:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<int32_t>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<int32_t>*>(outputElementVector));
            break;
        case OMNI_SHORT:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<int16_t>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<int16_t>*>(outputElementVector));
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<int64_t>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<int64_t>*>(outputElementVector));
            break;
        case OMNI_DECIMAL128:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(inputElementVector),
                dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(outputElementVector));
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(outputElementVector));
            break;
        case OMNI_DOUBLE:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<double>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<double>*>(outputElementVector));
            break;
        case OMNI_BOOLEAN:
            generateArrayRepeatedValuesBase(inputSize, inputArrayVector, outputArrayVector,
                                            dynamic_cast<omniruntime::vec::Vector<bool>*>(inputElementVector),
                                            dynamic_cast<omniruntime::vec::Vector<bool>*>(outputElementVector));
            break;
        default:
            std::cout << typeId << std::endl;
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "2This type not supported yet");
    }
}

template<typename VectorType>
void UnnestOperator::generateArrayRepeatedValuesBase(int32_t inputSize, omniruntime::vec::ArrayVector* inputVector,
                                                     omniruntime::vec::ArrayVector* outputVector,
                                                     VectorType* inputElementVector, VectorType* outputElementVector)
{
    int32_t index = 0;
    int32_t elementIndex = 0;
    for (auto i = 0; i < inputSize; ++i) {
        int64_t start = inputVector->GetOffset(i);
        int64_t end = inputVector->GetOffset(i + 1);
        for (auto j = 0; j < rawMaxSizes_[i]; ++j) {
            for (auto k = start; k < end; ++k) {
                if (inputElementVector->IsNull(k)) {
                    outputElementVector->SetNull(elementIndex++);
                } else {
                    auto value = inputElementVector->GetValue(k);
                    outputElementVector->SetValue(elementIndex++, value);
                }
            }
            outputVector->SetSize(index++, end - start);
        }
    }
}

template<typename VectorType>
void UnnestOperator::generateUnrepeatedValues(omniruntime::vec::BaseVector* inputVector, VectorType* elementVector, VectorType* outputVector)
{
    int32_t index = 0;
    int32_t inputSize = inputVector->GetSize();
    auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector);
    for (auto i = 0; i < inputSize; ++i) {
        if (arrayVector->IsNull(i)) {
            for (auto j = 0; j < rawMaxSizes_[i]; ++j) {
                outputVector->SetNull(index++);
            }
        } else {
            auto start = arrayVector->GetOffset(i);
            auto length = arrayVector->GetSize(i);
            for (auto j = 0; j < length; ++j) {
                auto value = elementVector->GetValue(start + j);
                outputVector->SetValue(index++, value);
            }
            if (rawMaxSizes_[i] > length) {
                for (auto j = length; j < rawMaxSizes_[i]; ++j) {
                    outputVector->SetNull(index++);
                }
            }
        }
    }
}

void UnnestOperator::generateRepeatedColumns(int32_t numElements, omniruntime::vec::VectorBatch* vecBatch,
                                             omniruntime::vec::VectorBatch* resultVecBatch)
{
    for (const auto& projection : identityProjections_) {
        omniruntime::vec::BaseVector* inputVector = vecBatch->Get(projection.inputChannel);
        if (inputVector->GetEncoding() != OMNI_FLAT && inputVector->GetEncoding() != OMNI_ENCODING_ARRAY) {
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "Unnest not support this encoding.");
        }
        auto typeId = inputVector->GetTypeId();
        omniruntime::vec::BaseVector* outputVector = nullptr;
        if (auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector)) {
            int64_t totalSize = 0;
            for (auto i = 0; i < inputVector->GetSize(); ++i) {
                totalSize += arrayVector->GetSize(i) * rawMaxSizes_[i];
            }
            auto elementVector = arrayVector->GetElementVector().get();
            if (elementVector->GetEncoding() != OMNI_FLAT) {
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "Unnest not support this encoding.");
            }
            auto typeId = elementVector->GetTypeId();
            auto newElementVector = std::shared_ptr<BaseVector>(VectorHelper::CreateVector(OMNI_FLAT, typeId, totalSize));
            outputVector = new omniruntime::vec::ArrayVector(numElements, newElementVector);
        } else {
            outputVector = VectorHelper::CreateVector(OMNI_FLAT, typeId, numElements);
        }

        switch (typeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<int32_t>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<int32_t>*>(outputVector));
                break;
            case OMNI_SHORT:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<int16_t>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<int16_t>*>(outputVector));
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<int64_t>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<int64_t>*>(outputVector));
                break;
            case OMNI_DECIMAL128:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(outputVector));
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(outputVector));
                break;
            case OMNI_DOUBLE:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<double>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<double>*>(outputVector));
                break;
            case OMNI_BOOLEAN:
                generateRepeatedValues(dynamic_cast<omniruntime::vec::Vector<bool>*>(inputVector),
                                       dynamic_cast<omniruntime::vec::Vector<bool>*>(outputVector));
                break;
            case OMNI_ARRAY:
                generateArrayRepeatedValues(inputVector, outputVector);
                break;
            default:
                std::cout << typeId << std::endl;
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "This type not supported yet");
        }

        resultVecBatch->SetVector(projection.outputChannel, outputVector);
    }
}

void UnnestOperator::generateUnrepeatedColumns(int32_t numElements, omniruntime::vec::VectorBatch* vecBatch,
                                               omniruntime::vec::VectorBatch* resultVecBatch)
{
    size_t resultIndex = identityProjections_.size();
    for (auto channel : unnestChannels_) {
        omniruntime::vec::BaseVector* inputVector = vecBatch->Get(channel);
        auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector);
        auto elementVector = arrayVector->GetElementVector().get();
        if (elementVector->GetEncoding() != OMNI_FLAT) {
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "Unnest not support this encoding.");
        }
        auto typeId = elementVector->GetTypeId();
        auto outputVector = VectorHelper::CreateVector(OMNI_FLAT, typeId, numElements);

        switch (typeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<int32_t>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<int32_t>*>(outputVector));
                break;
            case OMNI_SHORT:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<int16_t>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<int16_t>*>(outputVector));
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<int64_t>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<int64_t>*>(outputVector));
                break;
            case OMNI_DECIMAL128:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(outputVector));
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(outputVector));
                break;
            case OMNI_DOUBLE:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<double>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<double>*>(outputVector));
                break;
            case OMNI_BOOLEAN:
                generateUnrepeatedValues(inputVector,
                                         dynamic_cast<omniruntime::vec::Vector<bool>*>(elementVector),
                                         dynamic_cast<omniruntime::vec::Vector<bool>*>(outputVector));
                break;
            default:
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "This type not supported yet");
        }
        resultVecBatch->SetVector(resultIndex++, outputVector);
    }
}

OmniStatus UnnestOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
}
}