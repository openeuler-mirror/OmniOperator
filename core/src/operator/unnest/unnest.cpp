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

UnnestOperator::UnnestOperator(std::shared_ptr<const UnnestNode> planNode)
    : withOrdinality_(planNode->withOrdinality()),
      outputVecBatch(nullptr)
{
    const auto& unnestVariables = planNode->unnestVariables();
    for (const auto& variable : unnestVariables) {
        auto fieldExpr = dynamic_cast<FieldExpr *>(variable);
        if (!fieldExpr->FieldIsArray() && !fieldExpr->FieldIsMap()) {
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
    rawMaxSizes_.resize(vecBatch->Get(0)->GetSize());
    std::fill(rawMaxSizes_.begin(), rawMaxSizes_.end(), 1);
    for (size_t channel = 0; channel < unnestChannels_.size(); ++channel) {
        const auto& unnestVector = vecBatch->Get(unnestChannels_[channel]);

        auto processVector = [&](auto* inputVector) {
            for (auto row = 0; row < unnestVector->GetSize(); ++row) {
                auto rowSize = inputVector->GetSize(row);
                rawMaxSizes_[row] = (rowSize > rawMaxSizes_[row]) ? rowSize : rawMaxSizes_[row];
            }
        };

        if (auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(unnestVector)) {
            processVector(arrayVector);
        } else if (auto mapVector = dynamic_cast<omniruntime::vec::MapVector*>(unnestVector)) {
            processVector(mapVector);
        }
    }

    numElements = std::max(std::accumulate(rawMaxSizes_.begin(), rawMaxSizes_.end(), 0), vecBatch->Get(0)->GetSize());
    generateOutput(numElements, vecBatch);
    return 0;
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
    generateRepeatedColumns(numElements, vecBatch, result.get());
    generateUnrepeatedColumns(numElements, vecBatch, result.get());

    if (withOrdinality_) {
        generateOrdinalityColumns(numElements, vecBatch, result.get());
    }

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

void UnnestOperator::generateComplexRepeatedValuesForType(DataTypeId typeId, int32_t inputSize, auto* inputVector, auto* outputVector,
                                                          BaseVector* inputElementVector, BaseVector* outputElementVector)
{
    switch (typeId) {
        case OMNI_INT:
        case OMNI_DATE32:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<int32_t>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<int32_t>*>(outputElementVector));
            break;
        case OMNI_SHORT:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<int16_t>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<int16_t>*>(outputElementVector));
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<int64_t>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<int64_t>*>(outputElementVector));
            break;
        case OMNI_DECIMAL128:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<Decimal128>*>(outputElementVector));
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(outputElementVector));
            break;
        case OMNI_DOUBLE:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<double>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<double>*>(outputElementVector));
            break;
        case OMNI_BOOLEAN:
            generateComplexRepeatedValues(inputSize, inputVector, outputVector,
                                          dynamic_cast<omniruntime::vec::Vector<bool>*>(inputElementVector),
                                          dynamic_cast<omniruntime::vec::Vector<bool>*>(outputElementVector));
            break;
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "This type not supported yet");
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
    generateComplexRepeatedValuesForType(typeId, inputSize, inputArrayVector, outputArrayVector, inputElementVector, outputElementVector);
}

void UnnestOperator::generateMapRepeatedValues(omniruntime::vec::BaseVector* inputVector,
                                               omniruntime::vec::BaseVector* outputVector)
{
    auto inputMapVector = dynamic_cast<omniruntime::vec::MapVector*>(inputVector);
    auto outputMapVector = dynamic_cast<omniruntime::vec::MapVector*>(outputVector);
    auto inputKeyVector = inputMapVector->GetKeyVector().get();
    auto inputValueVector = inputMapVector->GetValueVector().get();
    auto outputKeyVector = outputMapVector->GetKeyVector().get();
    auto outputValueVector = outputMapVector->GetValueVector().get();
    auto keyTypeId = inputKeyVector->GetTypeId();
    auto valueTypeid = inputValueVector->GetTypeId();
    auto inputSize = inputVector->GetSize();

    generateComplexRepeatedValuesForType(keyTypeId, inputSize, inputMapVector, outputMapVector, inputKeyVector, outputKeyVector);
    generateComplexRepeatedValuesForType(valueTypeid, inputSize, inputMapVector, outputMapVector, inputValueVector, outputValueVector);
}

template<typename VectorType>
void UnnestOperator::generateComplexRepeatedValues(int32_t inputSize, auto* inputVector, auto* outputVector,
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

    auto insetVector = [&](auto* unrepeatVector, auto* outputVector) {
        for (auto i = 0; i < inputSize; ++i) {
            if (unrepeatVector->IsNull(i)) {
                for (auto j = 0; j < rawMaxSizes_[i]; ++j) {
                    outputVector->SetNull(index++);
                }
            } else {
                auto start = unrepeatVector->GetOffset(i);
                auto length = unrepeatVector->GetSize(i);
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
    };

    if (auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector)) {
        insetVector(arrayVector, outputVector);
    } else if (auto mapVector = dynamic_cast<omniruntime::vec::MapVector*>(inputVector)) {
        insetVector(mapVector, outputVector);
    }
}

void UnnestOperator::generateRepeatedColumns(int32_t numElements, omniruntime::vec::VectorBatch* vecBatch,
                                             omniruntime::vec::VectorBatch* resultVecBatch)
{
    for (const auto& projection : identityProjections_) {
        omniruntime::vec::BaseVector* inputVector = vecBatch->Get(projection.inputChannel);
        if (inputVector->GetEncoding() != OMNI_FLAT && inputVector->GetEncoding() != OMNI_ENCODING_ARRAY &&
            inputVector->GetEncoding() != OMNI_ENCODING_MAP) {
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
        } else if (auto mapVector = dynamic_cast<omniruntime::vec::MapVector*>(inputVector)) {
            int64_t totalSize = 0;
            for (auto i = 0; i < inputVector->GetSize(); ++i) {
                totalSize += mapVector->GetSize(i) * rawMaxSizes_[i];
            }
            auto keyVector = mapVector->GetKeyVector().get();
            auto valueVector = mapVector->GetValueVector().get();
            if (keyVector->GetEncoding() != OMNI_FLAT || valueVector->GetEncoding() != OMNI_FLAT) {
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "Unnest not support this encoding.");
            }
            auto newKeyVector = std::shared_ptr<BaseVector>(VectorHelper::CreateVector(OMNI_FLAT, keyVector->GetTypeId(), totalSize));
            auto newValueVector = std::shared_ptr<BaseVector>(VectorHelper::CreateVector(OMNI_FLAT, valueVector->GetTypeId(), totalSize));
            outputVector = new omniruntime::vec::MapVector(numElements, newKeyVector, newValueVector);
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
            case OMNI_MAP:
                generateMapRepeatedValues(inputVector, outputVector);
                break;
            default:
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
        if (auto arrayVector = dynamic_cast<omniruntime::vec::ArrayVector*>(inputVector)) {
            auto elementVector = arrayVector->GetElementVector().get();
            resultVecBatch->SetVector(resultIndex++, generateUnrepeatedValuesForType(elementVector, inputVector, numElements));
        } else if (auto mapVector = dynamic_cast<omniruntime::vec::MapVector*>(inputVector)) {
            auto keyElementVector = mapVector->GetKeyVector().get();
            resultVecBatch->SetVector(resultIndex++, generateUnrepeatedValuesForType(keyElementVector, inputVector, numElements));
            auto valueElementVector = mapVector->GetValueVector().get();
            resultVecBatch->SetVector(resultIndex++, generateUnrepeatedValuesForType(valueElementVector, inputVector, numElements));
        }
    }
}

omniruntime::vec::BaseVector* UnnestOperator::generateUnrepeatedValuesForType(omniruntime::vec::BaseVector* elementVector,
                                                                              omniruntime::vec::BaseVector* inputVector, int32_t numElements)
{
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
    return outputVector;
}

void UnnestOperator::generateOrdinalityColumns(int32_t numElements, omniruntime::vec::VectorBatch* vecBatch,
    omniruntime::vec::VectorBatch* resultVecBatch)
{
    int32_t index = 0;
    BaseVector* baseVector = VectorHelper::CreateVector(OMNI_FLAT, OMNI_LONG, numElements);
    auto ordVector = dynamic_cast<omniruntime::vec::Vector<int64_t>*>(baseVector);
    for (size_t i = 0; i < rawMaxSizes_.size(); ++i) {
        for (int64_t j = 0; j < rawMaxSizes_[i]; ++j) {
            ordVector->SetValue(index++, j + 1);
        }
    }
    resultVecBatch->SetVector(outputTypeSize_ - 1, baseVector);
}

OmniStatus UnnestOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
}
}