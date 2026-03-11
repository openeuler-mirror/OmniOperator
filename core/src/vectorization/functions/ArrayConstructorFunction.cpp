/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayConstructor function implementation
 */

#include "ArrayConstructorFunction.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

bool ArrayConstructorFunction::IsValueNull(BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == OMNI_ENCODING_CONST) {
        return vec->IsNull(0);
    }
    return vec->IsNull(row);
}

template <typename T>
T ArrayConstructorFunction::GetValue(BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == OMNI_ENCODING_CONST) {
        return static_cast<ConstVector<T> *>(vec)->GetConstValue();
    }
    return static_cast<Vector<T> *>(vec)->GetValue(row);
}

std::string_view ArrayConstructorFunction::GetStringValue(BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == OMNI_ENCODING_CONST) {
        auto *constVec = dynamic_cast<ConstVector<std::string_view> *>(vec);
        if (constVec != nullptr) {
            return constVec->GetConstValue();
        }
        return std::string_view();
    }
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    auto *varcharVec = dynamic_cast<VarcharVector *>(vec);
    if (varcharVec != nullptr) {
        return varcharVec->GetValue(row);
    }
    return std::string_view();
}

void ArrayConstructorFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const
{
    std::vector<BaseVector *> argVectors;
    while (!args.empty()) {
        argVectors.push_back(args.top());
        args.pop();
    }
    std::reverse(argVectors.begin(), argVectors.end());

    int32_t rowSize = context->GetResultRowSize();

    if (argVectors.empty()) {
        ConstructEmptyArray(result, rowSize);
        return;
    }

    DataTypeId elementTypeId = argVectors[0]->GetTypeId();

    switch (elementTypeId) {
        case OMNI_BYTE:
            ConstructArray<int8_t>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_SHORT:
            ConstructArray<int16_t>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            ConstructArray<int32_t>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            ConstructArray<int64_t>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_FLOAT:
            ConstructArray<float>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_DOUBLE:
            ConstructArray<double>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_BOOLEAN:
            ConstructArray<bool>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_DECIMAL128:
            ConstructArray<Decimal128>(argVectors, result, rowSize, elementTypeId);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
        case OMNI_VARBINARY:
            ConstructArrayVarchar(argVectors, result, rowSize);
            break;
        case OMNI_ARRAY:
            ConstructArrayNested(argVectors, result, rowSize);
            break;
        default:
            for (auto *vec : argVectors) {
                delete vec;
            }
            OMNI_THROW("ArrayConstructorFunction Error:",
                "Unsupported element type: " + std::to_string(elementTypeId));
    }

    for (auto *vec : argVectors) {
        delete vec;
    }
}

template <typename T>
void ArrayConstructorFunction::ConstructArray(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
    int32_t rowSize, DataTypeId elementTypeId) const
{
    size_t numArgs = argVectors.size();
    int64_t totalElements = static_cast<int64_t>(rowSize) * static_cast<int64_t>(numArgs);

    auto resultElementVec = std::shared_ptr<BaseVector>(
        VectorHelper::CreateFlatVector(static_cast<int32_t>(elementTypeId), static_cast<int32_t>(totalElements)));
    auto *typedResultElementVec = dynamic_cast<Vector<T> *>(resultElementVec.get());
    if (!typedResultElementVec) {
        OMNI_THROW("ArrayConstructorFunction Error:", "Failed to create result element vector");
    }

    auto *arrayResult = new ArrayVector(rowSize, resultElementVec);
    int64_t offset = 0;

    for (int32_t row = 0; row < rowSize; ++row) {
        arrayResult->SetOffset(row, static_cast<int32_t>(offset));

        for (size_t argIdx = 0; argIdx < numArgs; ++argIdx) {
            if (IsValueNull(argVectors[argIdx], row)) {
                typedResultElementVec->SetNull(static_cast<int32_t>(offset));
            } else {
                T value = GetValue<T>(argVectors[argIdx], row);
                typedResultElementVec->SetValue(static_cast<int32_t>(offset), value);
            }
            offset++;
        }

        arrayResult->SetOffset(row + 1, static_cast<int32_t>(offset));
    }

    result = arrayResult;
}

void ArrayConstructorFunction::ConstructArrayVarchar(const std::vector<BaseVector *> &argVectors,
    BaseVector *&result, int32_t rowSize) const
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

    size_t numArgs = argVectors.size();
    int64_t totalElements = static_cast<int64_t>(rowSize) * static_cast<int64_t>(numArgs);

    auto resultElementVec = std::shared_ptr<BaseVector>(
        new VarcharVector(static_cast<int32_t>(totalElements)));
    auto *typedResultElementVec = dynamic_cast<VarcharVector *>(resultElementVec.get());
    if (!typedResultElementVec) {
        OMNI_THROW("ArrayConstructorFunction Error:", "Failed to create result varchar vector");
    }

    auto *arrayResult = new ArrayVector(rowSize, resultElementVec);
    int64_t offset = 0;

    for (int32_t row = 0; row < rowSize; ++row) {
        arrayResult->SetOffset(row, static_cast<int32_t>(offset));

        for (size_t argIdx = 0; argIdx < numArgs; ++argIdx) {
            if (IsValueNull(argVectors[argIdx], row)) {
                typedResultElementVec->SetNull(static_cast<int32_t>(offset));
            } else {
                std::string_view value = GetStringValue(argVectors[argIdx], row);
                typedResultElementVec->SetValue(static_cast<int32_t>(offset), value);
            }
            offset++;
        }

        arrayResult->SetOffset(row + 1, static_cast<int32_t>(offset));
    }

    result = arrayResult;
}

void ArrayConstructorFunction::ConstructArrayNested(const std::vector<BaseVector *> &argVectors,
    BaseVector *&result, int32_t rowSize) const
{
    size_t numArgs = argVectors.size();
    int64_t totalMiddleElements = static_cast<int64_t>(rowSize) * static_cast<int64_t>(numArgs);

    auto *firstArrayArg = dynamic_cast<ArrayVector *>(argVectors[0]);
    if (!firstArrayArg) {
        OMNI_THROW("ArrayConstructorFunction Error:", "Expected ArrayVector for nested array construction");
    }
    auto innerElementVec = firstArrayArg->GetElementVector();
    DataTypeId innerElemTypeId = innerElementVec ? innerElementVec->GetTypeId() : OMNI_INT;

    int64_t totalInnerElements = 0;
    for (size_t argIdx = 0; argIdx < numArgs; ++argIdx) {
        auto *arrayArg = dynamic_cast<ArrayVector *>(argVectors[argIdx]);
        if (!arrayArg) {
            continue;
        }
        for (int32_t row = 0; row < rowSize; ++row) {
            if (!IsValueNull(argVectors[argIdx], row)) {
                totalInnerElements += arrayArg->GetSize(row);
            }
        }
    }

    std::shared_ptr<BaseVector> flatInnerElements;
    if (TypeUtil::IsStringType(innerElemTypeId)) {
        using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
        flatInnerElements = std::shared_ptr<BaseVector>(
            new VarcharVector(static_cast<int32_t>(totalInnerElements)));
    } else {
        flatInnerElements = std::shared_ptr<BaseVector>(
            VectorHelper::CreateFlatVector(static_cast<int32_t>(innerElemTypeId),
                static_cast<int32_t>(totalInnerElements)));
    }

    auto middleArrayVec = std::shared_ptr<BaseVector>(
        new ArrayVector(totalMiddleElements, flatInnerElements));
    auto *middleArray = dynamic_cast<ArrayVector *>(middleArrayVec.get());

    int64_t innerOffset = 0;
    int32_t middleIdx = 0;
    for (int32_t row = 0; row < rowSize; ++row) {
        for (size_t argIdx = 0; argIdx < numArgs; ++argIdx) {
            middleArray->SetOffset(middleIdx, static_cast<int32_t>(innerOffset));

            if (IsValueNull(argVectors[argIdx], row)) {
                middleArray->SetNull(middleIdx);
                middleArray->SetOffset(middleIdx + 1, static_cast<int32_t>(innerOffset));
            } else {
                auto *arrayArg = dynamic_cast<ArrayVector *>(argVectors[argIdx]);
                if (!arrayArg) {
                    middleArray->SetNull(middleIdx);
                    middleArray->SetOffset(middleIdx + 1, static_cast<int32_t>(innerOffset));
                } else {
                    int64_t srcStart = arrayArg->GetOffset(row);
                    int64_t srcLen = arrayArg->GetSize(row);
                    auto srcElemVec = arrayArg->GetElementVector();

                    if (srcLen > 0 && srcElemVec) {
                        BaseVector *srcSlice = srcElemVec->Slice(
                            static_cast<int>(srcStart), static_cast<int>(srcLen), false);
                        VectorHelper::AppendVector(flatInnerElements.get(),
                            static_cast<int32_t>(innerOffset), srcSlice, static_cast<int32_t>(srcLen));
                        delete srcSlice;
                    }
                    innerOffset += srcLen;
                    middleArray->SetOffset(middleIdx + 1, static_cast<int32_t>(innerOffset));
                }
            }
            middleIdx++;
        }
    }

    auto *outerArrayResult = new ArrayVector(rowSize, middleArrayVec);
    int64_t outerOffset = 0;
    for (int32_t row = 0; row < rowSize; ++row) {
        outerArrayResult->SetOffset(row, static_cast<int32_t>(outerOffset));
        outerOffset += static_cast<int64_t>(numArgs);
        outerArrayResult->SetOffset(row + 1, static_cast<int32_t>(outerOffset));
    }

    result = outerArrayResult;
}

void ArrayConstructorFunction::ConstructEmptyArray(BaseVector *&result, int32_t rowSize) const
{
    auto resultElementVec = std::shared_ptr<BaseVector>(
        VectorHelper::CreateFlatVector(static_cast<int32_t>(OMNI_INT), 0));
    auto *arrayResult = new ArrayVector(rowSize, resultElementVec);

    for (int32_t row = 0; row <= rowSize; ++row) {
        arrayResult->SetOffset(row, 0);
    }

    result = arrayResult;
}

std::shared_ptr<VectorFunction> makeArrayConstructor(const std::string &name,
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &)
{
    return std::make_shared<ArrayConstructorFunction>();
}

std::vector<std::shared_ptr<codegen::FunctionSignature>> ArrayConstructorSignatures()
{
    std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;

    std::vector<DataTypeId> supportedTypes = {
        OMNI_BOOLEAN,
        OMNI_BYTE,
        OMNI_SHORT,
        OMNI_INT,
        OMNI_LONG,
        OMNI_FLOAT,
        OMNI_DOUBLE,
        OMNI_VARCHAR,
        OMNI_CHAR,
        OMNI_DATE32,
        OMNI_TIMESTAMP,
        OMNI_DECIMAL64,
        OMNI_DECIMAL128,
        OMNI_VARBINARY,
        OMNI_ARRAY
    };

    for (const auto &typeId : supportedTypes) {
        signatures.emplace_back(
            codegen::FunctionSignature::Variadic("array", typeId, OMNI_ARRAY, 1));
    }

    return signatures;
}

template void ArrayConstructorFunction::ConstructArray<int8_t>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<int16_t>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<int32_t>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<int64_t>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<float>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<double>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<bool>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;
template void ArrayConstructorFunction::ConstructArray<Decimal128>(const std::vector<BaseVector *> &,
    BaseVector *&, int32_t, DataTypeId) const;

template int8_t ArrayConstructorFunction::GetValue<int8_t>(BaseVector *, int32_t);
template int16_t ArrayConstructorFunction::GetValue<int16_t>(BaseVector *, int32_t);
template int32_t ArrayConstructorFunction::GetValue<int32_t>(BaseVector *, int32_t);
template int64_t ArrayConstructorFunction::GetValue<int64_t>(BaseVector *, int32_t);
template float ArrayConstructorFunction::GetValue<float>(BaseVector *, int32_t);
template double ArrayConstructorFunction::GetValue<double>(BaseVector *, int32_t);
template bool ArrayConstructorFunction::GetValue<bool>(BaseVector *, int32_t);
template Decimal128 ArrayConstructorFunction::GetValue<Decimal128>(BaseVector *, int32_t);

} // namespace omniruntime::vectorization
