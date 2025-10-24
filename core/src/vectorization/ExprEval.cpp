/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "ExprEval.h"
#include <iostream>
#include "codegen/expr_evaluator.h"

namespace omniruntime::vectorization {
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace omniruntime::vec;
using namespace omniruntime::op;

template <typename T>
BaseVector *ColumnProjectionHelper(BaseVector *colVec, int32_t numSelectedRows)
{
    if (colVec->GetEncoding() == OMNI_DICTIONARY) {
        return reinterpret_cast<Vector<DictionaryContainer<T>> *>(colVec)->Slice(0, numSelectedRows);
    }
    return reinterpret_cast<Vector<T> *>(colVec)->Slice(0, numSelectedRows);
}

template <typename T>
BaseVector *ColumnProjectionCopyPositionsHelper(BaseVector *colVec, int32_t *selectedRows, int32_t numSelectedRows)
{
    if (colVec->GetEncoding() == OMNI_DICTIONARY) {
        return reinterpret_cast<Vector<DictionaryContainer<T>> *>(colVec)->CopyPositions(selectedRows, 0,
            numSelectedRows);
    }
    return reinterpret_cast<Vector<T> *>(colVec)->CopyPositions(selectedRows, 0, numSelectedRows);
}

template <typename T>
BaseVector *ColumnProjectionVarCharVectorHelper(BaseVector *colVec, int32_t numSelectedRows)
{
    auto rowCnt = colVec->GetSize();
    if (numSelectedRows != 0 && numSelectedRows == rowCnt) {
        if (colVec->GetEncoding() == OMNI_DICTIONARY) {
            return reinterpret_cast<Vector<DictionaryContainer<T>> *>(colVec)->Slice(0, numSelectedRows);
        } else {
            return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(colVec)->
                Slice(0, numSelectedRows);
        }
    }
    return nullptr;
}

template <typename T>
void SetConstantValues(T value, BaseVector *outVec)
{
    auto rowCount = outVec->GetSize();
    if constexpr (std::is_same_v<T, std::string_view>) {
        auto outputVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(outVec);
        for (int32_t i = 0; i < rowCount; i++) {
            outputVector->SetValue(i, value);
        }
    } else {
        auto outputVector = static_cast<Vector<T> *>(outVec);
        for (int32_t i = 0; i < rowCount; i++) {
            outputVector->SetValue(i, value);
        }
    }
}

void NullColumnProjection(ExecutionContext *context, BaseVector *outVec)
{
    auto outNulls = unsafe::UnsafeBaseVector::GetNulls(outVec);
    auto outNullsSize = BitUtil::Nbytes(outVec->GetSize());
    auto result = memset_s(outNulls, outNullsSize, -1, outNullsSize);
    if (result != EOK) {
        std::string errorMessage = "Memset failed, ret " + std::to_string(result) + " destMax " +
            std::to_string(outNullsSize) + " count " + std::to_string(outNullsSize);
        OMNI_THROW("Runtime error", "NullColumnProjection:" + errorMessage);
    }
}

void ConstantColumnProjection(ExecutionContext *context, BaseVector *outVec, const LiteralExpr &literalVal)
{
    if (literalVal.isNull) {
        return NullColumnProjection(context, outVec);
    }
    switch (literalVal.GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            SetConstantValues<int32_t>(literalVal.intVal, outVec);
            break;
        case OMNI_SHORT:
            SetConstantValues<int16_t>(literalVal.shortVal, outVec);
            break;
        case OMNI_BYTE:
            SetConstantValues<int8_t>(literalVal.byteVal, outVec);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
        case OMNI_TIMESTAMP:
            SetConstantValues<int64_t>(literalVal.longVal, outVec);
            break;
        case OMNI_DOUBLE:
            SetConstantValues<double>(literalVal.doubleVal, outVec);
            break;
        case OMNI_BOOLEAN:
            SetConstantValues<bool>(literalVal.boolVal, outVec);
            break;
        case OMNI_DECIMAL128: {
            std::string dec128String = *literalVal.stringVal;
            Decimal128 decimal128(dec128String);
            SetConstantValues<Decimal128>(decimal128, outVec);
            break;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            SetConstantValues<std::string_view>(*literalVal.stringVal, outVec);
            break;
        default:
            std::string errorMessage = "Do not support such vector type " +
                std::to_string(literalVal.GetReturnTypeId());
            OMNI_THROW("Runtime error", "NullColumnProjection:" + errorMessage);
    }
}

ExprEval::ExprEval(VectorBatch *vectorBatch, ExecutionContext *context): context(context)
{
    for (int i = 0; i < vectorBatch->GetVectorCount(); i++) {
        vecBatch_.push_back(vectorBatch->Get(i));
        typeIds.push_back(vectorBatch->Get(i)->GetTypeId());
    }
    rowSize = vectorBatch->GetRowCount();
}

void ExprEval::Visit(const LiteralExpr &e)
{
    if (!e.isRoot) {
        auto typeId = e.dataType->GetId();
        switch (e.dataType->GetId()) {
            case OMNI_INT:
            case OMNI_DATE32:
                inputValues_.push(new ConstVector(e.intVal, typeId));
                break;
            case OMNI_SHORT:
                inputValues_.push(new ConstVector(e.shortVal, typeId));
                break;
            case OMNI_BYTE:
                inputValues_.push(new ConstVector(e.byteVal, typeId));
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                inputValues_.push(new ConstVector(e.longVal, typeId));
                break;
            case OMNI_DOUBLE:
                inputValues_.push(new ConstVector(e.doubleVal, typeId));
                break;
            case OMNI_FLOAT:
                inputValues_.push(new ConstVector(e.floatVal, typeId));
                break;
            case OMNI_BOOLEAN:
                inputValues_.push(new ConstVector(e.boolVal, typeId));
                break;
            case OMNI_DECIMAL128:
                inputValues_.push(new ConstVector(e.stringVal, typeId));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                inputValues_.push(new ConstVector(*e.stringVal, typeId));
                break;
            default: LogError("Do not support such vector type %d", typeIds[e.dataType->GetId()]);
        }
        return;
    }
    BaseVector *outVec = VectorHelper::CreateFlatVector(e.dataType->GetId(), rowSize);
    ConstantColumnProjection(context, outVec, e);
    inputValues_.push(outVec);
}

void ExprEval::Visit(const FieldExpr &e)
{
    auto colVec = vecBatch_[e.colVal];
    vecBatch_[e.colVal]->SetIsField(true);
    if (context->hasFilter) {
        auto isSelect = context->GetIsSelectRow();
        int selectRow[context->GetResultRowSize()] = {-1};
        int selectSize = 0;
        for (int i = 0; i < context->GetResultRowSize(); i++) {
            if (isSelect[i]) {
                selectRow[selectSize] = i;
                ++selectSize;
            }
        }
        switch (typeIds[e.colVal]) {
            case OMNI_INT:
            case OMNI_DATE32:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<int32_t>(colVec, selectRow, rowSize));
                break;
            case OMNI_SHORT:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<int16_t>(colVec, selectRow, rowSize));
                break;
            case OMNI_BYTE:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<int8_t>(colVec, selectRow, rowSize));
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<int64_t>(colVec, selectRow, rowSize));
                break;
            case OMNI_DOUBLE:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<double>(colVec, selectRow, rowSize));
                break;
            case OMNI_FLOAT:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<float>(colVec, selectRow, rowSize));
                break;
            case OMNI_BOOLEAN:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<bool>(colVec, selectRow, rowSize));
                break;
            case OMNI_DECIMAL128:
                inputValues_.push(ColumnProjectionCopyPositionsHelper<Decimal128>(colVec, selectRow, rowSize));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                inputValues_.push(ColumnProjectionVarCharVectorHelper<std::string_view>(colVec, rowSize));
                break;
            case OMNI_ARRAY:
                inputValues_.push(reinterpret_cast<ArrayVector *>(colVec)->Slice(0, rowSize));
                break;
            case OMNI_MAP:
                inputValues_.push(reinterpret_cast<MapVector *>(colVec)->Slice(0, rowSize));
                break;
            default: LogError("Do not support such vector type %d", typeIds[e.colVal]);
        }
        return;
    }
    switch (typeIds[e.colVal]) {
        case OMNI_INT:
        case OMNI_DATE32:
            inputValues_.push(ColumnProjectionHelper<int32_t>(colVec, rowSize));
            break;
        case OMNI_SHORT:
            inputValues_.push(ColumnProjectionHelper<int16_t>(colVec, rowSize));
            break;
        case OMNI_BYTE:
            inputValues_.push(ColumnProjectionHelper<int8_t>(colVec, rowSize));
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            inputValues_.push(ColumnProjectionHelper<int64_t>(colVec, rowSize));
            break;
        case OMNI_DOUBLE:
            inputValues_.push(ColumnProjectionHelper<double>(colVec, rowSize));
            break;
        case OMNI_FLOAT:
            inputValues_.push(ColumnProjectionHelper<float>(colVec, rowSize));
            break;
        case OMNI_BOOLEAN:
            inputValues_.push(ColumnProjectionHelper<bool>(colVec, rowSize));
            break;
        case OMNI_DECIMAL128:
            inputValues_.push(ColumnProjectionHelper<Decimal128>(colVec, rowSize));
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            inputValues_.push(ColumnProjectionVarCharVectorHelper<std::string_view>(colVec, rowSize));
            break;
        case OMNI_ARRAY:
            inputValues_.push(reinterpret_cast<ArrayVector *>(colVec)->Slice(0, rowSize));
            break;
        case OMNI_MAP:
            inputValues_.push(reinterpret_cast<MapVector *>(colVec)->Slice(0, rowSize));
            break;
        default: LogError("Do not support such vector type %d", typeIds[e.colVal]);
    }
}

void ExprEval::Visit(const UnaryExpr &e)
{
    e.exp->Accept(*this);
    auto result = VectorHelper::CreateFlatVector(e.dataType->GetId(), rowSize);
    e.vectorFunction->Apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const BinaryExpr &e)
{
    e.left->Accept(*this);
    e.right->Accept(*this);

    BaseVector *result = nullptr;
    e.vectorFunction->Apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const InExpr &e) {}

void ExprEval::Visit(const BetweenExpr &e) {}

void ExprEval::Visit(const IfExpr &e) {}

void ExprEval::Visit(const CoalesceExpr &e) {}

void ExprEval::Visit(const IsNullExpr &e)
{
    e.value->Accept(*this);
    auto result = VectorHelper::CreateFlatVector(e.dataType->GetId(), rowSize);
    e.vectorFunction->Apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const FuncExpr &e)
{
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }

    BaseVector *result = nullptr;
    e.vectorFunction->Apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const SwitchExpr &e) {}

void ExprEval::VisitExpr(const Expr &e)
{
    e.Accept(*this);
}

BaseVector *ExprEval::GetResult()
{
    auto result = inputValues_.top();
    inputValues_.pop();
    return result;
}

int32_t ExprEval::GetRowCount() const
{
    return rowSize;
}
}
