/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "ExprEval.h"
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
            return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(colVec)->Slice(0, numSelectedRows);
        }
    }
    return nullptr;
}

ExprEval::ExprEval(VectorBatch *vectorBatch, ExecutionContext *context, const int32_t *typeIds): context(context),
    typeIds(typeIds)
{
    for (int i = 0; i < vectorBatch->GetVectorCount(); i++) {
        vecBatch_.push_back(vectorBatch->Get(i));
    }
    rowSize = vectorBatch->GetRowCount();
}

void ExprEval::Visit(const LiteralExpr &e)
{
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
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const BinaryExpr &e)
{
    e.left->Accept(*this);
    e.right->Accept(*this);

    BaseVector *result = nullptr;
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
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
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const FuncExpr &e)
{
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }

    BaseVector *result = nullptr;
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const SwitchExpr &e) {}

void ExprEval::VisitExpr(const Expr &e)
{
    e.Accept(*this);
}

BaseVector *ExprEval::GetResult()
{
    return inputValues_.top();
}

int32_t ExprEval::GetRowCount() const
{
    return rowSize;
}
}
