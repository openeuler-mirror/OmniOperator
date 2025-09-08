/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "expression/jsonparser/jsonparser.h"
#include "operator/config/operator_config.h"
#include "util/config/QueryConfig.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace std;

SimpleFilter::SimpleFilter(const Expr &expression)
    : codegen(nullptr), expression(&expression), func(nullptr), initialized(false)
{
    resultLength = new int(0);
    isResultNull = new bool(false);
}

SimpleFilter::~SimpleFilter()
{
    delete this->isResultNull;
    delete this->resultLength;
    this->codegen.reset();
}

SimpleFilter::SimpleFilter(const SimpleFilter &simpleFilter)
{
    this->codegen = simpleFilter.codegen;
    this->expression = simpleFilter.expression;
    this->func = simpleFilter.func;
    this->initialized = simpleFilter.initialized;
    this->resultLength = new int(0);
    this->isResultNull = new bool(false);
    this->isColumnFilter = simpleFilter.isColumnFilter;
}

bool SimpleFilter::Initialize(OverflowConfig *overflowConfig)
{
    if (this->expression == nullptr) {
        LogWarn("Unable to parse expression for simple filter");
        return false;
    }

    if (this->expression->GetReturnTypeId() != OMNI_BOOLEAN) {
        LogWarn("Filter expression can only return boolean, current type: %d", this->expression->GetReturnTypeId());
        return false;
    }

    this->codegen = std::make_unique<SimpleFilterCodeGen>("simple_row_expr_eval", *this->expression, overflowConfig);
    if (this->codegen == nullptr) {
        LogWarn("Unable to generate function for simple filter");
        return false;
    }

    int64_t fAddr = this->codegen->GetFunction();
    if (fAddr == 0) {
        LogWarn("Unable to generate function for simple filter");
        return false;
    }

    void *refFunc = &fAddr;
    this->func = *static_cast<SimpleRowExprEvalFunc *>(refFunc);
    isColumnFilter = this->expression->GetType() == ExprType::FIELD_E && GetVectorIndexes().size() == 1;
    this->initialized = true;
    return true;
}

set<int32_t> &SimpleFilter::GetVectorIndexes() { return this->codegen->vectorIndexes; }

bool SimpleFilter::Evaluate(int64_t *values, bool *isNulls, int32_t *lengths, int64_t executionContext)
{
    auto result = this->func(values, isNulls, lengths, this->isResultNull, this->resultLength, executionContext);
    return !*this->isResultNull && result;
}

Operator *FilterAndProjectOperatorFactory::CreateOperator()
{
    return new FilterAndProjectOperator(this->exprEvaluator, supportVectorized);
}

bool FilterAndProjectOperatorFactory::SupportVectorizedCheck(Expr * filter)
{
    return filter->supportVectorized();
}

OperatorFactory *CreateFilterOperatorFactory(
    const std::shared_ptr<const FilterNode> filterNode, const config::QueryConfig &queryConfig)
{
    auto filterExpr = filterNode->GetFilterExpr();
    std::vector<Expr *> projections;
    const auto &sourceTypes = *(filterNode->Sources()[0]->OutputType());
    int32_t idx = 0;
    if (filterNode->ProjectList().empty()) {
        for (const auto &item : sourceTypes.Get()) {
            projections.push_back(new FieldExpr(idx++, item));
        }
    } else {
        projections = filterNode->ProjectList();
    }
    auto overflowConfig = queryConfig.IsOverFlowASNull() == true ? new OverflowConfig(OVERFLOW_CONFIG_NULL)
                                                                 : new OverflowConfig(OVERFLOW_CONFIG_EXCEPTION);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(filterExpr, projections, sourceTypes, overflowConfig);
    return new FilterAndProjectOperatorFactory(move(exprEvaluator));
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    if (supportVectorized) {
        HandleVectorizedFilter(vecBatch);
        return 0;
    }

    if (vecBatch->GetRowCount() > 0) {
        projectedVecs = this->exprEvaluator->Evaluate(vecBatch, executionContext.get(), &selectedRowsBuffer);
    }
    UpdateAddInputInfo(vecBatch->GetRowCount());
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    return 0;
}


template <typename FlatVector, typename RAW_DATA_TYPE>
void FilterAndProjectOperator::SetFlatVectorValue(int32_t rowCount, BaseVector *baseVector, BaseVector *selectedBaseVector, const uint8_t *bitMark)
{
    int32_t index = 0;
    int32_t j = 0;
    uint8_t mask;
    const BitMaskIndex bitMaskIndex;
    const int32_t batchStep = 8;
    for (; j + batchStep <= rowCount; j += batchStep) {
        mask = bitMark[j >> 3];
        if (mask == 0) {
            continue;
        }
        const uint8_t *maskArr = bitMaskIndex[mask];
        for (int i = 1; i <= *maskArr; i++) {
            auto offset = j + maskArr[i];
            if (UNLIKELY(baseVector->IsNull(offset))) {
                static_cast<FlatVector *>(selectedBaseVector)->SetNull(index++);
            } else {
                auto value = static_cast<FlatVector *>(baseVector)->GetValue(offset);
                static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
            }
        }
    }
    for (; j < rowCount; j++) {
        if (BitUtil::IsBitSet(bitMark, j)) {
            if (UNLIKELY(baseVector->IsNull(j))) {
                static_cast<FlatVector *>(selectedBaseVector)->SetNull(index++);
            } else {
                auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
            }
        }
    }
}

void FilterAndProjectOperator::SetStringVectorValue(int32_t rowCount, Vector<LargeStringContainer<std::string_view>> *baseVector,
        Vector<LargeStringContainer<std::string_view>> *selectedBaseVector, const uint8_t *bitMark)
{
    int32_t index = 0;
    int32_t j = 0;
    uint8_t mask;

    const BitMaskIndex bitMaskIndex;
    const int32_t batchStep = 8;

    for (; j + batchStep <= rowCount; j += batchStep) {
        mask = bitMark[j >> 3];
        if (mask == 0) {
            continue;
        }
        const uint8_t *maskArr = bitMaskIndex[mask];
        for (int i = 1; i <= *maskArr; i++) {
            auto offset = j + maskArr[i];
            if (UNLIKELY(baseVector->IsNull(offset))) {
                selectedBaseVector->SetNull(index++);
            } else {
                auto value = baseVector->GetValue(offset);
                selectedBaseVector->SetValue(index++, value);
            }
        }
    }
    for (; j < rowCount; j++) {
        if (BitUtil::IsBitSet(bitMark, j)) {
            if (UNLIKELY(baseVector->IsNull(j))) {
                selectedBaseVector->SetNull(index++);
            } else {
                auto value = baseVector->GetValue(j);
                selectedBaseVector->SetValue(index++, value);
            }
        }
    }
}

void FilterAndProjectOperator::SetMapVectorValue(int32_t rowCount, MapVector *baseVector, MapVector *selectedBaseVector, const uint8_t *bitMark)
{
    auto resultSize = selectedBaseVector->vec::BaseVector::GetSize();

    std::vector<int> keyPositions;
    int keyLength = 0;
    int32_t j = 0;
    int32_t index = 0;
    for (; j < rowCount; j++) {
        if (BitUtil::IsBitSet(bitMark, j)) {
            if (UNLIKELY(baseVector->IsNull(j))) {
                selectedBaseVector->SetNull(index);
            }
            int keyIndex = baseVector->GetOffset(j);
            int keySize = baseVector->GetSize(j);
            selectedBaseVector->SetOffset(index, keyLength);
            keyLength += keySize;
            index++;
            MapVector::updateKeyPositions(keyPositions, keyIndex, keySize);
        }
    }
    selectedBaseVector->SetOffset(resultSize, keyLength);

    // FIXME use constant vector instead of create empty base vector in future
    auto keyVector = baseVector->GetKeyVector();
    if (UNLIKELY(keyLength == 0)) {
        selectedBaseVector->AddKeys(new BaseVector(0, keyVector->GetEncoding(), keyVector->GetTypeId()));
    } else {
        auto newKeyVector = keyVector->CopyPositions(keyPositions.data(), 0, keyLength);
        selectedBaseVector->AddKeys(newKeyVector);
    }

    auto valueVector = baseVector->GetValueVector();
    if (UNLIKELY(keyLength == 0)) {
        selectedBaseVector->AddValues(new BaseVector(0, valueVector->GetEncoding(), valueVector->GetTypeId()));
    } else {
        auto newValueVector = valueVector->CopyPositions(keyPositions.data(), 0, keyLength);
        selectedBaseVector->AddValues(newValueVector);
    }
}

BaseVector *FilterAndProjectOperator::CopyPositionsFromBitMark(DataTypeId dataType, int rowCount, BaseVector *baseVector, const uint8_t *bitMark, int32_t length) {
    switch (dataType) {
        case OMNI_INT:
        case OMNI_DATE32: {
            auto selectedBaseVector = new Vector<int32_t>(length);
            SetFlatVectorValue<Vector<int32_t>, int32_t>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_SHORT: {
            auto selectedBaseVector = new Vector<int16_t>(length);
            SetFlatVectorValue<Vector<int16_t>, int16_t>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: {
            auto selectedBaseVector = new Vector<int64_t>(length);
            SetFlatVectorValue<Vector<int64_t>, int64_t>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_DOUBLE: {
            auto selectedBaseVector = new Vector<double>(length);
            SetFlatVectorValue<Vector<double>, double>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_BOOLEAN: {
            auto selectedBaseVector = new Vector<bool>(length);
            SetFlatVectorValue<Vector<bool>, bool>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_DECIMAL128: {
            auto selectedBaseVector = new Vector<Decimal128>(length);
            SetFlatVectorValue<Vector<Decimal128>, Decimal128>(rowCount, baseVector, selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            auto selectedBaseVector = new Vector<LargeStringContainer<std::string_view>>(length);
            SetStringVectorValue(rowCount, dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector),
                                 dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(selectedBaseVector), bitMark);
            return selectedBaseVector;
        }
        case OMNI_MAP: {
            auto selectedBaseVector = new MapVector(length);
            SetMapVectorValue(rowCount, dynamic_cast<MapVector*>(baseVector), selectedBaseVector, bitMark);
            return selectedBaseVector;
        }
        case OMNI_ROW: {
            RowVector* selectedBaseVector = new RowVector(static_cast<int32_t>(length));

            std::vector<int> keyPositions;
            int32_t j = 0;
            int32_t index = 0;
            for (; j < rowCount; j++) {
                if (BitUtil::IsBitSet(bitMark, j)) {
                    if (UNLIKELY(baseVector->IsNull(j))) {
                        selectedBaseVector->SetNull(index);
                    }
                    keyPositions.push_back(j);
                }
            }

            auto originalRowVector = reinterpret_cast<RowVector*>(baseVector);
            for (int i = 0; i < originalRowVector->ChildSize(); i++) {
                auto childVector = originalRowVector->ChildAt(i);
                selectedBaseVector->Append(childVector->CopyPositions(keyPositions.data(), 0, length));
            }
            return selectedBaseVector;
        }
        default: {
            LogError("No such %d type support", dataType);
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                                                        "unsupported selectivity type: " + std::to_string(static_cast<int>(dataType)));
        }
    }
}

omniruntime::vec::VectorBatch* FilterAndProjectOperator::GetVecBatchFromBitMark(omniruntime::vec::VectorBatch &vecBatch,
            uint8_t *bitMark, int32_t originalRowCount) {
    int32_t resultSize = BitUtil::CountBits(reinterpret_cast<const uint64_t *>(bitMark), 0, originalRowCount);

    auto result = new VectorBatch(resultSize);

    for (int32_t i = 0; i < vecBatch.GetVectorCount(); i++) {
        auto baseVector = vecBatch.Get(i);
        auto dataType = baseVector->GetTypeId();
        // new vector
        auto selectedBaseVector = CopyPositionsFromBitMark(dataType, originalRowCount, baseVector, bitMark, resultSize);
        result->Append(selectedBaseVector);
    }
    return result;
}

void FilterAndProjectOperator::HandleVectorizedFilter(omniruntime::vec::VectorBatch *vecBatch)
{
    // update projectedVecs
    UpdateAddInputInfo(vecBatch->GetRowCount());

    // init
    auto size = vecBatch->GetRowCount();
    auto bitMarkBuf = std::make_unique<omniruntime::mem::AlignedBuffer<uint8_t>>(BitUtil::Nbytes(size) + 8, true);

    uint8_t *bitMark = filterExpr->compute(vecBatch, bitMarkBuf->GetBuffer());
    auto selectedRows = omniruntime::BitUtil::CountBits(reinterpret_cast<const uint64_t *>(bitMark), 0, size);


    if (selectedRows == 0) {
        // set projectedVecs nulllptr if no data selected
        projectedVecs = nullptr;
        VectorHelper::FreeVecBatch(vecBatch);
    } else if (selectedRows == size) {
        // select all data
        projectedVecs = vecBatch;
    } else {
        projectedVecs = GetVecBatchFromBitMark(*vecBatch, bitMark, size);
        VectorHelper::FreeVecBatch(vecBatch);
    }
}

int32_t FilterAndProjectOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->projectedVecs == nullptr) {
        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return 0;
    }
    int rowCount = this->projectedVecs->GetRowCount();
    *outputVecBatch = this->projectedVecs;
    this->projectedVecs = nullptr;
    UpdateGetOutputInfo(rowCount);
    return rowCount;
}

OmniStatus FilterAndProjectOperator::Close()
{
    if (projectedVecs != nullptr) {
        VectorHelper::FreeVecBatch(projectedVecs);
        projectedVecs = nullptr;
    }
    UpdateCloseInfo();
    return OMNI_STATUS_NORMAL;
}

/**
 * Process one row for fusion operator
 * @param valueAddrs contains value address of each column.
 * @param inputLens contains null or length of each column. inputLens[i] == -1 means i-th value is null; inputLens[i] >=
 * 0 represents the i-th values's length.
 * @param outValueAddrs contains output value address of each projection.
 * @param outLens contains null or length of each projection.
 * @return true(filter pass) or false(filter fail).
 */
bool FilterAndProjectOperator::ProcessRow(
    int64_t valueAddrs[], const int32_t inputLens[], int64_t outValueAddrs[], int32_t outLens[])
{
    auto vecCount = exprEvaluator->GetInputDataTypes().GetSize();
    auto dictsAddrs = new int64_t[vecCount];
    auto offsetsAddrs = new int64_t[vecCount];
    auto nullsAddrs = new int64_t[vecCount];
    for (int i = 0; i < vecCount; ++i) {
        dictsAddrs[i] = 0; // Spark's TableScan will not produce dictionary.
        auto null = new uint8_t[NullsBuffer::CalculateNbytes(1)];
        nullsAddrs[i] = reinterpret_cast<int64_t>(null);
        auto offset = new int32_t[2]; // offset[1] - offset[0] = length
        offsetsAddrs[i] = reinterpret_cast<int64_t>(offset);
    }

    // Construct nullsAddrs and offsetsAddrs from inputLens
    for (int i = 0; i < vecCount; ++i) {
        if (inputLens[i] == -1) {
            BitUtil::SetBit(reinterpret_cast<uint8_t *>(nullsAddrs[i]), 0, true);
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[0] = 0;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[1] = 0;
        } else {
            BitUtil::SetBit(reinterpret_cast<uint8_t *>(nullsAddrs[i]), 0, false);
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[0] = 0;
            reinterpret_cast<int32_t *>(offsetsAddrs[i])[1] = inputLens[i];
        }
    }

    const int rowCount = 1;
    int32_t selectedRows[rowCount];
    int32_t numSelectedRows = exprEvaluator->GetFilterFunc()(valueAddrs, rowCount, selectedRows, nullsAddrs,
        offsetsAddrs, reinterpret_cast<int64_t>(executionContext.get()), dictsAddrs);

    if (executionContext->HasError()) {
        executionContext->GetArena()->Reset();
        for (int i = 0; i < vecCount; ++i) {
            delete[] reinterpret_cast<uint8_t *>(nullsAddrs[i]);
            delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
        }
        delete[] dictsAddrs;
        delete[] nullsAddrs;
        delete[] offsetsAddrs;
        string errorMessage = executionContext->GetError();
        throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
    }

    if (numSelectedRows <= 0) {
        executionContext->GetArena()->Reset();
        for (int i = 0; i < vecCount; ++i) {
            delete[] reinterpret_cast<uint8_t *>(nullsAddrs[i]);
            delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
        }
        delete[] dictsAddrs;
        delete[] nullsAddrs;
        delete[] offsetsAddrs;
        return false;
    }

    for (int32_t i = 0; i < exprEvaluator->GetProjectVecCount(); i++) {
        auto &projections = exprEvaluator->GetProjections();
        if (projections[i]->IsColumnProjection()) {
            outValueAddrs[i] = valueAddrs[projections[i]->GetColumnProjectionIndex()];
            outLens[i] = inputLens[projections[i]->GetColumnProjectionIndex()];
        } else {
            executionContext->GetArena()->Reset();
            for (int j = 0; j < vecCount; ++j) {
                delete[] reinterpret_cast<uint8_t *>(nullsAddrs[j]);
                delete[] reinterpret_cast<int32_t *>(offsetsAddrs[j]);
            }
            delete[] dictsAddrs;
            delete[] nullsAddrs;
            delete[] offsetsAddrs;
            string errorMessage = "Fusion filter only supports raw column projection!";
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
        }
    }

    executionContext->GetArena()->Reset();
    for (int i = 0; i < vecCount; ++i) {
        delete[] reinterpret_cast<uint8_t *>(nullsAddrs[i]);
        delete[] reinterpret_cast<int32_t *>(offsetsAddrs[i]);
    }
    delete[] dictsAddrs;
    delete[] nullsAddrs;
    delete[] offsetsAddrs;
    return true;
}
} // namespace op
} // namespace omniruntime
