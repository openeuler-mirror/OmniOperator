/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"

using namespace std;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;

using uint8vec = std::vector<uint8_t>;

Projection::Projection(int32_t inputTypes[], int32_t nCols, std::string expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols)
{
    Parser parser;
    this->expr = parser.ParseRowExpression(expr, inputTypes, nCols);
    std::vector<DataType> datatypes;
    for (int32_t i = 0; i < nCols; i++) {
        datatypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), datatypes, filter);

    auto f = this->codegen->GetFunction();
    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
}

Projection::Projection(int32_t inputTypes[], int32_t nCols, Expr &expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols), expr(&expr)
{
    std::vector<DataType> datatypes;
    for (int32_t i = 0; i < nCols; i++) {
        datatypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", *(this->expr), datatypes, filter);

    auto f = this->codegen->GetFunction();
    void *function = &f;
    auto cfunction = static_cast<ProjFunc *>(function);
    this->projector = *cfunction;
}

unique_ptr<vector<uint8_t>> GetProjDataHelper(uint8_t actualChar[], int32_t len)
{
    auto accStr = make_unique<uint8vec>(len + 1);
    for (int32_t k = 0; k < len; k++) {
        (*accStr)[k] = actualChar[k];
    }
    (*accStr)[len] = '\0';
    return move(accStr);
}

// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
std::vector<int64_t> GetProjData(omniruntime::vec::VectorBatch &vecBatch,
                                 std::vector<unique_ptr<std::vector<int64_t>>> &vcdataVec,
                                 vector<unique_ptr<vector<uint8_t>>> &stringvalVec, int64_t bitmap[])
{
    uint32_t nCols = vecBatch.GetVectorCount();
    uint32_t nRows = vecBatch.GetRowCount();
    std::vector<int64_t> data;

    for (int32_t i = 0; i < nCols; i++) {
        // varchar vec GetValues is different from the rest
        if (vecBatch.GetVector(i)->GetType().GetId() == omniruntime::vec::OMNI_VEC_TYPE_VARCHAR) {
            auto *vcVec = static_cast<omniruntime::vec::VarcharVector *>(vecBatch.GetVector(i));
            // Create array to hold addresses
            unique_ptr<vec64> vcData = make_unique<vec64>();

            for (int32_t j = 0; j < nRows; j++) {
                // get data
                uint8_t *actualChar = nullptr;
                int32_t len = vcVec->GetValue(j, &actualChar);

                /// Truncate the resulting string
                unique_ptr<uint8vec> accStr = GetProjDataHelper(actualChar, len);

                actualChar = accStr->data();

                // add to vector so it can be freed later
                stringvalVec.push_back(move(accStr));

                // add to subarray of data
                auto ac = actualChar;
                void *accChar = &ac;
                auto caccChar = static_cast<int64_t *>(accChar);
                vcData->push_back(*caccChar);
            }
            // data handling
            auto dc = vcData->data();
            void *dataCol = &dc;
            auto cdataCol = static_cast<int64_t *>(dataCol);
            data.push_back(*cdataCol);

            vcdataVec.push_back(move(vcData));
        } else {
            // data handling
            auto dc = vecBatch.GetVector(i)->GetValues();
            void *dataCol = &dc;
            auto cdataCol = static_cast<int64_t *>(dataCol);
            data.push_back(*cdataCol);
        }
        // bitmap handling
        auto bc = vecBatch.GetVector(i)->GetValueNulls();
        void *bitmapCol = &bc;
        auto cbitmapCol = static_cast<int64_t *>(bitmapCol);
        bitmap[i] = *cbitmapCol;
    }

    return data;
}

omniruntime::vec::Vector *Projection::Project(omniruntime::vec::VectorBatch *vecBatch, int32_t selectedRows[],
    int32_t numSelectedRows) const
{
    if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount() && expr->GetType() == ExprType::DATA_E) {
        auto *dEx = static_cast<DataExpr *>(expr);
        if (dEx->isColumn) {
            return vecBatch->GetVector(dEx->colVal)->Slice(0, numSelectedRows);
        }
    }
    DataType outType = expr->GetExprDataType();
    omniruntime::vec::VectorAllocatorManager vam = omniruntime::vec::VectorAllocatorManager::GetInstance();
    omniruntime::vec::VectorAllocator *va = vam.GetOrCreateAllocator("projection_codegen");
    std::unique_ptr<omniruntime::vec::Vector> outVec;
    switch (outType) {
        case INT32D:
            outVec = std::make_unique<omniruntime::vec::IntVector>(va, numSelectedRows);
            break;
        case INT64D:
            outVec = std::make_unique<omniruntime::vec::LongVector>(va, numSelectedRows);
            break;
        case DOUBLED:
            outVec = std::make_unique<omniruntime::vec::DoubleVector>(va, numSelectedRows);
            break;
        case STRINGD:
            // Must set capacity appropriately (to do)
            // capacity = numSelectedRows * 50 cannot handle vectors with average string length over 50
            outVec = std::make_unique<omniruntime::vec::VarcharVector>(va, numSelectedRows * 50, numSelectedRows);
            break;
        default: {
            DebugError("No such data type %d", outType);
            break;
        }
    }
    if (outType == STRINGD) {
        return ProjectHelperVarWidth(*vecBatch, outVec.release(), numSelectedRows, selectedRows, *va);
    } else {
        return ProjectHelperFixedWidth(*vecBatch, outVec.release(), numSelectedRows, selectedRows, *va);
    }
}

omniruntime::vec::Vector *Projection::ProjectHelperVarWidth(omniruntime::vec::VectorBatch &vecBatch,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows,
    int32_t selectedRows[], omniruntime::vec::VectorAllocator &va) const
{
    // Contains arrays with addresses for varchar vecs
    std::vector<unique_ptr<std::vector<int64_t>>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<unique_ptr<vector<uint8_t>>> stringvalVec;

    vector<int64_t> bitmap(vecBatch.GetVectorCount());

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> data = GetProjData(vecBatch, vcdataVec, stringvalVec, bitmap.data());

    // using projector
    vector<int64_t> oVec(numSelectedRows);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    this->projector(data.data(), vecBatch.GetRowCount(),
        *cvecVals,
        selectedRows, numSelectedRows, bitmap.data());

    VarcharVector *outVarcharVec = static_cast<VarcharVector *>(outVec);
    for (int i = 0; i < numSelectedRows; i++) {
        auto charArr = reinterpret_cast<uint8_t *>(ov[i]);

        int j = 0;
        while (charArr[j] != '\0') {
            j++;
        }
        outVarcharVec->SetValue(i, charArr, j);
    }

    data.clear();
    delete &va;

    return outVec;
}

omniruntime::vec::Vector *Projection::ProjectHelperFixedWidth(omniruntime::vec::VectorBatch &vecBatch,
    omniruntime::vec::Vector *outVec, int32_t numSelectedRows,
    int32_t selectedRows[], omniruntime::vec::VectorAllocator &va) const
{
    // Contains arrays with addresses for varchar vecs
    std::vector<unique_ptr<std::vector<int64_t>>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<unique_ptr<vector<uint8_t>>> stringvalVec;

    vector<int64_t> bitmap(vecBatch.GetVectorCount());

    // contents of bitmap are modified in getProjData method
    std::vector<int64_t> data = GetProjData(vecBatch, vcdataVec, stringvalVec, bitmap.data());

    // using projector
    auto ov = outVec->GetValues();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    int32_t nReturned = this->projector(data.data(), vecBatch.GetRowCount(), *cvecVals,
        selectedRows, numSelectedRows, bitmap.data());

    data.clear();
    delete &va;
    return outVec;
}

omniruntime::vec::Vector *Projection::Project(omniruntime::vec::VectorBatch *vecBatch) const
{
    return this->Project(vecBatch, nullptr, vecBatch->GetRowCount());
}

int32_t ProjectionOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    auto outBatch = std::make_unique<omniruntime::vec::VectorBatch>(nProj);
    for (int32_t i = 0; i < nProj; i++) {
        omniruntime::vec::Vector *outCol = proj[i]->Project(vecBatch);
        outBatch->SetVector(i, outCol);
    }
    this->mutated = outBatch.release();
    return vecBatch->GetRowCount();
}

int32_t ProjectionOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data)
{
    if (this->mutated == nullptr) {
        std::cerr << "Error: No projected table ready for output" << std::endl;
        return -1;
    }
    data.push_back(this->mutated);
    FreeStrings();
    return this->mutated->GetRowCount();
}

ProjectionOperatorFactory::ProjectionOperatorFactory(std::string expressions[], int32_t nProj,
    int32_t inputTypes[], int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, expressions[i], false));
    }
}



ProjectionOperatorFactory::ProjectionOperatorFactory(Expr* exprs[], int32_t nProj, int32_t inputTypes[], int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, *(exprs[i]), false));
    }
}

ProjectionOperatorFactory::~ProjectionOperatorFactory()
{
    for (auto &projection : this->proj) {
        projection.reset();
    }
    this->proj.clear();
}

omniruntime::op::Operator *ProjectionOperatorFactory::CreateOperator()
{
    auto projectionOperator =
        std::make_unique<ProjectionOperator>(this->proj, this->inputTypes, this->nCols, this->nProj);
    return projectionOperator.release();
}