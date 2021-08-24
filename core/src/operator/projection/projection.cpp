/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;


Projection::Projection(int32_t *inputTypes, int32_t nCols, std::string expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols)
{
    Parser parser;
    this->expr = parser.ParseRowExpression(expr, inputTypes, nCols);
    std::vector<DataType> datatypes;
    for (int32_t i = 0; i < nCols; i++) {
        datatypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", this->expr, datatypes, filter);
    this->projector = reinterpret_cast<int32_t (*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, bool *)>(
        this->codegen->GetFunction());
}

Projection::Projection(int32_t *inputTypes, int32_t nCols, Expr *expr, bool filter)
    : inputTypes(inputTypes), nCols(nCols), expr(expr)
{
    std::vector<DataType> datatypes;
    for (int32_t i = 0; i < nCols; i++) {
        datatypes.push_back(expressions::ColTypeTrans(inputTypes[i]));
    }
    this->codegen = std::make_unique<ProjectionCodeGen>("proj_func", this->expr, datatypes, filter);
    this->projector = reinterpret_cast<int32_t (*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, bool *)>(
        this->codegen->GetFunction());
}

// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
std::vector<int64_t> Projection::GetData(omniruntime::vec::VectorBatch *&vecBatch, std::vector<std::vector<int64_t>> &vcdataVec,
    std::vector<uint8_t *> &stringvalVec, bool *bitmap) const
{
    uint32_t nCols = vecBatch->GetVectorCount();
    uint32_t nRows = vecBatch->GetRowCount();
    std::vector<int64_t> data;


    for (int32_t i = 0; i < nCols; i++) {
        // varchar vec GetValues is different from the rest
        if (vecBatch->GetVector(i)->GetType().GetId() == omniruntime::vec::OMNI_VEC_TYPE_VARCHAR) {
            auto *vcVec = reinterpret_cast<omniruntime::vec::VarcharVector *>(vecBatch->GetVector(i));
            // Create array to hold addresses
            std::vector<int64_t> vcdata;

            for (int32_t j = 0; j < nRows; j++) {
                // get data
                uint8_t *actualChar = nullptr;
                int len = vcVec->GetValue(j, &actualChar);
                // add to vector so it can be freed later
                stringvalVec.push_back(actualChar);

                vcdata.push_back(reinterpret_cast<int64_t>(actualChar));

                // deal with bitmap
                // bitmap[j * nCols + i] represents nullity of jth value of vector i
                bitmap[j * nCols + i] = vcVec->IsValueNull(j);
            }
            vcdataVec.push_back(vcdata);

            data.push_back(reinterpret_cast<int64_t>(vcdata.data()));
        } else {
            data.push_back(reinterpret_cast<int64_t>(vecBatch->GetVector(i)->GetValues()));
            for (int32_t j = 0; j < nRows; j++) {
                // whether the jth value of vector i is null is captured in bitmap[j * nCols + i]
                bitmap[j * nCols + i] = vecBatch->GetVector(i)->IsValueNull(j);
            }
        }
    }

    return data;
}

omniruntime::vec::Vector *Projection::Project(omniruntime::vec::VectorBatch *vecBatch, int32_t *selectedRows,
    const int32_t numSelectedRows) const
{
    if (numSelectedRows != 0 && numSelectedRows == vecBatch->GetRowCount() && expr->GetType() == ExprType::DATA_E) {
        auto *dEx = reinterpret_cast<DataExpr *>(expr);
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
            // TODO: set capacity appropriately
            // capacity = numSelectedRows * 50 cannot handle vectors with average string length over 50
            outVec = std::make_unique<omniruntime::vec::VarcharVector>(va, numSelectedRows * 50, numSelectedRows);
            break;
        default: {
            DebugError("No such data type %d", outType);
            break;
        }
    }

    // Contains arrays with addresses for varchar vecs
    std::vector<std::vector<int64_t>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    std::vector<uint8_t *> stringvalVec;

    const int totalRowCount = vecBatch->GetRowCount() * vecBatch->GetVectorCount();
    bool bitmap[totalRowCount];

    // contents of bitmap are modified in GetData method
    std::vector<int64_t> data = GetData(vecBatch, vcdataVec, stringvalVec, bitmap);

    this->projector(data.data(), vecBatch->GetRowCount(), reinterpret_cast<int64_t>(outVec->GetValues()),
        selectedRows, numSelectedRows, bitmap);

    for (auto v : vcdataVec) {
        v.clear();
    }

    data.clear();
    delete va;

    return outVec.release();
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
    return this->mutated->GetRowCount();
}

ProjectionOperatorFactory::ProjectionOperatorFactory(std::string const * expressions, int32_t nProj,
    int32_t *inputTypes, int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, expressions[i], false));
    }
}

ProjectionOperatorFactory::ProjectionOperatorFactory(Expr **exprs, int32_t nProj, int32_t *inputTypes, int32_t nCols)
    : inputTypes(inputTypes), nCols(nCols), nProj(nProj)
{
    this->SetJitContext(nullptr);
    for (int32_t i = 0; i < nProj; i++) {
        this->proj.push_back(std::make_unique<Projection>(inputTypes, nCols, exprs[i], false));
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