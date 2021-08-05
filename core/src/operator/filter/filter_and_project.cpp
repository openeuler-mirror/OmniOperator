/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator source file
 */
#include "filter_and_project.h"
#include "filter_compiler.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace std;

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes,
    int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;
    this->SetJitContext(nullptr);

    Parser parserObject;
    // std::cout << "parsing: " << expression << std::endl;

    Expr *parsedExpr = parserObject.ParseRowExpression(expression, inputTypes, vecCount);
    // std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    // TODO: replace the placeholder context

    unique_ptr<Compiler> compiler = make_unique<Compiler>(parsedExpr, inputTypes, vecCount);
    this->filter = std::move(compiler->Compile());
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        auto exp = make_unique<DataExpr>();
        exp->isColumn = true;
        exp->colVal = this->projectIndex[i];
        exp->dataType = ColTypeTrans(inputTypes[projectIndex[i]]);
        projections.push_back(make_unique<Projection>(inputTypes, vecCount, exp.release(), true));
    }
}

FilterAndProjectOperatorFactory::~FilterAndProjectOperatorFactory()
{
    this->filter.reset();
    for (auto &projection : this->projections) {
        projection.reset();
    }
    this->projections.clear();
}

Operator *FilterAndProjectOperatorFactory::CreateOperator()
{
    auto filterAndProjectOperator = make_unique<FilterAndProjectOperator>(this->filter, this->inputTypes,
        this->vecCount, this->projections, this->projectVecCount);
    return filterAndProjectOperator.release();
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch *vecBatch)
{
    const int rowCount = vecBatch->GetRowCount();
    int32_t selectedRows[rowCount];
    int32_t numSelectedRows = this->filter->DoFilter(vecBatch, selectedRows, rowCount);
    if (numSelectedRows <= 0) {
        return 0;
    }

    auto projectedData = make_unique<VectorBatch>(this->projectVecCount);
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        Vector *col = this->projections[i]->Project(vecBatch, selectedRows, numSelectedRows);
        projectedData->SetVector(i, col);
    }
    this->projectedVecs = std::move(projectedData);
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    int rowCount = this->projectedVecs->GetRowCount();
    data.push_back(this->projectedVecs.release());

    // TODO: cleanup memory in old vecBatches
    return rowCount;
}

Filter::Filter(unique_ptr<FilterCodeGen> codeGen, Expr *expr)
{
    this->codeGen = std::move(codeGen);
    this->expr = expr;
    this->func = reinterpret_cast<int32_t (*)(int64_t *, int32_t, int32_t *, bool *)>(this->codeGen->GetFunction());
}


// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
std::vector<int64_t> Filter::GetData(VectorBatch *&vecBatch, vector<vector<int64_t>> &vcdataVec, vector<char *> &stringvalVec,
    bool *bitmap) const
{
    uint32_t nCols = vecBatch->GetVectorCount();
    uint32_t nRows = vecBatch->GetRowCount();
    std::vector<int64_t> data;

    for (int32_t i = 0; i < nCols; i++) {
        // varchar vec GetValues is different from the rest
        if (vecBatch->GetVector(i)->GetType().GetId() == OMNI_VEC_TYPE_VARCHAR) {
            VarcharVector *vcVec = reinterpret_cast<VarcharVector *>(vecBatch->GetVector(i));
            // Create array to hold addresses
            std::vector<int64_t> vcData;

            for (int32_t j = 0; j < nRows; j++) {
                // get data
                char *actualChar = nullptr;
                int len = vcVec->GetValue(j, &actualChar);
                // add to vector so it can be freed later
                stringvalVec.push_back(actualChar);

                vcData.push_back(reinterpret_cast<int64_t>(actualChar));

                // deal with bitmap
                // bitmap[j * nCols + i] represents nullity of jth value of vector i
                bitmap[j * nCols + i] = vcVec->IsValueNull(j);
            }
            vcdataVec.push_back(vcData);

            data.push_back(reinterpret_cast<int64_t>(vcData.data()));
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

int32_t Filter::DoFilter(VectorBatch *&vecBatch, int32_t *selectedRows, int rowCount) const
{
    // Contains arrays with addresses for varchar vecs
    vector<vector<int64_t>> vcdataVec;
    // Contains all strings created in VarcharVector::GetValue method which need to be freed
    vector<char *> stringvalVec;

    const int totalRowCount = rowCount * vecBatch->GetVectorCount();
    bool bitmap[totalRowCount];

    // contents of bitmap are appropriately modified in GetData
    std::vector<int64_t> data = GetData(vecBatch, vcdataVec, stringvalVec, bitmap);
    int32_t ret = this->func(data.data(), rowCount, selectedRows, bitmap);

    for (auto v : vcdataVec) {
        v.clear();
    }
    for (auto v : stringvalVec) {
        delete[] v;
    }

    data.clear();

    return ret;
}
} // end of op
} // end of omniruntime
