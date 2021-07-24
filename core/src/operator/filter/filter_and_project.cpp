#include "filter_and_project.h"
#include "filter_compiler.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace std;

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;
    this->SetJitContext(nullptr);

    Parser parserObject;
    // std::cout << "parsing: " << expression << std::endl;
    Expr* parsedExpr = parserObject.parseRowExpression(expression, inputTypes, vecCount);
    // parsedExpr->printExprTree();
    // std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    unique_ptr<Compiler> compiler = make_unique<Compiler>(parsedExpr, inputTypes, vecCount);
    this->filter = compiler->compile();
    
    this->projections = new Projection*[this->projectVecCount];
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        DataExpr* exp = new DataExpr();
        exp->isColumn = true;
        exp->colVal = this->projectIndex[i];
        exp->dataType = colTypeTrans(inputTypes[projectIndex[i]]);
        projections[i] = new Projection(inputTypes, vecCount, exp, true);
    }
}

FilterAndProjectOperatorFactory::~FilterAndProjectOperatorFactory()
{
    delete this->filter;
}

Operator * FilterAndProjectOperatorFactory::CreateOperator()
{
    return new FilterAndProjectOperator(this->filter, this->inputTypes, this->vecCount, this->projections, this->projectVecCount);
}

int32_t FilterAndProjectOperator::AddInput(VectorBatch* vecBatch)
{
    int32_t* selectedRows = new int32_t[vecBatch->GetRowCount()];
    int32_t numSelectedRows = this->filter->filter(vecBatch, selectedRows);
    VectorBatch* projectedData = new VectorBatch(this->projectVecCount);
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        Vector* col = this->projections[i]->project(vecBatch, selectedRows, numSelectedRows);
        projectedData->SetVector(i, col);
    }
    this->projectedVecs = projectedData;
    delete[] selectedRows;
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::GetOutput(std::vector<VectorBatch*>& data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    data.push_back(this->projectedVecs);
    // this->projectedVecs = nullptr;
    // TODO: cleanup memory in old vecBatches
    return projectedVecs->GetRowCount();
}

Filter::Filter(FilterCodeGen* codeGen, Expr* expr)
{
    this->codeGen = codeGen;
    this->expr = expr;
    this->func = (int32_t (*)(int64_t*, int32_t, int32_t*, bool*)) (intptr_t) codeGen->getFunction();
}


// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
int64_t* Filter::getData(VectorBatch* &vecBatch, vector<int64_t *> &vcdataVec, vector<char *> &stringvalVec, bool* bitmap) 
{
    uint32_t nCols = vecBatch->GetVectorCount();
    uint32_t nRows = vecBatch->GetRowCount();
    int64_t* data = new int64_t[nCols];


    for (int32_t i = 0; i < nCols; i++) {
        // varchar vec getValues is different from the rest
        if (vecBatch->GetVector(i)->GetType() == OMNI_VEC_TYPE_VARCHAR) {
            VarcharVector* vcVec = (VarcharVector*)(vecBatch->GetVector(i));
            // Create array to hold addresses
            int64_t* vcdata = new int64_t[nRows];

            for (int32_t j = 0; j < nRows; j++) {
                // get data
                char* actualChar = nullptr;
                int len = vcVec->GetValue(j, &actualChar);
                // add to vector so it can be freed later
                stringvalVec.push_back(actualChar);

                vcdata[j] = (int64_t)(actualChar);

                // deal with bitmap
                // bitmap[j * nCols + i] represents nullity of jth value of vector i
                bitmap[j * nCols + i] = vcVec->IsValueNull(j);
            }
            vcdataVec.push_back(vcdata);

            data[i] = (int64_t)(vcdata);
        }

        else {
            data[i] = (int64_t) vecBatch->GetVector(i)->GetValues();
            for (int32_t j = 0; j < nRows; j++) {
                // whether the jth value of vector i is null is captured in bitmap[j * nCols + i]
                bitmap[j * nCols + i] = vecBatch->GetVector(i)->IsValueNull(j);
            }
        }
    }

    return data;
}

int32_t Filter::filter(VectorBatch* &vecBatch, int32_t *selectedRows)
{
    // Contains arrays with addresses for varchar vecs
    vector<int64_t *> vcdataVec;
    // Contains all strings created in VarcharVector::getValue method which need to be freed
    vector<char *> stringvalVec;

    bool* bitmap = new bool[vecBatch->GetRowCount() * vecBatch->GetVectorCount()];

    // contents of bitmap are appropriately modified in getData
    int64_t* data = getData(vecBatch, vcdataVec, stringvalVec, bitmap);

    int32_t ret = this->func(data, vecBatch->GetRowCount(), selectedRows, bitmap);


    for (auto v : vcdataVec) {
        delete[] v;
    }
    for (auto v : stringvalVec) {
        delete[] v;
    }

    delete[] data;
    delete[] bitmap;

    return ret;
}

} // end of op
} // end of omniruntime
