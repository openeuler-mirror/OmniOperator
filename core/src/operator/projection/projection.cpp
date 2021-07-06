#include "projection.h"

using namespace omniruntime::op;

Projection::Projection(int32_t* inputTypes, int32_t nCols, std::string expr, bool filter) :
inputTypes(inputTypes), nCols(nCols) {
    Parser parser;
    this->expr = parser.parseRowExpression(expr, inputTypes, nCols);
    vector<DataType>* datatypes = new vector<DataType>();
    for (int32_t i = 0; i < nCols; i++) datatypes->push_back(expressions::colTypeTrans(inputTypes[i]));
    this->codegen = new ProjectionCodeGen("proj_func", this->expr, datatypes, filter);
    this->projector = (int32_t (*)(int64_t*, int32_t, int64_t, int32_t*, int32_t)) (intptr_t) this->codegen->getFunction();
}

Projection::Projection(int32_t* inputTypes, int32_t nCols, Expr* expr, bool filter) :
inputTypes(inputTypes), nCols(nCols), expr(expr) {
    vector<DataType>* datatypes = new vector<DataType>();
    for (int32_t i = 0; i < nCols; i++) datatypes->push_back(expressions::colTypeTrans(inputTypes[i]));
    this->codegen = new ProjectionCodeGen("proj_func", this->expr, datatypes, filter);
    this->projector = (int32_t (*)(int64_t*, int32_t, int64_t, int32_t*, int32_t)) (intptr_t) this->codegen->getFunction();
}

Vector* Projection::project(VectorBatch* vecBatch, int32_t* selectedRows, int32_t numSelectedRows) {
    if (numSelectedRows != 0 && numSelectedRows == vecBatch->getRowCount() && expr->getType() == ExprType::DATA_E) {
        DataExpr* dEx = (DataExpr*) expr;
        if (dEx->isColumn) {
            return vecBatch->getVector(dEx->colVal);
        }
    }
    DataType outType = expr->getExprDataType();
    VectorAllocatorManager vam = VectorAllocatorManager::getInstance();
    VectorAllocator* va = vam.getOrCreateAllocator("projection_codegen");
    Vector* outVec;
    switch (outType) {
        case INT32D:
            outVec = new IntVector(va, numSelectedRows);
            break;
        case INT64D:
            outVec = new LongVector(va, numSelectedRows);
            break;
        case DOUBLED:
            outVec = new DoubleVector(va, numSelectedRows);
            break;
    }
    // expr->printExprTree();
    // std::cout << std::endl;
    int32_t nCols = vecBatch->getVectorCount();
    int64_t* data = new int64_t[nCols];
    for (int32_t i = 0; i < nCols; i++) {
        data[i] = (int64_t) vecBatch->getVector(i)->getValues();
    }
    int32_t nReturned = this->projector(data, vecBatch->getRowCount(), (int64_t) outVec->getValues(), selectedRows, numSelectedRows);
    delete[] data;
    return outVec;
}

Vector* Projection::project(VectorBatch* vecBatch) {
    return this->project(vecBatch, nullptr, vecBatch->getRowCount());
}

int32_t ProjectionOperator::addInput(VectorBatch* vecBatch) {
    VectorBatch* outBatch = new VectorBatch(nProj);
    for (int32_t i = 0; i < nProj; i++) {
        Vector* outCol = proj[i]->project(vecBatch);
        outBatch->setVector(i, outCol);
    }
    this->mutated = outBatch;
    return vecBatch->getRowCount();
}

int32_t ProjectionOperator::getOutput(std::vector<VectorBatch*>& ret) {
    if (this->mutated == nullptr) {
        std::cerr << "Error: No projected table ready for output" << std::endl;
        return -1;
    }
    ret.push_back(this->mutated);
    return this->mutated->getRowCount();
}

ProjectionOperatorFactory::ProjectionOperatorFactory(std::string* expressions, int32_t nProj, int32_t* inputTypes, int32_t nCols) :
inputTypes(inputTypes), nCols(nCols), nProj(nProj) {
    this->setJitContext(nullptr);
    this->proj = new Projection*[nProj];
    for (int32_t i = 0; i < nProj; i++) this->proj[i] = new Projection(inputTypes, nCols, expressions[i], false);
}

ProjectionOperatorFactory::ProjectionOperatorFactory(Expr** exprs, int32_t nProj, int32_t* inputTypes, int32_t nCols) :
inputTypes(inputTypes), nCols(nCols), nProj(nProj) {
    this->setJitContext(nullptr);
    this->proj = new Projection*[nProj];
    for (int32_t i = 0; i < nProj; i++) this->proj[i] = new Projection(inputTypes, nCols, exprs[i], false);
}

omniruntime::op::Operator* ProjectionOperatorFactory::createOperator() {
    return new ProjectionOperator(this->proj, this->inputTypes, this->nCols, this->nProj);
}