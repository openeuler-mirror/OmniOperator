#include "projection.h"

using namespace omniruntime::op;

Projection::Projection(int32_t* inputTypes, int32_t nCols, std::string expr, bool filter) :
inputTypes(inputTypes), nCols(nCols) {
    Parser parser;
    this->expr = parser.parseRowExpression(expr, inputTypes, nCols);
    vector<DataType>* datatypes = new vector<DataType>();
    for (int32_t i = 0; i < nCols; i++) datatypes->push_back(expressions::colTypeTrans(inputTypes[i]));
    this->codegen = new ProjectionCodeGen("proj_func", this->expr, datatypes, filter);
    this->projector = (int32_t (*)(int64_t*, int32_t, int64_t, int32_t*, int32_t, bool*)) (intptr_t) this->codegen->getFunction();
}

Projection::Projection(int32_t* inputTypes, int32_t nCols, Expr* expr, bool filter) :
inputTypes(inputTypes), nCols(nCols), expr(expr) {
    vector<DataType>* datatypes = new vector<DataType>();
    for (int32_t i = 0; i < nCols; i++) datatypes->push_back(expressions::colTypeTrans(inputTypes[i]));
    this->codegen = new ProjectionCodeGen("proj_func", this->expr, datatypes, filter);
    this->projector = (int32_t (*)(int64_t*, int32_t, int64_t, int32_t*, int32_t, bool*)) (intptr_t) this->codegen->getFunction();
}


// Helper function to return an array of data
// Modifies bitmap array, also adds to vcdataVec and stringvalVec so that the values can be freed
int64_t* Projection::getData(VectorBatch* &vecBatch, vector<int64_t *> &vcdataVec, vector<char *> &stringvalVec, bool* bitmap) 
{
    uint32_t nCols = vecBatch->getVectorCount();
    uint32_t nRows = vecBatch->getRowCount();
    int64_t* data = new int64_t[nCols];


    for (int32_t i = 0; i < nCols; i++) {
        // varchar vec getValues is different from the rest
        if (vecBatch->getVector(i)->getType() == OMNI_VEC_TYPE_VARCHAR) {
            VarcharVector* vcVec = (VarcharVector*)(vecBatch->getVector(i));
            // Create array to hold addresses
            int64_t* vcdata = new int64_t[nRows];

            for (int32_t j = 0; j < nRows; j++) {
                // get data
                char* actualChar = nullptr;
                int len = vcVec->getValue(j, &actualChar);
                // add to vector so it can be freed later
                stringvalVec.push_back(actualChar);

                vcdata[j] = (int64_t)(actualChar);

                // deal with bitmap
                // bitmap[j * nCols + i] represents nullity of jth value of vector i
                bitmap[j * nCols + i] = vcVec->isValueNull(j);
            }
            vcdataVec.push_back(vcdata);

            data[i] = (int64_t)(vcdata);
        }

        else {
            data[i] = (int64_t) vecBatch->getVector(i)->getValues();
            for (int32_t j = 0; j < nRows; j++) {
                // whether the jth value of vector i is null is captured in bitmap[j * nCols + i]
                bitmap[j * nCols + i] = vecBatch->getVector(i)->isValueNull(j);
            }
        }
    }

    return data;
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
        case STRINGD:
            // TODO: set capacity appropriately
            // capacity = numSelectedRows * 50 cannot handle vectors with average string length over 50
            outVec = new VarcharVector(va, numSelectedRows * 50, numSelectedRows);
            break;
    }
    // expr->printExprTree();
    // std::cout << std::endl;
    
    // Contains arrays with addresses for varchar vecs
    vector<int64_t *> vcdataVec;
    // Contains all strings created in VarcharVector::getValue method which need to be freed
    vector<char *> stringvalVec;

    bool* bitmap = new bool[vecBatch->getRowCount() * vecBatch->getVectorCount()];

    // contents of bitmap are modified in getData method
    int64_t* data = getData(vecBatch, vcdataVec, stringvalVec, bitmap);

    int32_t nReturned = this->projector(data, vecBatch->getRowCount(), (int64_t) outVec->getValues(), selectedRows, numSelectedRows, bitmap);


    for (auto v : vcdataVec) {
        delete[] v;
    }
    for (auto v : stringvalVec) {
        delete[] v;
    }

    delete[] bitmap;
    delete[] data;
    delete va;


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

ProjectionOperatorFactory::~ProjectionOperatorFactory() {
    for (int32_t i = 0; i < nProj; i++) {
        delete this->proj[i];
    }
    delete[] this->proj;
}

omniruntime::op::Operator* ProjectionOperatorFactory::createOperator() {
    return new ProjectionOperator(this->proj, this->inputTypes, this->nCols, this->nProj);
}