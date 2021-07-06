#include "filter_and_project.h"
#include "filter_compiler.h"

namespace omniruntime {
namespace op {
using namespace std;

FilterAndProjectOperatorFactory::FilterAndProjectOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;
    this->setJitContext(nullptr);

    Parser parserObject;
    // std::cout << "parsing: " << expression << std::endl;
    Expr* parsedExpr = parserObject.parseRowExpression(expression, inputTypes, vecCount);
    // parsedExpr->printExprTree();
    // std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Compiler *compiler = new Compiler(parsedExpr, inputTypes, vecCount);
    this->filter = compiler->compile();
    delete compiler;
    this->projections = new Projection*[this->projectVecCount];
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        DataExpr* exp = new DataExpr();
        exp->isColumn = true;
        exp->colVal = this->projectIndex[i];
        exp->dataType = Parser::colTypeTrans(inputTypes[projectIndex[i]]);
        projections[i] = new Projection(inputTypes, vecCount, exp, true);
    }
}

FilterAndProjectOperatorFactory::~FilterAndProjectOperatorFactory()
{
    delete this->filter;
}

Operator * FilterAndProjectOperatorFactory::createOperator()
{
    return new FilterAndProjectOperator(this->filter, this->inputTypes, this->vecCount, this->projections, this->projectVecCount);
}

int32_t FilterAndProjectOperator::addInput(VectorBatch* vecBatch)
{
    int32_t* selectedRows = new int32_t[vecBatch->getRowCount()];
    int32_t numSelectedRows = this->filter->filter(vecBatch, selectedRows);
    VectorBatch* projectedData = new VectorBatch(this->projectVecCount);
    for (int32_t i = 0; i < this->projectVecCount; i++) {
        Vector* col = this->projections[i]->project(vecBatch, selectedRows, numSelectedRows);
        projectedData->setVector(i, col);
    }
    this->projectedVecs = projectedData;
    delete[] selectedRows;
    return numSelectedRows;
}

int32_t FilterAndProjectOperator::getOutput(std::vector<VectorBatch*>& data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    data.push_back(this->projectedVecs);
    // this->projectedVecs = nullptr;
    // TODO: cleanup memory in old vecBatches
    return projectedVecs->getRowCount();
}

Filter::Filter(FilterCodeGen* codeGen, Expr* expr)
{
    this->codeGen = codeGen;
    this->expr = expr;
    this->func = (int32_t (*)(int64_t*, int32_t, int32_t*)) (intptr_t) codeGen->getFunction();
}

int32_t Filter::filter(VectorBatch *vecBatch, int32_t *selectedRows)
{
    uint32_t nCols = vecBatch->getVectorCount();
    int64_t* data = new int64_t[nCols];
    for (int32_t i = 0; i < nCols; i++) {
        data[i] = (int64_t) vecBatch->getVector(i)->getValues();
    }

    return this->func(data, vecBatch->getRowCount(), selectedRows);
}
} // end of op
} // end of omniruntime
