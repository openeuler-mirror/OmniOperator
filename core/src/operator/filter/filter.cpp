#include "stdio.h"
#include "chrono"
#include <vector>
#include "filter.h"
#include "filter_compiler.h"
#include "../projection/projection.h"
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"
#include "../../codegen/llvm_codegen.h"

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
    std::cout << "parsing: " << expression << std::endl;
    Expr* parsedExpr = parserObject.parseRowExpression(expression, inputTypes, vecCount);
    parsedExpr->printExprTree();
    // std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Compiler *compiler = new Compiler(parsedExpr, inputTypes, vecCount);
    this->filter = compiler->compile();
    delete compiler;
}

FilterAndProjectOperatorFactory::~FilterAndProjectOperatorFactory()
{
    delete this->filter;
}

Operator * FilterAndProjectOperatorFactory::createOperator()
{
    return new FilterAndProjectOperator(this->filter, this->inputTypes, this->vecCount, this->projectIndex, this->projectVecCount);
}

int32_t FilterAndProjectOperator::addInput(VectorBatch* vecBatch)
{
    int32_t *selectedRows = new int32_t[vecBatch->getRowCount()];
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int32_t numSelectedRows = this->filter->filter(vecBatch, selectedRows);
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // std::cout << "TIME TAKEN = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
    Projection *projection = new Projection(this->inputTypes, this->vecCount, vecBatch->getRowCount(), this->projectIndex, this->projectVecCount);
    VectorBatch *projectedData = projection->project(selectedRows, numSelectedRows, vecBatch);
    this->projectedVecs = projectedData;
    delete[] selectedRows;
    delete projection;
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

Filter::Filter(LLVMCodeGen* codeGen, Expr* expr)
{
    this->codeGen = codeGen;
    this->expr = expr;
}

int32_t Filter::filter(VectorBatch *vecBatch, int32_t *selectedRows)
{
    uint32_t nCols = vecBatch->getVectorCount();
    int64_t* data = new int64_t[nCols];
    for (int32_t i = 0; i < nCols; i++) {
        data[i] = (int64_t) vecBatch->getVector(i)->getValues();
    }

    return this->codeGen->execute(data, vecBatch->getRowCount(), selectedRows);
}
} // end of op
} // end of omniruntime
