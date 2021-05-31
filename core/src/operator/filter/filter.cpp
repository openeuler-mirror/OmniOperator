#include "stdio.h"
#include "chrono"
#include <vector>
#include "filter.h"
#include "filter_compiler.h"
#include "../projection/projection.h"
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"

using namespace std;

NativeOmniFilterOperatorFactory::NativeOmniFilterOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;
    this->setJitContext(nullptr);

    Parser parserObject;
    std::cout << "parsing: " << expression << std::endl;
    Expr* parsedExpr = parserObject.parseRowExpression(expression);
    ComparisionExpr *c_expr =  (ComparisionExpr *) parsedExpr;
    std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Compiler *compiler = new Compiler(parsedExpr, inputTypes, vecCount);
    this->filter = compiler->compile();
    delete compiler;
}

NativeOmniFilterOperatorFactory::~NativeOmniFilterOperatorFactory()
{
    delete this->filter;
}

NativeOmniOperator * NativeOmniFilterOperatorFactory::createOmniOperator()
{
    return new NativeOmniFilterOperator(this->filter, this->inputTypes, this->vecCount, this->projectIndex, this->projectVecCount);
}

int32_t NativeOmniFilterOperator::addInput(Table* data, int32_t rowCount)
{
    int32_t *selectedRows = new int32_t[rowCount];
    int32_t numSelectedRows = this->filter->filter(data, rowCount, selectedRows);
    Projection *projection = new Projection(this->inputTypes, this->vecCount, rowCount, this->projectIndex, this->projectVecCount);
    Table *projectedData = projection->project(selectedRows, numSelectedRows, data);
    this->projectedVecs = projectedData;

    delete[] selectedRows;
    delete projection;
    return numSelectedRows;
}

int32_t NativeOmniFilterOperator::addInput(Table** data, int32_t* rowCount, int32_t pageCount) override
{
    if (pageCount != 1) {
        std::cout << "ERROR: invalid page count " << pageCount << std::end
    }

    int32_t pageRowCount = rowCount[0];

    int32_t *selectedRows = new int32_t[pageRowCount];
    int32_t numSelectedRows = this->filter->filter(data[0], pageRowCount, selectedRows);
    Projection *projection = new Projection(this->inputTypes, this->vecCount, pageRowCount, this->projectIndex, this->projectVecCount);
    Table *projectedData = projection->project(selectedRows, numSelectedRows, data[0]);
    this->projectedVecs = projectedData;

    delete[] selectedRows;
    delete projection;
    return numSelectedRows;
}

int32_t NativeOmniFilterOperator::getOutput(std::vector<Table*>& data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    data.push_back(this->projectedVecs);
    // this->projectedVecs = nullptr;
    // TODO: cleanup memory in old tables
    return projectedVecs->getPositionCount();
}

Filter::Filter(LLVMCodeGen* codeGen, Expr* expr)
{
    this->codeGen = codeGen;
    this->expr = expr;
}

int32_t Filter::filter(Table *table, int32_t rowNumber, int32_t *selectedRows)
{
    int numSelectedRows = 0;
    ComparisionExpr *c_expr =  (ComparisionExpr *) expr;

    Column *column = table->getColumn(c_expr->columnIdx);

    for (int index = 0; index < rowNumber; index++)
    {
        switch (column->getType())
        {
            case INT32:{
                if (codeGen->execute(expr, *((int32_t*) column->getValue(index)))) {
                    selectedRows[numSelectedRows++] = index;
                }
                break;
            }
            case INT64:{
                // TODO: add Support for type
                break;
            }
            case DOUBLE:{
                // TODO: add Support for type
                break;
            }
            default:
                break;
        }
        
    }
    return numSelectedRows;
}