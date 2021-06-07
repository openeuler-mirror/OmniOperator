#include "stdio.h"
#include "chrono"
#include <vector>
#include "filter.h"
#include "filter_compiler.h"
#include "../projection/projection.h"
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"

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
    Expr* parsedExpr = parserObject.parseRowExpression(expression);
    ComparisionExpr *c_expr =  (ComparisionExpr *) parsedExpr;
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

int32_t FilterAndProjectOperator::addInput(Table* data, int32_t rowCount)
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

int32_t FilterAndProjectOperator::addInput(Table** data, int32_t* rowCount, int32_t pageCount)
{
    if (pageCount != 1) {
        std::cout << "ERROR: invalid page count " << pageCount << std::endl;
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

int32_t FilterAndProjectOperator::getOutput(std::vector<Table*>& data)
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
                Data actualData;
                actualData.intVal = *((int32_t*) column->getValue(index));
                actualData.dataType = DataType::INT32D;
		 c_expr->columnData.dataType = DataType::INT32D;
                if (codeGen->executeComparisionExprFunc(c_expr, &actualData)) {
                    selectedRows[numSelectedRows++] = index;
                }
                break;
            }
            case INT64:{
                Data actualData;
		actualData.longVal = *((int64_t*) column->getValue(index));
		actualData.dataType = DataType::INT64D;
	        c_expr->columnData.dataType = DataType::INT64D;
		if (codeGen->executeComparisionExprFunc(c_expr, &actualData)) {
		    selectedRows[numSelectedRows++] = index;
		}
                break;
            }
            case DOUBLE:{
                Data actualData;
		actualData.doubleVal = *((double*) column->getValue(index));
		actualData.dataType = DataType::DOUBLED;
	        c_expr->columnData.dataType = DataType::DOUBLED;
		if (codeGen->executeComparisionExprFunc(c_expr, &actualData)) {
		    selectedRows[numSelectedRows++] = index;
		}
                break;
            }
            default:
                break;
        }
        
    }
    return numSelectedRows;
}
} // end of op
} // end of omniruntime
