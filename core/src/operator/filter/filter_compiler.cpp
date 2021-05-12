#include "filter_compiler.h"
#include "../../common/expressions.h"
#include <cstring>

bool testExpressionEvaluater(Table *table, int32_t index)
{
    Column *column = table->getColumn(0);
    switch (column->getType())
    {
    case INT32:
        return *((int32_t*) column->getValue(index)) % 2 == 0;
    case INT64:
        return *((int64_t*) column->getValue(index)) % 2 == 0;
    case DOUBLE:
        return *((double*) column->getValue(index)) > 10;
    default:
        break;
    }
}

Compiler::Compiler(Expr expression, int32_t *inputTypes, int32_t vecCount)
{
    this->expression = expression;
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
}

Filter *Compiler::compile()
{
    // TODO: replace the evaluater with compiled function
    return new Filter(testExpressionEvaluater);
}