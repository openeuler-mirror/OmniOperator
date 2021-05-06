#include "filter_api.h"
#include "../common/expressions.h"
#include "../operator/filter/filter.h"
#include "../operator/filter/filter_compiler.h"
#include "../parser/parser.h"
#include "../operator/projection.h"
#include <iostream>
#include <thread>
#include <string>

int64_t filterCompile(
    std::string filterExpression,
    int32_t *inputTypes,
    int32_t vecCount)
{
    Parser parserObject;
    Expr parsed = parserObject.parseRowExpression(filterExpression);
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Compiler *compiler = new Compiler(parsed, inputTypes, vecCount);
    Filter *filter = compiler->compile();
    free(inputTypes);
    return (int64_t) filter;
}

int32_t filterExecute(
    int64_t filterPtr,
    int64_t *inputData,
    int32_t *inputTypes,
    int32_t vecCount,
    int32_t rowNumber,
    int64_t *projectVecAddress,
    int32_t *projectIdx,
    int32_t projectVecCount)
{
    int32_t *selectedRows = new int32_t[rowNumber];

    Table* table = new Table(rowNumber, vecCount);
    for (int i = 0; i < vecCount; i++) {
        void* data = (void *)(inputData[i]);
        ColumnType vecType;
        switch (*(inputTypes + i))
        {
            case 1:
                vecType = INT32;
                break;
            case 2:
                vecType = INT64;
                break;
            case 3:
                vecType = DOUBLE;
                break;
            default:
                DebugError("Unsupported column type %d", *(inputTypes + i));
                delete[] selectedRows;
                return -1;
        }
        Column* col = new Column(data, vecType, rowNumber);
        table->setColumn(col, vecType);
    }

    Filter *filter = (Filter*) filterPtr;
    int32_t numSelectedRows = filter->filter(table, rowNumber, selectedRows);

    Projection *projection = new Projection(inputTypes, vecCount, rowNumber, projectIdx, projectVecCount, projectVecAddress);
    projection->project(selectedRows, numSelectedRows, table);

    delete[] selectedRows;
    return numSelectedRows;
}
