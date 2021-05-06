#ifndef __FILTER_H__
#define __FILTER_H__

#include "../../data/table.h"

typedef bool (*jit_evaluateExpression)(Table* table, int32_t);

class Filter
{
public:
    Filter(jit_evaluateExpression evaluater);
    int32_t filter(Table *table, int32_t rowNumber, int32_t *selectedRows);

private:
    jit_evaluateExpression evaluater;
};

#endif