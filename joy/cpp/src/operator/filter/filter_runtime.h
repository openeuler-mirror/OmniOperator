#ifndef __FILTER_RUNTIME_H__
#define __FILTER_RUNTIME_H__

#include "../../common/expressions.h"
#include <string>
#include <iostream>

enum Status
{
    OK,
    ERROR
};

class Filter_runtime{
public:
    int64_t filter_compile_runtime_with_parser(Context* context, Expr expression, int32_t* input_type);
    //void filter_execute(JitFunction filterModule, Table inputData, usize rowNumber, bool* selectedRows);
};

#endif