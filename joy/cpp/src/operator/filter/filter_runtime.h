#ifndef __FILTER_RUNTIME_H__
#define __FILTER_RUNTIME_H__

#include "../../common/expressions.h"
#include <string>
#include <iostream>

// this is used as a replace of rust match status, might be useful if we want to handle the error at the api level
enum Status
{
    OK,
    ERROR
};

class Filter_runtime{
public:
    int64_t filter_compile_runtime_with_parser(Context* context, Expr expression, int32_t* input_type);
};

#endif