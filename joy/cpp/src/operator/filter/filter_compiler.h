#ifndef __FILTER_COMPILER_H__
#define __FILTER_COMPILER_H__
#include "../../common/expressions.h"
#include <cstring>

class Compiler{
public:
    // Context context;
    // Builder builder;
    // PassManager fpm;
    // Module module;
    // Layout layout;
    // Expr expression;
    // HashMap<string, long> variables;
    // Optional fn_value_opt;
    
    // // todo getter functions

    long compile(Expr expression, Context* context, int32_t* inputTypes, int count);
}

#endif