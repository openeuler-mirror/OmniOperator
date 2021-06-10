#ifndef __FILTER_COMPILER_H__
#define __FILTER_COMPILER_H__

#include "../../common/expressions.h"
#include "filter.h"
#include <cstring>
#include <stdint.h>

namespace omniruntime {
namespace op {

class Compiler
{
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
    Compiler(Expr* expression, int32_t *inputTypes, int32_t vecCount);
    ~Compiler() {}
    Filter *compile();

private:
    Expr* expression;
    int32_t *inputTypes;
    int32_t vecCount;
};
} // end of op
} // end of omniruntime
#endif