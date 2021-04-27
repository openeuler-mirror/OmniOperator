#include "filter_api.h"
#include "../common/expressions.h"
#include "../operator/filter/filter_runtime.h"
#include "../parser/parser.h"
#include <iostream>
#include <thread>
#include <string>


int32_t* createInputVec(int64_t address, int32_t count)
{
    int32_t* inputVec;
    inputVec = (int32_t*) malloc(count*sizeof(int32_t));
    for (int i=0; i<count; i++) {
      *((int32_t*)(inputVec+sizeof(int32_t)*i)) = *((int32_t*)(address+sizeof(int32_t)*i));
    }
    return inputVec;
}

int64_t filterCompile(
    string filterExpression,
    int64_t inputType,
    int32_t vecCount)
{
    int32_t* inputTypes = createInputVec(inputType, vecCount);
    Parser parserObject;
    Expr parsed = parserObject.parseRowExpression(filterExpression);
    // might want to check if parsed suceed?
    //Context context = Context::create();
    Context context;
    Filter_runtime runtime;
    int64_t filter_ptr = runtime.filter_compile_runtime_with_parser(&context, parsed, inputTypes);
    free(inputTypes);
    return filter_ptr;
    // if (compile_status) {
    //     return filter_ptr;
    // } else {
    //     cout << "{" + err + "}" << endl;
    //     return 0;
    // }
}


