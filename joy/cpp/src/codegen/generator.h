#ifndef __GENERATOR_H__
#define __GENERATOR_H__
#include <iostream>
#include <string>
#include <optional>
#include "loop_block.h"

// TODO: complete direct translate from rust generator
typedef struct InSituFunction{
    // TODO: implement the following missing types
    CodeBlock entry;
    LoopBlock forloop;
    //FunctionType functionTy;
    //FunctionValue function;
} InSituFunction;

typedef struct Generator{
    // TODO: add cpp version of the following
    //Context context;
    //Builder builder;;
    //Moduler moduler;
    //ExcutionEngine executionEngine;
    std::string name;
    std::optional<InSituFunction> function;
} Generator;

class InSituDebug {
    public:
        virtual std::string debug_func() = 0;
};


#endif