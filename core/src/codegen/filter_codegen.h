#ifndef FILTER_CODEGEN_H
#define FILTER_CODEGEN_H

#include "llvm_codegen.h"

class FilterCodeGen : public LLVMCodeGen {
public:
    FilterCodeGen(string name, Expr* expr, vector<DataType>* datatypes) :
    LLVMCodeGen(name, expr, datatypes) {}
    ~FilterCodeGen() {}
    int64_t getFunction() override;

private:
    int64_t createWrapper(Function* filter);
};
#endif