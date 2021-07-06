#ifndef PROJECTION_CODEGEN_H
#define PROJECTION_CODEGEN_H

#include "llvm_codegen.h"

class ProjectionCodeGen : public LLVMCodeGen {
public:
ProjectionCodeGen(string name, Expr* expr, vector<DataType>* datatypes, bool filter) :
LLVMCodeGen(name, expr, datatypes), filter(filter) {}
int64_t getFunction() override;
bool isFilterEnabled() {return filter;}

private:
    int64_t createWrapper(Function* proj);
    bool filter;
};
#endif