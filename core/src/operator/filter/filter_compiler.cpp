#include "filter_compiler.h"
#include "../../common/expressions.h"
#include "../../codegen/llvm_codegen.h"
#include <cstring>

namespace omniruntime {
namespace op {

using namespace std;

//bool testExpressionEvaluater(VectorBatch *vecBatch, int32_t index)
//{
//    Vector *column = vecBatch->getColumn(0);
//    switch (column->getType())
//    {
//    case OMNI_VEC_TYPE_INT:
//        return *((int32_t*) column->getValue(index)) % 2 == 0;
//    case OMNI_VEC_TYPE_LONG:
//        return *((int64_t*) column->getValue(index)) % 2 == 0;
//    case OMNI_VEC_TYPE_DOUBLE:
//        return *((double*) column->getValue(index)) > 10;
//    default:
//        break;
//    }
//}

Compiler::Compiler(Expr* expression, int32_t *inputTypes, int32_t vecCount)
{
    this->expression = expression;
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
}

Filter *Compiler::compile()
{
    vector<DataType>* datatypes = new vector<DataType>();
    for (int32_t i = 0; i < vecCount; i++) datatypes->push_back(expressions::colTypeTrans(inputTypes[i]));
    LLVMCodeGen* codeGenObj = new LLVMCodeGen("comparisionFunc", expression, datatypes);
    codeGenObj->compile();
    return new Filter(codeGenObj, expression);
}
} // end of op
} // end of omniruntime
