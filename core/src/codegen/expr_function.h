/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Generated Expression Function
 */
#ifndef OMNI_RUNTIME_EXPR_FUNCTION_H
#define OMNI_RUNTIME_EXPR_FUNCTION_H

#include <utility>
#include <llvm/IR/Function.h>
#include <llvm/IR/Argument.h>

#include "expression/expr_visitor.h"
#include "expression/expressions.h"
#include "codegen/llvm_types.h"
#include "codegen/codegen_base.h"
#include "vector/vector_batch.h"

namespace omniruntime::codegen {
using namespace llvm;
using namespace omniruntime::expressions;

struct ExprArgument {
    std::string name;
    llvm::Type *type;
};

/**
 * This class encapsulates the generated expression function so that we don't have to expose the details on handling the
 * argument index, invoking the function, etc.
 * We invoke this function to process every row of data, hence it's performance can dramatically impact the performance
 * of expression evaluation
 */
class ExprFunction {
public:
    /**
     * The ExprFunction should be in a "complete" state once created, e.g. access to this object should
     * provide valid and consistent information. It is also reentrant, e.g. all functions can be invoked
     * as many times as need without impacting the consistency of the information contained in this class
     */
    ExprFunction(std::string funcName, const Expr &e, CodegenBase &codegen, const DataTypes &inputDataTypes);

    Argument *GetColumnArgument(int i);

    Argument *GetDicArgument(int i);

    Argument *GetNullArgument(int i);

    Argument *GetOffsetArgument(int i);

    int32_t GetInputColumnCount();

    size_t GetArgumentCount();

    /**
     * @return return all arguments to the generated function
     */
    std::vector<Type *> GetArguments();

    /**
     * @return returns the return type of the generated function
     */
    Type *GetReturnType();

    /**
     * @return returns the LLVM function
     */
    llvm::Function *GetFunction();

    /**
     * Convert the data from each column in the table into individual arguments
     * @param table the pointer of data address array
     * @return the vector containing data pointer for each column
     */
    std::vector<Value *> ToColumnArgs(Value *data);

    /**
     * Convert the dictionary from each column in the table into individual arguments
     * @param dictionary the pointer of dictionary address array
     * @return the vector containing dictionary pointer for each column
     */
    std::vector<Value *> ToDicArgs(Value *dictionary);

    /**
     * Convert the bitmap from each column in the table into individual arguments
     * @param bitmap the pointer of bitmap address array
     * @return the vector containing bitmap pointer for each column
     */
    std::vector<Value *> ToNullArgs(Value *bitmap);

    /**
     * Convert the offset from each column in the table into individual arguments
     * @param offset the pointer of offset address array
     * @return the vector containing offset pointer for each column
     */
    std::vector<Value *> ToOffsetArgs(Value *offset);

private:
    std::string funcName;
    Expr &expr;
    CodegenBase &codegen;
    llvm::Function *llvmFunc = nullptr;
    DataTypes columnTypes;

    /**
     * Predefined argument list
     * The input columns will be appended to the predefined argument list
     */
    std::vector<ExprArgument> arguments { { "rowIdx", Type::getInt32Ty(*codegen.GetContext()) },
        { "dataLength", Type::getInt32PtrTy(*codegen.GetContext()) },
        { "executionContext", Type::getInt64Ty(*codegen.GetContext()) },
        { "isNullPtr", Type::getInt1PtrTy(*codegen.GetContext()) } };

    /**
     * Method to create prototype of function
     */
    void CreateFunction();
};
}

#endif // OMNI_RUNTIME_EXPR_FUNCTION_H
