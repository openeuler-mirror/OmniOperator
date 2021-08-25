/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#include "projection_codegen.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;


namespace {
    const int ARG2 = 2;
    const int SELECTED = 3;
    const int NUM_SELECTED = 4;
    const int BITMAP = 5;
    const int ARGUMENT_ZERO = 0;
    const int ARGUMENT_ONE = 1;
    const int ARGUMENT_TWO = 2;
}
int64_t ProjectionCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    return this->CreateWrapper(*func);
}


int64_t ProjectionCodeGen::CreateWrapper(Function &projFunc)
{
    Function *proj = &projFunc;

    int32_t nArgs = this->datatypes.size();
    std::vector<Type*> args;
    /*
    For filter enabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr)
    For filter disabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr, i32* selected_array, i32 num_selected)
    */
    // Input table, array of addresses
    args.push_back(Type::getInt64PtrTy(*context));
    // Number of rows in input
    args.push_back(Type::getInt32Ty(*context));
    // Results column to write to
    args.push_back(Type::getInt64Ty(*context));
    // These two arguments will not be used if filter is disabled
    // Array of indices from input to select
    args.push_back(Type::getInt32PtrTy(*context));
    // Number of selected rows
    args.push_back(Type::getInt32Ty(*context));
    // bitmap is a 2d array of booleans
    Type *bitmapArg = Type::getInt64PtrTy(*context);
    args.push_back(bitmapArg);
    FunctionType *funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "PROJECT_WRAPPER", module.get());
    BasicBlock *preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock *loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock *addToOutput = BasicBlock::Create(*context, "ADD_OUTPUT", funcDecl);
    BasicBlock *incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock *endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument *input = funcDecl->getArg(0);
    input->setName("INPUT_TABLE");
    Argument *numRows = funcDecl->getArg(1);
    numRows->setName("NUM_ROWS");
    Argument *outputAddress = funcDecl->getArg(ARG2);
    outputAddress->setName("OUTPUT_ADDRESS");

    // Only use these values if filter enabled
    Argument *selected;
    Argument *numSelected;
    if (filter) {
        selected = funcDecl->getArg(SELECTED);
        selected->setName("SELECTED_ARRAY");
        numSelected = funcDecl->getArg(NUM_SELECTED);
        numSelected->setName("NUM_SELECTED");
    }

    Argument *bitmap = funcDecl->getArg(BITMAP);
    bitmap->setName("BITMAP");

    Value *minusOne = this->CreateConstantInt(-1);
    Value *zero = this->CreateConstantInt(0);
    Value *one = this->CreateConstantInt(1);
    std::vector<Value*> projFuncArgs;
    // filterFuncArgs contains the values of the arguments to the filter function
    // filterFuncArgs[2 * i] contains the value of the ith argument (where 0 <= i < datatypes.size())
    // filterFuncArgs[2 * i+1] contains a boolean value stating whether argument i is null
    projFuncArgs.reserve(ARG2 * nArgs);
    Value *gep;
    Value *elementAddr;
    Value *elementPtr;
    Value *elementValue;
    // for bitmap
    Value *bitmapIdx = nullptr;
    Value *bitmapGEP;
    Value *bitmapValue;

    DataType type;
    CallInst *ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to counter
    // i32* ptrToCounter
    AllocaInst *indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Counter variable value.
    // i32 counter
    Value *curIndexVal;
    // Index of row to be processed.
    // i32 rowIndex
    Value *rowIndexVal;
    // Temp value for next row index.
    // i32 nextCounterValue
    Value *nextIndexVal;

    // Only use if filter enabled
    // i64 selectedAddress
    Value *selectedAddress;

    // Type of output column
    Type *outPtrType;

    switch (this->expr->GetExprDataType()) {
        case DataType::INT32D:
            outPtrType = Type::getInt32PtrTy(*context);
            break;
        case DataType::INT64D:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        case DataType::DOUBLED:
            outPtrType = Type::getDoublePtrTy(*context);
            break;
        case DataType::STRINGD:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        case DataType::DECIMAL128D:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        default:
            LLVM_DEBUG_LOG("Error: Invalid column type %d", expr->GetExprDataType());
            break;
    }
    Value *outColPtr = builder->CreateIntToPtr(outputAddress, outPtrType);

    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    // i32 counter = *ptrToCounter
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");
    if (filter) {
        // Get address of selected index.
        // i32* selectedAddress = gep i32* selected, i32 counter
        selectedAddress = builder->CreateGEP(selected, curIndexVal, "SELECTED_ADDRESS");
        // i32 rowIndexVal = *selectedAddress
        rowIndexVal = builder->CreateLoad(selectedAddress);
    } else {
        // i32 rowIndexVal = counter
        rowIndexVal = curIndexVal;
    }

    for (int32_t i = 0; i < nArgs; i++) {
        Value *colValue = this->CreateConstantInt(i);
        // Find address of this column in the addresses array argument.
        gep = builder->CreateGEP(input, colValue);

        // Load the address value.
        elementAddr = builder->CreateLoad(gep);
        type = this->datatypes.at(i);

        // Convert the column address to array of proper datatype.
        switch (type) {
            case DataType::BOOLD:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt1PtrTy(*context));
                break;
            case DataType::INT32D:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt32PtrTy(*context));
                break;
            case DataType::INT64D:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
            case DataType::DOUBLED:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getDoublePtrTy(*context));
                break;
            case DataType::STRINGD:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
            case DataType::DECIMAL128D:
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
            default:
                LLVM_DEBUG_LOG("Unsupported column data type %d", type);
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
                break;
        }
        // Find the address of the row to be processed.
        gep = builder->CreateGEP(elementPtr, rowIndexVal);
        // Value to be processed.
        elementValue = builder->CreateLoad(gep);
        // Pass to filter function's arguments.
        projFuncArgs.push_back(elementValue);

        // Get bitmap value bitmap[i][j]

        bitmapGEP = builder->CreateGEP(bitmap, colValue);
        bitmapValue = builder->CreateLoad(bitmapGEP);
        bitmapValue = builder->CreateIntToPtr(bitmapValue, Type::getInt1PtrTy(*context));
        bitmapGEP = builder->CreateGEP(bitmapValue, curIndexVal);
        bitmapValue = builder->CreateLoad(bitmapGEP);
        // Pass whether the current value is null to projection function arguments
        projFuncArgs.push_back(bitmapValue);
    }

    // Get the boolean response for this row from the filter function.
    // ret = column value after applying projection
    ret = builder->CreateCall(proj, projFuncArgs, "ROW_PROCESS");
    // Add the processed value to output column.
    builder->CreateBr(addToOutput);
    // Add row index to results array
    builder->SetInsertPoint(addToOutput);
    // x* gep = gep x* outColPtr, i32 counter
    gep = builder->CreateGEP(outColPtr, curIndexVal, "OUTPUT_ADDRESS");
    // *gep = ret
    builder->CreateStore(ret, gep);
    builder->CreateBr(incrementCounter);
    // Increment loop counter
    builder->SetInsertPoint(incrementCounter);
    // Increment counter.
    nextIndexVal = builder->CreateAdd(curIndexVal, one, "NEXT_INDEX");
    builder->CreateStore(nextIndexVal, indexStore);
    // If there are rows remaining, repeat, otherwise, exit.
    Value *sentinel;
    if (filter) {
        sentinel = numSelected;
    } else {
        sentinel = numRows;
    }
    Value *cond = builder->CreateICmpSLT(nextIndexVal, sentinel, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);
    // Return results
    builder->SetInsertPoint(endBlock);
    builder->CreateRet(nextIndexVal);

    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    auto sym = eoe(jit->lookup("PROJECT_WRAPPER"));
    return sym.getAddress();
}

std::vector<Type*> GetSingleProjectArguments(LLVMContext &context)
{
    std::vector<Type*> args = {
        Type::getInt64PtrTy(context),
        Type::getInt1PtrTy(context),
        Type::getInt32Ty(context)
    };
    return args;
}

/*
Apply the row expression on a single row in the table.
Returns the address of a function with the signature void* (*) (int64_t*, bool*, int32_t)
Takes the following arguments
- An array of addresses representing the input table, where each address points to a vec column.
- A 1D array of bools representing the null values in the table.
- An integer representing the row index to select and perform the row expression for.
In reality the function returns a pointer of appropriate type depending on the row expression
and input types. For example if the expected output is an int32, the function will return int32_t*
but since this type is not known at compile time it can be treated as void* or int64_t or any 8 byte
datatype and casted appropriately.
*/
int64_t ProjectionCodeGen::GetExpressionEvaluator()
{
    int32_t nCols = this->datatypes.size();
    // Array of addresses, bitmap, row index
    std::vector<Type*> args = GetSingleProjectArguments(*context);
    int32_t retIdx = -1;
    Function* baseFunc = nullptr;
    // Special case for when the projection is only a column index
    if (expr->GetType() == ExprType::DATA_E) {
        auto *dEx = static_cast<DataExpr *>(expr);
        if (dEx->isColumn) {
            retIdx = dEx->colVal;
        }
    } else {
        baseFunc = this->CreateFunction();
    }
    FunctionType* funcSignature = FunctionType::get(ToPointerType(expr->GetExprDataType()), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FUNC_WRAPPER", module.get());
    builder->SetInsertPoint(BasicBlock::Create(*context, "DATA_ACCESS", funcDecl));
    // Name the arguments
    Argument *inputData = funcDecl->getArg(ARGUMENT_ZERO);
    inputData->setName("INPUT_DATA");
    Argument *nulls = funcDecl->getArg(ARGUMENT_ONE);
    nulls->setName("NULLS");
    Argument *rowIndex = funcDecl->getArg(ARGUMENT_TWO);
    rowIndex->setName("ROW_INDEX");

    Value* gep;
    Value* colValue;
    Value* colPtr;
    Value* colIndex;

    if (retIdx != -1) {
        gep = builder->CreateGEP(inputData, CreateConstantInt(retIdx));
        colPtr = builder->CreateLoad(gep);
        colPtr = builder->CreateIntToPtr(colPtr, ToPointerType(datatypes.at(retIdx)));
        builder->CreateRet(builder->CreateGEP(colPtr, rowIndex));
    } else {
        Value* bitmapIdx;
        Value* bitmapGEP;
        std::vector<Value*> funcArgs;
        for (int32_t i = 0; i < nCols; i++) {
            // Get the address for column i
            // gep is of type int64_t* pointing to the value of the address
            colIndex = CreateConstantInt(i);
            gep = builder->CreateGEP(inputData, colIndex);
            // Derefence the gep, colPtr is now type int64_t
            colPtr = builder->CreateLoad(gep);
            // Convert colPtr to proper ponter type instead of int64_t
            colPtr = builder->CreateIntToPtr(colPtr, ToPointerType(this->datatypes.at(i)));
            // Get pointer to value at rowIndex for this column
            gep = builder->CreateGEP(colPtr, rowIndex);
            colValue = builder->CreateLoad(gep);
            funcArgs.push_back(colValue);

            // Get bitmap value bitmap[nArgs * curIndexVal + i]
            bitmapIdx = builder->CreateMul(CreateConstantInt(nCols), rowIndex, "FIRST_COL_IDX");
            bitmapIdx = builder->CreateAdd(bitmapIdx, colIndex, "BITMAP_INDEX");
            bitmapGEP = builder->CreateGEP(nulls, bitmapIdx);
            funcArgs.push_back(builder->CreateLoad(bitmapGEP));
        }

        // Store the result
        AllocaInst *retStore = builder->CreateAlloca(baseFunc->getReturnType(), nullptr, "RET_STORE");
        builder->CreateStore(builder->CreateCall(baseFunc, funcArgs, "ROW_EVAL"), retStore);

        builder->CreateRet(retStore);
    }
#ifdef DEBUG
    module->print(errs(), nullptr);
#endif
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    return eoe(jit->lookup("FUNC_WRAPPER")).getAddress();
}
