/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:filter code generation methods
 */
#include "filter_codegen.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;

namespace {
    const int ARGUMENT_ZERO = 0;
    const int ARGUMENT_ONE = 1;
    const int ARGUMENT_TWO = 2;
    const int ARGUMENT_THREE = 3;
}

int64_t FilterCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    return this->CreateWrapper(*func);
}


int64_t FilterCodeGen::CreateWrapper(Function &filterFn)
{
    Function *filterFunc = &filterFn;
    int32_t nArgs = this->datatypes.size();

    std::vector<Type *> args;
    Type *ptrArg = Type::getInt64PtrTy(*context); // table
    args.push_back(ptrArg);
    args.push_back(Type::getInt32Ty(*context)); // no of rows
    args.push_back(Type::getInt32PtrTy(*context)); // output array
    // bitmap is a 2d array of booleans
    Type *bitmapArg = Type::getInt64PtrTy(*context); // record nullk values
    args.push_back(bitmapArg);
    FunctionType *funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FILTER_WRAPPER", module.get());
    BasicBlock *preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock *loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock *filterPassed = BasicBlock::Create(*context, "FILTER_PASSED", funcDecl);
    BasicBlock *incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock *endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument *start = funcDecl->getArg(ARGUMENT_ZERO);
    start->setName("ARGS_ARRAY");
    Argument *numRows = funcDecl->getArg(ARGUMENT_ONE);
    numRows->setName("NUM_ROWS");
    Argument *resultsArray = funcDecl->getArg(ARGUMENT_TWO);
    resultsArray->setName("RESULTS");
    Argument *bitmap = funcDecl->getArg(ARGUMENT_THREE);
    bitmap->setName("BITMAP");
    Value *minusOne = this->CreateConstantInt(-1);
    Value *zero = this->CreateConstantInt(0);
    Value *one = this->CreateConstantInt(1);
    std::vector<Value*> filterFuncArgs;
    // filterFuncArgs contains the values of the arguments to the filter function
    // filterFuncArgs[2 * i] contains the value of the ith argument (where 0 <= i < nArgs)
    // filterFuncArgs[2 * i+1] contains a boolean value stating whether argument i is null
    // filterFuncArgs[2 * nArgs] contains the current row number
    filterFuncArgs.reserve(2 * nArgs);
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
    // Pointer to the current row index to be processed.
    AllocaInst *indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Value of the current row index to be processed.
    Value *curIndexVal;
    // Temp value for next row index.
    Value *nextIndexVal;
    // Pointer to the index of the selected positions array to be filled next.
    AllocaInst *selectedIndexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "SELECTED_INDEX_PTR");
    // Initialize index to 0.
    builder->CreateStore(zero, selectedIndexStore);
    // Value of the selected positions index.
    Value *selectedIndexVal;
    // Address of the selected index for writing.
    Value *selectedAddress;
    // Temp value for next selected index.
    Value *nextSelectedIndexVal;
    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");
    for (int32_t i = 0; i < nArgs; i++) {
        Value *colValue = this->CreateConstantInt(i);
        // Find address of this column in the addresses array argument.
        gep = builder->CreateGEP(start, colValue);
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
        gep = builder->CreateGEP(elementPtr, curIndexVal);
        // Value to be processed.
        elementValue = builder->CreateLoad(gep);
        // Pass to filter function's arguments.
        filterFuncArgs.push_back(elementValue);

        // Get bitmap value bitmap[i][j]

        bitmapGEP = builder->CreateGEP(bitmap, colValue);
        bitmapValue = builder->CreateLoad(bitmapGEP);
        bitmapValue = builder->CreateIntToPtr(bitmapValue, Type::getInt1PtrTy(*context));
        bitmapGEP = builder->CreateGEP(bitmapValue, curIndexVal);
        bitmapValue = builder->CreateLoad(bitmapGEP);

        // Pass whether the current value is null to filter function arguments
        filterFuncArgs.push_back(bitmapValue);
    }

    // Get the boolean response for this row from the filter function.
    ret = builder->CreateCall(filterFunc, filterFuncArgs, "ROW_EVAL");
    // If true, add row index to selected array, otherwise, process next row.
    builder->CreateCondBr(ret, filterPassed, incrementCounter);
    // Add row index to results array
    builder->SetInsertPoint(filterPassed);
    // Get value of selected index.
    selectedIndexVal = builder->CreateLoad(selectedIndexStore, "SELECTED_INDEX");
    // Get address of selected index.
    selectedAddress = builder->CreateGEP(resultsArray, selectedIndexVal, "SELECTED_ADDRESS");
    // Set the selected value to the current row index.
    builder->CreateStore(curIndexVal, selectedAddress);
    // Increment the selected index.
    nextSelectedIndexVal = builder->CreateAdd(selectedIndexVal, one, "NEXT_SELECTED_INDEX");
    builder->CreateStore(nextSelectedIndexVal, selectedIndexStore);
    // Increment counter and process next row.
    builder->CreateBr(incrementCounter);
    // Increment loop counter
    builder->SetInsertPoint(incrementCounter);
    // Increment counter.
    nextIndexVal = builder->CreateAdd(curIndexVal, one, "NEXT_INDEX");
    builder->CreateStore(nextIndexVal, indexStore);
    // If there are rows remaining, repeat, otherwise, exit.
    Value *cond = builder->CreateICmpSLT(nextIndexVal, numRows, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);

    builder->SetInsertPoint(endBlock);

    nextSelectedIndexVal = builder->CreateLoad(selectedIndexStore);
    builder->CreateRet(nextSelectedIndexVal);

    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;

    auto sym = eoe(jit->lookup("FILTER_WRAPPER"));
    return sym.getAddress();
}

std::vector<Type*> GetSingleFilterArguments(LLVMContext &context)
{
    std::vector<Type*> args = {
        Type::getInt64PtrTy(context),
        Type::getInt1PtrTy(context),
        Type::getInt32Ty(context)
    };
    return args;
}

int64_t FilterCodeGen::GetExpressionEvaluator()
{
    int32_t nCols = this->datatypes.size();
    // Array of addresses, bitmap, row index
    std::vector<Type*> args = GetSingleFilterArguments(*context);
    Function* baseFunc = this->CreateFunction();
    FunctionType* funcSignature = FunctionType::get(Type::getInt1Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FUNC_WRAPPER", module.get());
    BasicBlock *wrapperBody = BasicBlock::Create(*context, "DATA_ACCESS", funcDecl);
    builder->SetInsertPoint(wrapperBody);
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
    DataType type;

    Value* bitmapIdx;
    Value* bitmapGEP;
    Value* bitmapValue;
    std::vector<Value*> funcArgs;
    for (int32_t i = 0; i < nCols; i++) {
        // Get the address for column i
        // gep is of type int64_t* pointing to the value of the address
        colIndex = CreateConstantInt(i);
        gep = builder->CreateGEP(inputData, colIndex);
        // Derefence the gep, colPtr is now type int64_t
        colPtr = builder->CreateLoad(gep);
        type = this->datatypes.at(i);
        // Convert colPtr to proper ponter type instead of int64_t
        colPtr = builder->CreateIntToPtr(colPtr, ToPointerType(type));
        // Get pointer to value at rowIndex for this column
        gep = builder->CreateGEP(colPtr, rowIndex);
        colValue = builder->CreateLoad(gep);
        funcArgs.push_back(colValue);

        // Get bitmap value bitmap[nArgs * curIndexVal + i]
        bitmapIdx = builder->CreateMul(CreateConstantInt(nCols), rowIndex, "FIRST_COL_IDX");
        bitmapIdx = builder->CreateAdd(bitmapIdx, colIndex, "BITMAP_INDEX");
        bitmapGEP = builder->CreateGEP(nulls, bitmapIdx);
        bitmapValue = builder->CreateLoad(bitmapGEP);
        funcArgs.push_back(bitmapValue);
    }

    builder->CreateRet(builder->CreateCall(baseFunc, funcArgs, "ROW_EVAL"));
#ifdef DEBUG
    module->print(errs(), nullptr);
#endif
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    return eoe(jit->lookup("FUNC_WRAPPER")).getAddress();
}