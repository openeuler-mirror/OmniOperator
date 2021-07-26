/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description: project  codegen
 */
#include "projection_codegen.h"
namespace {
    const int SELECTED = 3;
    const int NUM_SELECTED = 4;
    const int BITMAP = 5;
}
int64_t ProjectionCodeGen::GetFunction()
{
    Function *func = this->createFunction();
    return this->createWrapper(func);
}

int64_t ProjectionCodeGen::createWrapper(Function *proj)
{
    int32_t nArgs = this->datatypes->size();
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
    Type *bitmapArg = Type::getInt1PtrTy(*context);
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
    Argument *outputAddress = funcDecl->getArg(2);
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

    Value *minusOne = this->createConstantInt(-1);
    Value *zero = this->createConstantInt(0);
    Value *one = this->createConstantInt(1);
    std::vector<Value*> projFuncArgs;
    // filterFuncArgs contains the values of the arguments to the filter function
    // filterFuncArgs[2 * i] contains the value of the ith argument (where 0 <= i < datatypes.size())
    // filterFuncArgs[2 * i+1] contains a boolean value stating whether argument i is null
    // filterFuncArgs[2 * datatypes.size()] contains the current row number
    projFuncArgs.reserve(2 * nArgs + 1);
    Value *gep;
    Value *elementAddr;
    Value *elementPtr;
    Value *elementValue;
    // for bitmap
    Value *bitmapIdx;
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
        default:
            std::cerr << "Error: Invalid column type " << expr->GetExprDataType() << std::endl;
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
        Value *colValue = this->createConstantInt(i);
        // Find address of this column in the addresses array argument.
        gep = builder->CreateGEP(input, colValue);
        // Load the address value.
        elementAddr = builder->CreateLoad(gep);
        type = this->datatypes->at(i);
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
            default:
                std::cout << "Unsupported column data type" << std::endl;
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
        }
        // Find the address of the row to be processed.
        gep = builder->CreateGEP(elementPtr, rowIndexVal);
        // Value to be processed.
        elementValue = builder->CreateLoad(gep);
        // Pass to filter function's arguments.
        projFuncArgs.push_back(elementValue);
        // Get bitmap value bitmap[nArgs * curIndexVal + i]
        bitmapIdx = builder->CreateMul(createConstantInt(nArgs), curIndexVal);
        bitmapIdx = builder->CreateAdd(bitmapIdx, colValue, "BITMAP_INDEX");
        bitmapGEP = builder->CreateGEP(bitmap, bitmapIdx);
        bitmapValue = builder->CreateLoad(bitmapGEP);
        // Pass whether the current value is null to projection function arguments
        projFuncArgs.push_back(bitmapValue);
    }
    // Add the row number to the end of projFuncArgs
    projFuncArgs.push_back(curIndexVal);

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
    if (filter) sentinel = numSelected;
    else sentinel = numRows;
    Value *cond = builder->CreateICmpSLT(nextIndexVal, sentinel, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);
    // Return results
    builder->SetInsertPoint(endBlock);
    builder->CreateRet(nextIndexVal);
    auto resTracker = JIT->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    EOE(JIT->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    auto sym = EOE(JIT->lookup("PROJECT_WRAPPER"));
    return sym.getAddress();
}
