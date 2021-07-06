#include "filter_codegen.h"

int64_t FilterCodeGen::getFunction() {
    Function* func = this->createFunction();
    return this->createWrapper(func);
}

int64_t FilterCodeGen::createWrapper(Function* filterFunc) {
    int32_t nArgs = this->datatypes->size();
    vector<Type*> args;
    Type* ptrArg = Type::getInt64PtrTy(*context);
    args.push_back(ptrArg);
    args.push_back(Type::getInt32Ty(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    FunctionType* funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function* funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FILTER_WRAPPER", _module.get());
    BasicBlock* preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock* loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock* filterPassed = BasicBlock::Create(*context, "FILTER_PASSED", funcDecl);
    BasicBlock* incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock* endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument* start = funcDecl->getArg(0);
    start->setName("ARGS_ARRAY");
    Argument* numRows = funcDecl->getArg(1);
    numRows->setName("NUM_ROWS");
    Argument* resultsArray = funcDecl->getArg(2);
    resultsArray->setName("RESULTS");
    Value* minusOne = this->createConstantInt(-1);
    Value* zero = this->createConstantInt(0);
    Value* one = this->createConstantInt(1);
    vector<Value*> filterFuncArgs;
    filterFuncArgs.reserve(nArgs);
    Value* gep;
    Value* elementAddr;
    Value* elementPtr;
    Value* elementValue;
    DataType type;
    CallInst* ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to the current row index to be processed.
    AllocaInst* indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Value of the current row index to be processed.
    Value* curIndexVal;
    // Temp value for next row index.
    Value* nextIndexVal;
    // Pointer to the index of the selected positions array to be filled next.
    AllocaInst* selectedIndexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "SELECTED_INDEX_PTR");
    // Initialize index to 0.
    builder->CreateStore(zero, selectedIndexStore);
    // Value of the selected positions index.
    Value* selectedIndexVal;
    // Address of the selected index for writing.
    Value* selectedAddress;
    // Temp value for next selected index.
    Value* nextSelectedIndexVal;
    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");
    for (int32_t i = 0; i < nArgs; i++) {
        // Find address of this column in the addresses array argument.
        gep = builder->CreateGEP(start, this->createConstantInt(i));
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
                cout << "Unsupported column data type" << endl;
                elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
        }
        // Find the address of the row to be processed.
        gep = builder->CreateGEP(elementPtr, curIndexVal);
        // Value to be processed.
        elementValue = builder->CreateLoad(gep);
        // Pass to filter function's arguments.
        filterFuncArgs.push_back(elementValue);
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
    Value* cond = builder->CreateICmpSLT(nextIndexVal, numRows, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);
    // Return results
    builder->SetInsertPoint(endBlock);
    // Return the filled in results.
    nextSelectedIndexVal = builder->CreateLoad(selectedIndexStore);
    builder->CreateRet(nextSelectedIndexVal);
    // _module->print(errs(), nullptr);
    auto resTracker = JIT->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = ThreadSafeModule(move(_module), move(context));
    EOE(JIT->addIRModule(resTracker, move(threadSafeModule)));
    rt = resTracker;
    // initModule();
    auto sym = EOE(JIT->lookup("FILTER_WRAPPER"));
    return sym.getAddress();
}