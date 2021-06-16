#include "llvm_codegen.h"

Value* LLVMCodeGen::createConstantInt(int32_t v) {
    return ConstantInt::get(*context, APInt(32, v, true));
}

Value* LLVMCodeGen::createConstantLong(int64_t v) {
    return ConstantInt::get(*context, APInt(64, v, true));
}

Value* LLVMCodeGen::createConstantDouble(double v) {
    return ConstantFP::get(*context, APFloat(v));
}

LLVMCodeGen::LLVMCodeGen(std::string name, Expr *expr, vector<DataType>* datatypes)
{
    _func_name = name;
    _expr = expr;
    this->datatypes = datatypes;

    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    // llvm::InitializeNativeTargetDisassembler();
    JIT = EOE(LLJITBuilder().create());
    
    context = make_unique<LLVMContext>();
    // Create module called the_module
    _module = make_unique<Module>("the_module", *context);
    _module->setDataLayout(JIT->getDataLayout());

    builder = make_unique<IRBuilder<>>(*context);
}

LLVMCodeGen::~LLVMCodeGen() {
    rt->remove();
    delete this->datatypes;
}

int32_t LLVMCodeGen::execute(int64_t* data, int32_t nRows, int32_t* selected) {
    return this->_filter(data, nRows, selected);
}

Function* LLVMCodeGen::createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse) {
    vector<Type*> args;
    args.reserve(datatypes->size());
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        switch (type) {
            case DataType::INT32D:
                args.push_back(Type::getInt32Ty(*context));
                break;
            case DataType::INT64D:
                args.push_back(Type::getInt64Ty(*context));
                break;
            case DataType::DOUBLED:
                args.push_back(Type::getDoubleTy(*context));
                break;
            case DataType::BOOLD:
                args.push_back(Type::getInt1Ty(*context));
                break;
            case DataType::STRINGD:
                // todo
                cout << "Error: Unsupported string argument type" << endl;
                break;
            default:
                cout << "Error: Unknown argument datatype " << type << endl;
                break;
        }
    }
    Type* retTypePtr;
    switch (retType) {
    case INT32D:
        retTypePtr = Type::getInt32Ty(*context);
        break;
    case INT64D:
        retTypePtr = Type::getInt64Ty(*context);
        break;
    case DOUBLED:
        retTypePtr = Type::getDoubleTy(*context);
        break;
    case BOOLD:
        retTypePtr = Type::getInt1Ty(*context);
        break;
    }
    FunctionType *prototype = FunctionType::get(retTypePtr, args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, "IF_CONDITIONAL", _module.get());

    int32_t idx = 0;
    for (auto& arg : func->args()) {
        arg.setName(to_string(idx++));
    }

    BasicBlock* conditionalCheck = BasicBlock::Create(*context, "CONDITIONAL_CHECK", func);
    BasicBlock* trueBlock = BasicBlock::Create(*context, "TRUE_BLOCK", func);
    BasicBlock* falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK", func);

    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    builder->SetInsertPoint(conditionalCheck);
    Value* evCond = this->parseExpr(cond, fArgs);
    Value* evTrue = this->parseExpr(ifTrue, fArgs);
    Value* evFalse = this->parseExpr(ifFalse, fArgs);
    builder->CreateCondBr(evCond, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    builder->CreateRet(evTrue);
    builder->SetInsertPoint(falseBlock);
    builder->CreateRet(evFalse);

    verifyFunction(*func);
    return func;
}

Value* LLVMCodeGen::parseExpr(Expr* root, map<string, Value*>& args) {
    switch (root->getType()) {
    case ExprType::BINARY_E: {
        BinaryExpr* bExpr = (BinaryExpr*) root;
        if (bExpr->left->getType() == ExprType::DATA_E || bExpr->right->getType() == ExprType::DATA_E) {
            DataType biggerType = std::max(bExpr->left->getExprDataType(), bExpr->right->getExprDataType());
            bExpr->left->dataType = biggerType;
            bExpr->right->dataType = biggerType;
        }
        Value* left = this->parseExpr(bExpr->left, args);
        Value* right = this->parseExpr(bExpr->right, args);

        if (bExpr->op == expressions::Operator::AND) {
            return builder->CreateAnd(left, right, "logical_and");
        }
        if (bExpr->op == expressions::Operator::OR) {
            return builder->CreateOr(left, right, "logical_or");
        }

        // todo: Need a different case for strings
        if (bExpr->left->getExprDataType() != DataType::DOUBLED) {
            switch(bExpr->op) {
            case LT:
                return builder->CreateICmpSLT(left, right, "relational_lt");
            case GT:
                return builder->CreateICmpSGT(left, right, "relational_gt");
            case LTE:
                return builder->CreateICmpSLE(left, right, "relational_le");
            case GTE:
                return builder->CreateICmpSGE(left, right, "relational_ge");
            case EQ:
                return builder->CreateICmpEQ(left, right, "relational_eq");
            case NEQ:
                return builder->CreateNot(builder->CreateICmpEQ(left, right), "relational_neq");
            case ADD:
                return builder->CreateAdd(left, right, "arithmetic_add");
            case SUB:
                return builder->CreateSub(left, right, "arithmetic_sub");
            case MUL:
                return builder->CreateMul(left, right, "arithmetic_mul");
            case DIV:
                return builder->CreateSDiv(left, right, "arithmetic_div");
            case MOD:
                return builder->CreateSRem(left, right, "arithmetic_mod");
            }
            cout << "Error: Binary operator not supported " << bExpr->op << endl;
        }
        // Assume double
        switch(bExpr->op) {
        case LT:
            return builder->CreateFCmpULT(left, right, "frelational_lt");
        case GT:
            return builder->CreateFCmpUGT(left, right, "frelational_gt");
        case LTE:
            return builder->CreateFCmpULE(left, right, "frelational_le");
        case GTE:
            return builder->CreateFCmpUGE(left, right, "frelational_ge");
        case EQ:
            return builder->CreateFCmpUEQ(left, right, "farithmetic_eq");
        case NEQ:
            return builder->CreateNot(builder->CreateFCmpUEQ(left, right, "farithmetic_neq"));
        case ADD:
            return builder->CreateFAdd(left, right, "farithmetic_add");
        case SUB:
            return builder->CreateFSub(left, right, "farithmetic_sub");
        case MUL:
            return builder->CreateFMul(left, right, "farithmetic_mul");
        case DIV:
            return builder->CreateFDiv(left, right, "farithmetic_div");
        }
        cout << "Error: Unsupported double binary expr op " << bExpr->op << endl;
        // default:
        //     break;
    }
    case ExprType::DATA_E: {
        DataExpr* dEx = (DataExpr*) root;
        if (dEx->isColumn) {
            return args[to_string(dEx->colVal)];
        }
        switch (dEx->getExprDataType()) {
        case DataType::INT32D: {
            return this->createConstantInt(dEx->intVal);
        }
        case DataType::INT64D: {
            return this->createConstantLong(dEx->longVal);
        }
        case DataType::DOUBLED: {
            return this->createConstantDouble(dEx->doubleVal);
        }
        case DataType::STRINGD: {
            cout << "Error: Unsupported datatype String" << endl;
            break;
        }
        }
        break;
    }
    case ExprType::CALL_E: {
        CallExpr* ce = (CallExpr*) root;
        switch (ce->callType) {
        case IF: {
            BasicBlock* currentBlock = builder->GetInsertBlock();
            DataType retType = ce->arguments[1]->getExprDataType();
            Expr* cond = ce->arguments[0];
            Expr* ifTrue = ce->arguments[1];
            Expr* ifFalse = ce->arguments[2];

            Function* conditionalFunc = createConditional(retType, cond, ifTrue, ifFalse);
            builder->SetInsertPoint(currentBlock);
            vector<Value*> passArgs;
            for (map<string, Value*>::iterator i = args.begin() ; i != args.end() ; i ++ ) {
                Value* a = i->second;
                passArgs.push_back(a);
            }
            CallInst* condCall = builder->CreateCall(conditionalFunc, passArgs, "EVAL_IF");
            return condCall;
        }
        default:
            cout << "Error: Unsupported call type " << ce->callType << endl;
        }
        break;
    }
    default:
        cout << "Error: Unsupported expr type " << root->getType() << endl;
        break;
    }
}

// Logic to generate the filter function.
Function* LLVMCodeGen::generateFunc()
{   
    // cout << "Generating function" << endl;
    vector<Type*> args;
    args.reserve(datatypes->size());
    for (int32_t i = 0; i < datatypes->size(); i++) {
        DataType type = datatypes->at(i);
        // cout << "Adding argument of type " << type << endl;
        switch (type) {
            case DataType::INT32D:
                args.push_back(Type::getInt32Ty(*context));
                break;
            case DataType::INT64D:
                args.push_back(Type::getInt64Ty(*context));
                break;
            case DataType::DOUBLED:
                args.push_back(Type::getDoubleTy(*context));
                break;
            case DataType::STRINGD:
                // todo
                cout << "Error: Unsupported string argument type" << endl;
                break;
            default:
                cout << "Error: Unknown argument datatype " << type << endl;
                break;
        }
    }
    
    FunctionType *prototype = FunctionType::get(Type::getInt1Ty(*context), args, false);
    Function *func = Function::Create(prototype, Function::ExternalLinkage, _func_name, _module.get());
    // cout << "Created function declaration " << _func_name << endl;
    int32_t idx = 0;
    for (auto& arg : func->args()) {
        arg.setName(to_string(idx++));
    }
    BasicBlock *body = BasicBlock::Create(*context, "FILTER_FUNC_BODY", func);
    builder->SetInsertPoint(body);
    map<string, Value*> fArgs;
    for (auto& arg : func->args()) {
        fArgs[arg.getName().str()] = &arg;
    }

    // cout << "Generating filter body" << endl;
    Value* ret = this->parseExpr(_expr, fArgs);
    builder->CreateRet(ret);
    verifyFunction(*func);
    return func;
}

void LLVMCodeGen::compile()
{
    Function* filterFunc = this->generateFunc();
    int64_t address = this->createWrapper(filterFunc);
    _filter = (int32_t (*)(int64_t*, int32_t, int32_t*)) (intptr_t) address;
}

std::string LLVMCodeGen::dumpCode() {
  std::string ir;
  llvm::raw_string_ostream stream(ir);
  _module->print(stream, nullptr);
  cout<<" Generated code::" << ir;
  return ir;
}

int64_t LLVMCodeGen::createWrapper(Function* filterFunc) {
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
    Value* minusOne = createConstantInt(-1);
    Value* zero = createConstantInt(0);
    Value* one = createConstantInt(1);
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
        gep = builder->CreateGEP(start, createConstantInt(i));
        // Load the address value.
        elementAddr = builder->CreateLoad(gep);
        type = this->datatypes->at(i);
        // Convert the column address to array of proper datatype.
        switch (type) {
        case DataType::INT32D:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt32PtrTy(*context));
            break;
        case DataType::INT64D:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
            break;
        case DataType::DOUBLED:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getDoublePtrTy(*context));
            break;
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
    _module->print(errs(), nullptr);
    auto resTracker = JIT->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = ThreadSafeModule(move(_module), move(context));
    EOE(JIT->addIRModule(resTracker, move(threadSafeModule)));
    rt = resTracker;
    // initModule();
    auto sym = JIT->lookup("FILTER_WRAPPER");
    return sym->getAddress();
}
