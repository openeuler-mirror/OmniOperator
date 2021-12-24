/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "row_expression_codegen.h"

using namespace std;
using namespace llvm;
using namespace llvm::orc;
using namespace omniruntime::expressions;

namespace {
    const string FUNCTION_NAME = "ROW_EXPR_EVALUATOR";
}

void RowExpressionCodeGen::Visit(const omniruntime::expressions::DataExpr &dataExpr)
{
    if (dataExpr.isColumn) {
        Value *data = this->codegenContext->data;
        Value *isNulls = this->codegenContext->nullBitmap;
        Value *lengths = this->codegenContext->offsets;

        Value *colIdx = this->CreateConstantInt(dataExpr.colVal);
        // Find address of this column in the addresses array argument.
        Value *gep = builder->CreateGEP(data, colIdx);
        // Load the address value.
        Value *elementAddr = builder->CreateLoad(gep);
        Value *elementPtr = GetIntToPtr(dataExpr, elementAddr);

        Value *dataValue = nullptr;
        Value *length = nullptr;
        if (IsStringDataType(dataExpr.GetExprDataType())) {
            // Get length for varchar/char type
            auto lengthGEP = builder->CreateGEP(lengths, colIdx);
            length = builder->CreateLoad(lengthGEP);
            // For varchar, only need to get the pointer
            dataValue = elementPtr;
        } else {
            dataValue = builder->CreateLoad(elementPtr);
        }

        // Get isNull value
        auto isNullGEP = builder->CreateGEP(isNulls, colIdx);
        Value *isNull = builder->CreateLoad(isNullGEP);

        this->value.reset(new CodeGenValue(dataValue, isNull, length));
        return;
    }

    this->value.reset(DataExprConstantHelper(dataExpr));
}

bool RowExpressionCodeGen::InitializeCodegenContext(iterator_range<Function::arg_iterator> args)
{
    this->codegenContext = std::make_unique<CodegenContext>();
    for (auto &arg : args) {
        auto argName = arg.getName().str();
        if (argName == "data") {
            codegenContext->data = &arg;
        } else if (argName == "isNulls") {
            codegenContext->nullBitmap = &arg;
        } else if (argName == "lengths") {
            codegenContext->offsets = &arg;
        } else if (argName == "executionContext") {
            codegenContext->executionContext = &arg;
        } else if (argName == "dataLength" || argName == "isResultNull") {
            continue;
        } else {
            LLVM_DEBUG_LOG("Invalid argument %s", argName.c_str());
            return false;
        }
    }

    codegenContext->print = module->getOrInsertFunction("printf",
        FunctionType::get(IntegerType::getInt32Ty(*context), PointerType::get(Type::getInt8Ty(*context), 0), true));

    return true;
}

Function *RowExpressionCodeGen::CreateFunction()
{
    int32_t argsSize = 6;
    std::vector<Type *> args;
    args.reserve(argsSize);
    // Values in args vector follow the format:
    // valueArray*, isNullArray*, lengthArray*, isResultNull*, outputLength*, executionContext
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt1PtrTy(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    args.push_back(Type::getInt1PtrTy(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    args.push_back(Type::getInt64Ty(*context));
#ifdef DEBUG_LLVM
    std::cout << "exprtree: ";
    ExprPrinter p;
    expr->Accept(p);
    std::cout << std::endl;
#endif
    FunctionType *prototype = FunctionType::get(GetFunctionReturnType(expr->GetExprDataType()), args, false);
    func = Function::Create(prototype, Function::ExternalLinkage, FUNCTION_NAME, module.get());

    std::string argNames[] = {
        "data", "isNulls", "lengths", "isResultNull",
        "dataLength", "executionContext"
    };
    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    // Generate code
    auto result = VisitExpr(*expr);
    int32_t outputLengthIndex = 4;
    // Update final output Length
    if (result->length != nullptr) {
        Argument *outputLength = func->getArg(outputLengthIndex);
        Value *lengthGep = builder->CreateGEP(outputLength, this->CreateConstantInt(0), "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    return func;
}

int64_t RowExpressionCodeGen::GetFunction()
{
    this->CreateFunction();

    OptimizeFunctionsAndModule();
#ifdef DEBUG
    module->print(errs(), nullptr);
#endif
    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;

    auto sym = eoe(jit->lookup(FUNCTION_NAME));
    return sym.getAddress();
}
