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

std::unique_ptr<RowExpressionCodeGen> RowExpressionCodeGen::Create(std::string name,
    const omniruntime::expressions::Expr &expression, omniruntime::op::OverflowConfig *overflowConfig)
{
    std::unique_ptr<RowExpressionCodeGen> codegen { new RowExpressionCodeGen(std::move(name), expression,
        overflowConfig) };
    LLVMEngine::Create(&(codegen->llvmEngine));
    codegen->context = codegen->GetContext();
    codegen->builder = codegen->GetIRBuilder();
    codegen->module = codegen->GetModule();
    codegen->jit = codegen->GetJit();
    codegen->llvmTypes = codegen->GetTypes();
    codegen->decimalIRBuilder = codegen->GetDecimalIRBuilder();
    codegen->ExtractVectorIndexes();
    return codegen;
}

void RowExpressionCodeGen::Visit(const omniruntime::expressions::LiteralExpr &literalData)
{
    this->value.reset(LiteralExprConstantHelper(literalData));
}

void RowExpressionCodeGen::Visit(const omniruntime::expressions::FieldExpr &fieldExpr)
{
    Value *data = this->codegenContext->data;
    Value *isNulls = this->codegenContext->nullBitmap;
    Value *lengths = this->codegenContext->offsets;

    Value *colIdx = llvmTypes->CreateConstantInt(fieldExpr.colVal);
    // Find address of this column in the addresses array argument.
    Value *gep = builder->CreateGEP(llvmTypes->I64Type(), data, colIdx);
    // Load the address value.
    Value *elementAddr = builder->CreateLoad(llvmTypes->I64Type(), gep);
    Value *elementPtr = GetIntToPtr(fieldExpr.GetReturnTypeId(), elementAddr);

    Type *ty = llvmTypes->VectorToLLVMType(*(fieldExpr.GetReturnType()));
    Value *dataValue = nullptr;
    Value *length = nullptr;
    if (TypeUtil::IsStringType(fieldExpr.GetReturnTypeId())) {
        // Get length for varchar/char type
        auto lengthGEP = builder->CreateGEP(llvmTypes->I32Type(), lengths, colIdx);
        length = builder->CreateLoad(llvmTypes->I32Type(), lengthGEP);
        // For varchar, only need to get the pointer
        dataValue = elementPtr;
    } else {
        dataValue = builder->CreateLoad(ty, elementPtr);
    }

    // Get isNull value
    auto isNullGEP = builder->CreateGEP(llvmTypes->I1Type(), isNulls, colIdx);
    Value *isNull = builder->CreateLoad(llvmTypes->I1Type(), isNullGEP);

    if (TypeUtil::IsDecimalType(fieldExpr.GetReturnTypeId())) {
        Value *precision = llvmTypes->CreateConstantInt(
            static_cast<DecimalDataType *>(fieldExpr.GetReturnType().get())->GetPrecision());
        Value *scale =
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fieldExpr.GetReturnType().get())->GetScale());
        this->value.reset(new DecimalValue(dataValue, isNull, precision, scale));
    } else {
        this->value.reset(new CodeGenValue(dataValue, isNull, length));
    }
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
    args.push_back(llvmTypes->I64PtrType());
    args.push_back(llvmTypes->I1PtrType());
    args.push_back(llvmTypes->I32PtrType());
    args.push_back(llvmTypes->I1PtrType());
    args.push_back(llvmTypes->I32PtrType());
    args.push_back(llvmTypes->I64Type());

    FunctionType *prototype = FunctionType::get(llvmTypes->GetFunctionReturnType(expr->GetReturnTypeId()), args, false);
    func = Function::Create(prototype, Function::ExternalLinkage, FUNCTION_NAME, module);

    std::string argNames[] = {
        "data", "isNulls", "lengths", "isResultNull",
        "dataLength", "executionContext"
    };
    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    llvmEngine->RecordMainFunction(func);

    BasicBlock *body = BasicBlock::Create(*context, "FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    // Generate code
    auto result = VisitExpr(*expr);
    if (!result->IsValidValue()) {
        return nullptr;
    }

    int32_t outputLengthIndex = 4;
    // Update final output Length
    if (result->length != nullptr) {
        Argument *outputLength = func->getArg(outputLengthIndex);
        Value *lengthGep = builder->CreateGEP(llvmTypes->I32Type(), outputLength, llvmTypes->CreateConstantInt(0),
            "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    int32_t outputNullIndex = 3;
    Argument *isResultNull = this->func->getArg(outputNullIndex);
    Value *nullGep =
        builder->CreateGEP(llvmTypes->I1Type(), isResultNull, llvmTypes->CreateConstantInt(0), "OUTPUT_NULL_ADDRESS");
    builder->CreateStore(result->isNull, nullGep);

    // Return value
    builder->CreateRet(result->data);

    llvmEngine->OptimizeModule();

    verifyFunction(*func);
    return func;
}

int64_t RowExpressionCodeGen::GetFunction()
{
#ifdef DEBUG
    std::cout << "Row Expression: " << std::endl;
    ExprPrinter p;
    expr->Accept(p);
    std::cout << std::endl;
#endif

    auto func = this->CreateFunction();
    if (func == nullptr) {
        return 0;
    }

#ifdef DEBUG_LLVM
    GetModule()->print(errs(), nullptr);
#endif
    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    llvmEngine->MakeThreadSafe(&resTracker);
    rt = resTracker;

    auto sym = eoe(jit->lookup(FUNCTION_NAME));
    return sym.getValue();
}