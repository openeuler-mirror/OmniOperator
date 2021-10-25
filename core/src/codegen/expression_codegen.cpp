/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#include "expression_codegen.h"

#include <chrono>

#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include "vector"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;
using namespace omniruntime::vec;
using std::make_shared;

namespace {
const int INT32_VALUE = 32;
const int INT64_VALUE = 64;
const int FEXPR_VALUE3 = 3;
const int FEXPR_VALUE2 = 2;
const int LENGTH_LOC = 2;
const int STARTEXT_VALUE = 2;
const int SUBSTREXT_VALUE = 3;
}

CodeGenValuePtr ExpressionCodeGen::VisitExpr(omniruntime::expressions::Expr &e)
{
    e.Accept(*this);
    return this->value;
}

Value *ExpressionCodeGen::CreateConstantBool(bool v)
{
    return ConstantInt::get(*context, APInt(1, v));
}

Value *ExpressionCodeGen::CreateConstantInt(int32_t v)
{
    return ConstantInt::get(*context, APInt(INT32_VALUE, v, true));
}

Value *ExpressionCodeGen::CreateConstantLong(int64_t v)
{
    return ConstantInt::get(*context, APInt(INT64_VALUE, v, true));
}

Value *ExpressionCodeGen::CreateConstantDouble(double v)
{
    return ConstantFP::get(*context, APFloat(v));
}


Type *ExpressionCodeGen::ToLlvmType(DataType t)
{
    switch (t) {
        case DataType::INT32D:
            return Type::getInt32Ty(*context);
        case DataType::INT64D:
            return Type::getInt64Ty(*context);
        case DataType::DOUBLED:
            return Type::getDoubleTy(*context);
        case DataType::BOOLD:
            return Type::getInt1Ty(*context);
        case DataType::STRINGD:
            return Type::getInt64Ty(*context);
        case DataType::DECIMAL128D:
            return Type::getInt64Ty(*context);
        default:
            LLVM_DEBUG_LOG("Error: Unknown argument datatype %d", t);
            return nullptr;
    }
}

Type *ExpressionCodeGen::ToPointerType(DataType type)
{
    switch (type) {
        case DataType::BOOLD:
            return Type::getInt1PtrTy(*context);
        case DataType::INT32D:
            return Type::getInt32PtrTy(*context);
        case DataType::INT64D:
            return Type::getInt64PtrTy(*context);
        case DataType::DOUBLED:
            return Type::getDoublePtrTy(*context);
        case DataType::STRINGD:
            return Type::getInt64PtrTy(*context);
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", type);
            return Type::getInt64PtrTy(*context);
    }
}

// Tells whether or not a function is a valid name
bool IsFunctionName(std::string fnName)
{
    // Either use a global string set to contain function names, or get function names from module
    return true;
}

/**
 *  Usage example: std::vector<Value *> values;
 *  values.push_back(value1);
 *  values.push_back(value2);
 *  PrintValues("LLVM DEBUG: %d, %d\n", values);
 */
void ExpressionCodeGen::PrintValues(std::string format, const std::vector<Value *>& values)
{
    auto charType = Type::getInt8Ty(*context);
    std::vector<llvm::Constant *> chars(format.size());
    for (unsigned int i = 0; i < format.size(); i++) {
        chars[i] = ConstantInt::get(charType, format[i]);
    }
    chars.push_back(llvm::ConstantInt::get(charType, 0));
    auto stringType = llvm::ArrayType::get(charType, chars.size());

    // Create the declaration statement
    auto globalDeclaration = static_cast<llvm::GlobalVariable*>(module->getOrInsertGlobal(".str", stringType));
    globalDeclaration->setInitializer(llvm::ConstantArray::get(stringType, chars));
    globalDeclaration->setConstant(true);
    globalDeclaration->setLinkage(llvm::GlobalValue::LinkageTypes::PrivateLinkage);
    globalDeclaration->setUnnamedAddr (llvm::GlobalValue::UnnamedAddr::Global);

    // Return a cast to an i8*
    auto formatPtr = llvm::ConstantExpr::getBitCast(globalDeclaration, charType->getPointerTo());
    std::vector<Value*> args;
    args.push_back(formatPtr);
    for (auto v : values) {
        args.push_back(v);
    }

    builder->CreateCall(codegenContext->print, args, "printfCall");
}

ExpressionCodeGen::ExpressionCodeGen(std::string name, Expr &cpExpr, std::vector<DataType> &datatypes)
    : datatypes(datatypes)
{
    funcName = name;
    expr = &cpExpr;
    this->datatypes = datatypes;
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    jit = eoe(LLJITBuilder().create());
    context = std::make_unique<LLVMContext>();
    // Create module called the_module
    module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(jit->getDataLayout());
    // Create IR builder to create IR instructions
    builder = std::make_unique<IRBuilder<>>(*context);

    fr = std::make_unique<FunctionRegistry>(jit, context, module).release();
    // Only register the necessary functions for the expression
    // Necessary functions are found using RequiredFunctions method
    fr->RegisterNecessaryFuncs(RequiredFunctions(cpExpr));
    funcNameToSignature = fr->funcNameToSignatureMap;
}

ExpressionCodeGen::~ExpressionCodeGen()
{
    eoe(rt->remove());
    delete fr;
}


// Goes through the expression tree and determines which functions need to be registered
// Helper function for requiredFunctions method
void ExpressionCodeGen::RequiredFunctionsHelper2(Expr &funcExpr, std::set<std::string> &s)
{
    auto *fExpr = static_cast<FuncExpr *>(&funcExpr);
    std::string fn = fExpr->funcName;

    if (fn == "CAST") {
        s.insert(fn + "_" + DataTypeString(fExpr->arguments[0]->GetExprDataType()) + "_" +
            DataTypeString(fExpr->GetExprDataType()));
    } else if (fn == "abs") {
        s.insert(fn + "_" + DataTypeString(fExpr->GetExprDataType()));
    } else if (fn == "substr") {
        if (fExpr->arguments.size() == STARTEXT_VALUE) {
            s.insert("substrWithStartExt");
        }
        if (fExpr->arguments.size() == SUBSTREXT_VALUE) {
            s.insert("substrExt");
        }
    } else if (fn == "mm3hash") {
        s.insert(fn + "_" + DataTypeString(fExpr->arguments[0]->GetExprDataType()));
    } else {
        s.insert(fExpr->funcName);
    }
    // Recurse on the arguments
    for (auto arg : fExpr->arguments) {
        RequiredFunctionsHelper(*arg, s);
    }
}

void ExpressionCodeGen::RequiredFunctionsHelper(Expr &cpExpression, std::set<std::string> &s)
{
    // For all types of Expr except for FuncExpr, recurse on the children
    Expr *cpExpr = &cpExpression;
    switch (cpExpr->GetType()) {
        case ExprType::DATA_E: {
            return;
        }

        case ExprType::BINARY_E: {
            auto *bExpr = static_cast<BinaryExpr *>(cpExpr);
            RequiredFunctionsHelper(*(bExpr->left), s);
            RequiredFunctionsHelper(*(bExpr->right), s);
            return;
        }

        case ExprType::UNARY_E: {
            RequiredFunctionsHelper(*(static_cast<UnaryExpr *>(cpExpr)->exp), s);
            return;
        }

        case ExprType::IF_E: {
            auto *ifExpr = static_cast<IfExpr *>(cpExpr);
            RequiredFunctionsHelper(*(ifExpr->condition), s);
            RequiredFunctionsHelper(*(ifExpr->trueExpr), s);
            RequiredFunctionsHelper(*(ifExpr->falseExpr), s);
            return;
        }

        case ExprType::IN_E: {
            for (auto arg : static_cast<InExpr *>(cpExpr)->arguments) {
                RequiredFunctionsHelper(*arg, s);
            }
            return;
        }

        case ExprType::BETWEEN_E: {
            auto *bExpr = static_cast<BetweenExpr *>(cpExpr);
            RequiredFunctionsHelper(*(bExpr->value), s);
            RequiredFunctionsHelper(*(bExpr->lowerBound), s);
            RequiredFunctionsHelper(*(bExpr->upperBound), s);
            return;
        }

        case ExprType::COALESCE_E: {
            auto *cExpr = static_cast<CoalesceExpr *>(cpExpr);
            RequiredFunctionsHelper(*(cExpr->value1), s);
            RequiredFunctionsHelper(*(cExpr->value2), s);
            return;
        }

        case ExprType::IS_NULL_E: {
            auto *isNullExpr = static_cast<IsNullExpr *>(cpExpr);
            RequiredFunctionsHelper(*(isNullExpr->value), s);
            return;
        }

        // Add the name of the required extern function
        case ExprType::FUNC_E: {
            RequiredFunctionsHelper2(*cpExpr, s);
            return;
        }

        default:
            return;
    }
}

// Returns a set of the names of required functions
std::set<std::string> ExpressionCodeGen::RequiredFunctions(Expr &cpExpr)
{
    std::set<std::string> ret;
    RequiredFunctionsHelper(cpExpr, ret);
    return ret;
}

// Other operations which require externed functions
Value *ExpressionCodeGen::StringCmp(Value *lhs, Value *lLen, Value *rhs, Value *rLen)
{
    // call function
    std::vector<Value *> argVals {lhs, lLen, rhs, rLen};
    auto f = module->getFunction(fr->strCompareExtStr);
    Value *ret = builder->CreateCall(f, argVals, "call_str_cmp");
    return ret;
}

// Other operations which require externed functions
Value *ExpressionCodeGen::Decimal128Cmp(const Value &lhs, const Value &rhs)
{
    // call function
    std::vector<Value*> argVals;
    argVals.push_back(const_cast<Value*>(&lhs));
    argVals.push_back(const_cast<Value*>(&rhs));
    auto f = module->getFunction(fr->decimal128CompareExtStr);
    Value *ret = builder->CreateCall(f, argVals, "call_decimal_cmp");
    return ret;
}

// Helper methods to parse binary expressions
Value *ExpressionCodeGen::BinaryExprIntHelper(omniruntime::expressions::Operator op, Value *left, Value *right)
{
    switch (op) {
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
        default:
            std::cout << "Unsupported int/long binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::BinaryExprDoubleHelper(omniruntime::expressions::Operator op,
                                                 Value *left, Value *right)
{
    switch (op) {
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
        default:
            std::cout << "Unsupported double binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::BinaryExprStringHelper(omniruntime::expressions::Operator op,
                                                 Value *leftVal, Value *leftLen, Value *rightVal, Value *rightLen)
{
    switch (op) {
        case LT:
            return builder->CreateICmpSLT(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        case GT:
            return builder->CreateICmpSGT(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        case LTE:
            return builder->CreateICmpSLE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        case GTE:
            return builder->CreateICmpSGE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        case EQ:
            return builder->CreateICmpEQ(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        case NEQ:
            return builder->CreateICmpNE(this->StringCmp(leftVal, leftLen, rightVal, rightLen), CreateConstantInt(0));
        default:
            std::cout << "Unsupported string binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

Value *ExpressionCodeGen::BinaryExprDecimalHelper(
    omniruntime::expressions::Operator op, Value *left, Value *right)
{
    std::vector<Value*> argVals {left, right};

    switch (op) {
        case LT:
            return builder->CreateICmpSLT(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case GT:
            return builder->CreateICmpSGT(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case LTE:
            return builder->CreateICmpSLE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case GTE:
            return builder->CreateICmpSGE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case EQ:
            return builder->CreateICmpEQ(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case NEQ:
            return builder->CreateICmpNE(this->Decimal128Cmp(*left, *right), CreateConstantInt(0));
        case ADD:
            return builder->CreateCall(module->getFunction(fr->addDec128Str), argVals, fr->addDec128Str);
        case SUB:
            return builder->CreateCall(module->getFunction(fr->subDec128Str), argVals, fr->subDec128Str);
        case MUL:
            return builder->CreateCall(module->getFunction(fr->mulDec128Str), argVals, fr->mulDec128Str);
        case DIV:
            return builder->CreateCall(module->getFunction(fr->divDec128Str), argVals, fr->divDec128Str);
        default:
            std::cout << "Unsupported string binary operator " << op << std::endl;
            return this->CreateConstantBool(false);
    }
}

// Helper functions for parsing functions
void ExpressionCodeGen::FuncExprAbsHelper(FuncExpr &fExpr)
{
    std::string absFuncName = "Abs_" + DataTypeString(fExpr.dataType);
    std::vector<Value *> argVals { VisitExpr(*(fExpr.arguments[0]))->data };
    auto f = module->getFunction(absFuncName);
    Value *ret = nullptr;
    if (f) {
        ret = builder->CreateCall(f, argVals, absFuncName);
    } else {
        std::cout << "Unable to parse function " << absFuncName << std::endl;
        ret = CreateConstantInt(0);
    }
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false));
}

void ExpressionCodeGen::FuncExprLikeHelper(omniruntime::expressions::FuncExpr &fExpr)
{
    Value *str = VisitExpr(*(fExpr.arguments[0]))->data;
    Value *strLen = this->value->length;
    Value *regex = VisitExpr(*(fExpr.arguments[1]))->data;
    Value *regexLen = this->value->length;
    std::vector<Value *> argVals { str, strLen, regex, regexLen };
    auto f = module->getFunction(fr->likeExtStr);
    Value *ret = builder->CreateCall(f, argVals, fr->likeExtStr);
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false));
}

void ExpressionCodeGen::FuncExprCastHelper(FuncExpr &fExpr)
{
    llvm::Value *val = VisitExpr(*(fExpr.arguments[0]))->data;
    std::vector<Value *> argVals { val };
    DataType from = fExpr.arguments[0]->dataType;
    DataType to = fExpr.GetExprDataType();

    std::string castFuncName = "Cast_" + DataTypeString(from) + "_" + DataTypeString(to);

    AllocaInst *outputLenPtr = nullptr;
    Value *outputLen = nullptr;
    Value *ret = nullptr;
    // if casting to same type, treat it as constant
    if (from == to) {
        auto *dataExpr = static_cast<DataExpr *>(fExpr.arguments[0]);
        Value *ret = VisitExpr(*dataExpr)->data;
        Value *length = this->value->length;
        this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false), length);
        return;
    } else if (from == DataType::STRINGD) {
        argVals.push_back(this->value->length);
    } else if (to == DataType::STRINGD) {
        AllocaInst *outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
        argVals.push_back(outputLenPtr);
    }

    auto f = module->getFunction(castFuncName);
    if (f) {
        ret = builder->CreateCall(f, argVals, castFuncName);
        outputLen = (outputLenPtr == nullptr) ? nullptr : builder->CreateLoad(outputLenPtr);
    } else {
        LLVM_DEBUG_LOG("Unable to parse function %s", castFuncName.c_str());
        ret = CreateConstantInt(0);
        outputLen = nullptr;
    }
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false), outputLen);
}

void ExpressionCodeGen::FuncExprConcatHelper(omniruntime::expressions::FuncExpr &fExpr)
{
    auto str1Value = VisitExpr(*(fExpr.arguments[0]));
    Value *str1 = str1Value->data;
    Value *str1Len = str1Value->length;
    auto str2Value = VisitExpr(*(fExpr.arguments[1]));
    Value *str2 = str2Value->data;
    Value *str2Len = str2Value->length;
    AllocaInst *outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
    std::vector<Value *> argVals { str1, str1Len, str2, str2Len, outputLenPtr };

    auto f = module->getFunction(fr->concatStrExtStr);
    Value *ret = builder->CreateCall(f, argVals, fr->concatStrExtStr);
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false), builder->CreateLoad(outputLenPtr));
}

void ExpressionCodeGen::FuncExprSubstrHelper(FuncExpr &fExpr)
{
    if (fExpr.arguments.size() == FEXPR_VALUE3) {
        auto strValue = VisitExpr(*(fExpr.arguments[0]));
        Value *str = strValue->data;
        Value *strLen = strValue->length;
        Value *startIdx = VisitExpr(*(fExpr.arguments[1]))->data;
        Value *length = VisitExpr(*(fExpr.arguments[LENGTH_LOC]))->data;
        AllocaInst *outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
        std::vector<Value*> argVals { str, strLen, startIdx, length, outputLenPtr};
        auto f = module->getFunction(fr->substrExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->substrExtStr);
        this->value = make_shared<CodeGenValue>(
            ret, this->CreateConstantBool(false), builder->CreateLoad(outputLenPtr));
        return;
    }
    if (fExpr.arguments.size() == FEXPR_VALUE2) {
        Value *str = VisitExpr(*(fExpr.arguments[0]))->data;
        Value *strLen = this->value->length;
        Value *startIdx = VisitExpr(*(fExpr.arguments[1]))->data;
        AllocaInst *outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "output_len");
        std::vector<Value *> argVals { str, strLen, startIdx, outputLenPtr};

        auto f = module->getFunction(fr->substrWithStartExtStr);
        Value *ret = builder->CreateCall(f, argVals, fr->substrWithStartExtStr);
        this->value = make_shared<CodeGenValue>(
            ret, this->CreateConstantBool(false), builder->CreateLoad(outputLenPtr));
        return;
    }
    std::cout << "Error: Incorrect number of arguments used for substr" << std::endl;
    this->value = make_shared<CodeGenValue>(CreateConstantLong(0), this->CreateConstantBool(false));
}

void ExpressionCodeGen::FuncExprExtHelper(FuncExpr &fExpr)
{
    FunctionSignature fs = funcNameToSignature[fExpr.funcName];

    std::vector<Value *> argVals;
    for (int i = 0; i < fExpr.arguments.size(); i++) {
        // Cast arguments to the correct type
        DataType desiredType = fs.GetParams()[i];
        DataType currType = fExpr.arguments[i]->GetExprDataType();
        if (desiredType == DOUBLED && (currType == INT32D || currType == INT64D)) {
            Value *argDouble = builder->CreateCast(Instruction::SIToFP,
                VisitExpr(*(fExpr.arguments[i]))->data, Type::getDoubleTy(*(context)));
            argVals.push_back(argDouble);
        } else if (desiredType == INT64D && currType == INT32D) {
            Value *argInt64 =
                builder->CreateIntCast(VisitExpr(*(fExpr.arguments[i]))->data, Type::getInt64Ty(*context), true);
            argVals.push_back(argInt64);
        } else {
            argVals.push_back(VisitExpr(*(fExpr.arguments[i]))->data);
        }
    }
    // Assume that the function name of the fExpr and in the module are matching
    auto f = module->getFunction(fExpr.funcName);
    Value *ret = builder->CreateCall(f, argVals, "call_" + fExpr.funcName);
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false));
}

void ExpressionCodeGen::FuncExprMm3HashHelper(FuncExpr &fExpr)
{
    llvm::Value *val = VisitExpr(*(fExpr.arguments[0]))->data;
    llvm::Value *seed = VisitExpr(*(fExpr.arguments[1]))->data;
    std::string mm3FuncName = "Mm3_" + DataTypeString(fExpr.arguments[0]->dataType);
    std::vector<Value*> argVals {val, seed};

    auto f = module->getFunction(mm3FuncName);
    Value *ret = nullptr;
    if (f) {
        ret = builder->CreateCall(f, argVals, mm3FuncName);
    } else {
        LLVM_DEBUG_LOG("Unable to parse function %s", mm3FuncName.c_str());
        ret = CreateConstantInt(0);
    }
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false));
}

void ExpressionCodeGen::FuncExprCombineHashHelper(FuncExpr &fExpr)
{
    Value *prevHashVal = VisitExpr(*(fExpr.arguments[0]))->data;
    Value *val = VisitExpr(*(fExpr.arguments[1]))->data;
    if (fExpr.arguments[0]->GetExprDataType() == INT32D) {
        prevHashVal = builder->CreateIntCast(prevHashVal, Type::getInt64Ty(*context), true);
    }
    if (fExpr.arguments[1]->GetExprDataType() == INT32D) {
        val = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
    }
    std::vector<Value *> argVals { prevHashVal, val };
    auto f = module->getFunction(fr->combineHashStr);
    Value *ret = builder->CreateCall(f, argVals, fr->combineHashStr);
    DumpCode();
    this->value = make_shared<CodeGenValue>(ret, this->CreateConstantBool(false));
}

std::string ExpressionCodeGen::DumpCode()
{
    std::string ir;
    llvm::raw_string_ostream stream(ir);
    module->print(stream, nullptr);
    std::cout << " Generated code::" << ir;
    return ir;
}

void AddOptimizationPasses(legacy::FunctionPassManager *fpm, llvm::legacy::PassManager &mpm)
{
    fpm->add(createSCCPPass());
    fpm->add(createNewGVNPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());

    fpm->add(createLICMPass());
    fpm->add(createLoopUnrollPass());
    fpm->add(createLoopUnswitchPass());

    fpm->add(createLoopLoadEliminationPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());
    fpm->add(createLoopInstSimplifyPass());
    fpm->add(createLoopSimplifyCFGPass());
    fpm->add(createMergedLoadStoreMotionPass());
    fpm->add(createMergeICmpsLegacyPass());
    fpm->add(createAggressiveDCEPass());
    fpm->add(createDeadStoreEliminationPass());

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());
}

bool ExpressionCodeGen::InitializeCodegenContext(iterator_range<Function::arg_iterator> args)
{
    this->codegenContext = std::make_unique<CodegenContext>();
    for (auto &arg : args) {
        auto argName = arg.getName().str();
        if (argName == "data") {
            codegenContext->data = &arg;
        } else if (argName == "nullBitmap") {
            codegenContext->nullBitmap = &arg;
        } else if (argName == "offsets") {
            codegenContext->offsets = &arg;
        } else if (argName == "rowIdx") {
            codegenContext->rowIdx = &arg;
        } else if (argName == "isResultNull") {
            codegenContext->isResultNull = &arg;
        } else if (argName == "dataLength") {
            continue;;
        } else {
            LLVM_DEBUG_LOG("Invalid argument %s", argName.c_str());
            return false;
        }
    }

    codegenContext->print = module->getOrInsertFunction("printf",
        FunctionType::get(IntegerType::getInt32Ty(*context), PointerType::get(Type::getInt8Ty(*context), 0), true)
    );

    return true;
}

Function *ExpressionCodeGen::CreateFunction()
{
    int32_t argsSize = 6;
    std::vector<Type *> args;
    args.reserve(argsSize);
    // Values in args vector follow the format:
    // value*, bitmap*, offset*, rowIdx, isResultNull*, outputLength*
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt32Ty(*context));
    args.push_back(Type::getInt1PtrTy(*context));
    args.push_back(Type::getInt32PtrTy(*context));

#ifdef DEBUG_LLVM
    std::cout << "exprtree: ";
    ExprPrinter p;
    expr->Accept(p);
    std::cout << std::endl;
#endif
    FunctionType *prototype = FunctionType::get(this->ToLlvmType(expr->GetExprDataType()), args, false);
    func = Function::Create(prototype, Function::ExternalLinkage, funcName, module.get());

    std::string argNames[] = {"data", "nullBitmap", "offsets", "rowIdx", "isResultNull", "dataLength"};
    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    // Generate code
    auto result = VisitExpr(*expr);
    int32_t outputLengthIndex = 5;
    // Update final output Length
    if (result->length != nullptr) {
        Argument *outputLength = func->getArg(outputLengthIndex);
        Value *lengthGep = builder->CreateGEP(outputLength, this->CreateConstantInt(0), "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    auto fpm = std::make_unique<legacy::FunctionPassManager>(module.get());
    llvm::legacy::PassManager mpm;
    AddOptimizationPasses(fpm.get(), mpm);
    fpm->doInitialization();
    for (auto &F : *module)
        fpm->run(F);
    mpm.run(*module);
    return func;
}

Value* ExpressionCodeGen::GetIntToPtr(DataExpr &dExpr, llvm::Value *elementAddr)
{
    Value *elementPtr = nullptr;
    DataExpr *dEx = &dExpr;
    // Convert the column address to array of proper datatype.
    switch (dEx->GetExprDataType()) {
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
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt8PtrTy(*context));
            break;
        case DataType::DECIMAL128D:
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
            break;
        default:
            LLVM_DEBUG_LOG("Unsupported column data type %d", dEx->GetExprDataType());
            elementPtr = builder->CreateIntToPtr(elementAddr, Type::getInt64PtrTy(*context));
            break;
    }
    return elementPtr;
}


CodeGenValue *ExpressionCodeGen::DataExprConstantHelper(DataExpr &dExpr)
{
    DataExpr *dEx = &dExpr;
    CodeGenValue *codeGenValue = nullptr;
    switch (dEx->GetExprDataType()) {
        case DataType::INT32D: {
            codeGenValue = new CodeGenValue(this->CreateConstantInt(dEx->intVal), this->CreateConstantBool(false));
            break;
        }
        case DataType::INT64D: {
            codeGenValue = new CodeGenValue(this->CreateConstantLong(dEx->longVal), this->CreateConstantBool(false));
            break;
        }
        case DataType::DOUBLED: {
            codeGenValue = new CodeGenValue(
                this->CreateConstantDouble(dEx->doubleVal), this->CreateConstantBool(false));
            break;
        }
        case DataType::STRINGD: {
            Constant *strValConst =
                    ConstantInt::get(*context, APInt(INT64_VALUE, reinterpret_cast<int64_t>(dEx->stringVal->c_str())));
            Constant *strLenConst =
                    ConstantInt::get(*context, APInt(INT32_VALUE, static_cast<int32_t>(dEx->stringVal->length())));
            codeGenValue = new CodeGenValue(strValConst, this->CreateConstantBool(false), strLenConst);
            break;
        }
        case DataType::BOOLD: {
            codeGenValue = new CodeGenValue(this->CreateConstantBool(dEx->boolVal), this->CreateConstantBool(false));
            break;
        }
        case DataType::DECIMAL128D: {
            int32_t length = 2;
            Decimal128 decValue = dEx->longVal;
            auto decimal = std::make_unique<int64_t[]>(length).release();

            decimal[0] = decValue.LowBits();
            decimal[1] = decValue.HighBits();

            Constant *addr = ConstantInt::get(*context, APInt(INT64_VALUE,
                                                              reinterpret_cast<int64_t>(decimal)));
            codeGenValue = new CodeGenValue(addr, this->CreateConstantBool(false));
            break;
        }
        default: {
            LLVM_DEBUG_LOG("Unsupported data type in Data Expr %d", dEx->GetExprDataType());
            codeGenValue = new CodeGenValue(this->CreateConstantBool(dEx->boolVal), this->CreateConstantBool(false));
            break;
        }
    }
    return codeGenValue;
}


void ExpressionCodeGen::Visit(DataExpr &dExpr)
{
    DataExpr *dEx = &dExpr;

    if (dEx->isColumn) {
        Value *rowIdx = this->codegenContext->rowIdx;
        Value *vecBatch = this->codegenContext->data;
        Value *bitmap = this->codegenContext->nullBitmap;
        Value *offsets = this->codegenContext->offsets;
        Value *isResultNull = this->codegenContext->isResultNull;

        Value *colIdx = this->CreateConstantInt(dEx->colVal);
        // Find address of this column in the addresses array argument.
        Value *gep = builder->CreateGEP(vecBatch, colIdx);

        // Load the address value.
        Value *elementAddr = builder->CreateLoad(gep);

        Value *elementPtr = GetIntToPtr(dExpr, elementAddr);

        Value *elementValue = nullptr;
        Value *length = nullptr;
        if (dEx->GetExprDataType() == DataType::STRINGD) {
            // Get offset for varchar
            auto offsetsGEP = builder->CreateGEP(offsets, colIdx);
            Value *offsetPtr = builder->CreateLoad(offsetsGEP);
            offsetPtr = builder->CreateIntToPtr(offsetPtr, Type::getInt32PtrTy(*context));
            auto colOffsetGEP = builder->CreateGEP(offsetPtr, rowIdx);
            Value *startOffset = builder->CreateLoad(colOffsetGEP);
            // Find the address of the row to be processed.
            gep = builder->CreateGEP(elementPtr, startOffset);
            elementValue = builder->CreatePtrToInt(gep, Type::getInt64Ty(*context));

            colOffsetGEP = builder->CreateGEP(offsetPtr, builder->CreateAdd(rowIdx, CreateConstantInt(1)));
            Value *endOffset = builder->CreateLoad(colOffsetGEP);
            // Get length for varchar
            length = builder->CreateSub(endOffset, startOffset);
        } else {
            // Find the address of the row to be processed.
            gep = builder->CreateGEP(elementPtr, rowIdx);
            // Value to be processed.
            elementValue = builder->CreateLoad(gep);
        }

        // Get isNull value
        auto bitmapGEP = builder->CreateGEP(bitmap, colIdx);
        Value *bitmapValue = builder->CreateLoad(bitmapGEP);
        bitmapValue = builder->CreateIntToPtr(bitmapValue, Type::getInt1PtrTy(*context));
        bitmapGEP = builder->CreateGEP(bitmapValue, rowIdx);
        bitmapValue = builder->CreateLoad(bitmapGEP);

        isResultNull = builder->CreateLoad(isResultNull);
        isResultNull = builder->CreateOr(isResultNull, bitmapValue);
        builder->CreateStore(isResultNull, this->codegenContext->isResultNull);

        this->value.reset(new CodeGenValue(elementValue, bitmapValue, length));
        return;
    }

    this->value.reset(DataExprConstantHelper(dExpr));
}

void ExpressionCodeGen::Visit(BinaryExpr &binaryExpr)
{
    BinaryExpr *bExpr = &binaryExpr;

    if (bExpr->left->GetType() == ExprType::DATA_E || bExpr->right->GetType() == ExprType::DATA_E) {
        DataType biggerType = std::max(bExpr->left->GetExprDataType(), bExpr->right->GetExprDataType());
        bExpr->left->dataType = biggerType;
        bExpr->right->dataType = biggerType;
    }
    CodeGenValuePtr left = VisitExpr(*(bExpr->left));
    Value *leftValue = left->data;
    Value *leftLen = left->length;
    CodeGenValuePtr right = VisitExpr(*(bExpr->right));
    Value *rightValue = right->data;
    Value *rightLen = right->length;

    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        this->value = make_shared<CodeGenValue>(
            builder->CreateAnd(leftValue, rightValue, "logical_and"), this->CreateConstantBool(false));
        return;
    }
    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        this->value = make_shared<CodeGenValue>(
            builder->CreateOr(leftValue, rightValue, "logical_or"), this->CreateConstantBool(false));
        return;
    }

    if (bExpr->left->GetExprDataType() == DataType::INT32D || bExpr->left->GetExprDataType() == DataType::INT64D) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprIntHelper(bExpr->op, leftValue, rightValue), this->CreateConstantBool(false));
        return;
    } else if (bExpr->left->GetExprDataType() == DOUBLED) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprDoubleHelper(bExpr->op, leftValue, rightValue), this->CreateConstantBool(false));
        return;
    } else if (bExpr->left->GetExprDataType() == STRINGD) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprStringHelper(bExpr->op, leftValue, leftLen, rightValue, rightLen), this->CreateConstantBool(false));
        return;
    } else if (bExpr->left->GetExprDataType() == DECIMAL128D) {
        this->value = make_shared<CodeGenValue>(
            this->BinaryExprDecimalHelper(bExpr->op, leftValue, rightValue), this->CreateConstantBool(false));
        return;
    }
    LLVM_DEBUG_LOG("Unsupported binary operator %d", bExpr->op);
    this->value = make_shared<CodeGenValue>(this->CreateConstantBool(false), this->CreateConstantBool(false));
}

void ExpressionCodeGen::Visit(UnaryExpr &uExpr)
{
    Value *val = VisitExpr(*(uExpr.exp))->data;
    switch (uExpr.op) {
        case NOT: {
            Value *notValue = builder->CreateNot(val, "logical_not");
            this->value = make_shared<CodeGenValue>(notValue, this->CreateConstantBool(false));
            break;
        }
        default: {
            // ignore the unary operator if it is invalid
            this->value = make_shared<CodeGenValue>(val, this->CreateConstantBool(false));
            break;
        }
    }
}

void ExpressionCodeGen::Visit(IfExpr &ifExpr)
{
    Expr *cond = ifExpr.condition;
    Expr *ifTrue = ifExpr.trueExpr;
    Expr *ifFalse = ifExpr.falseExpr;

    BasicBlock *trueBlock = BasicBlock::Create(*context, "TRUE_BLOCK", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "FALSE_BLOCK");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");

    CodeGenValuePtr evCond = VisitExpr(*cond);

    // If cond evaluates to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(evCond->data, trueBlock, falseBlock);
    builder->SetInsertPoint(trueBlock);
    auto evTrue = VisitExpr(*ifTrue);
    Value *evTrueValue = evTrue->data;
    Value *evTrueLength = evTrue->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'true' can change the current block, update trueBlock for the PHI.
    trueBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(falseBlock);
    builder->SetInsertPoint(falseBlock);
    auto evFalse = VisitExpr(*ifFalse);
    Value *evFalseValue = evFalse->data;
    Value *evFalseLength = evFalse->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'false' can change the current block, update falseBlock for the PHI.
    falseBlock = builder->GetInsertBlock();
    int32_t numReservedValues = 2;
    // Emit merge block.
    Type *phiType = this->ToLlvmType(ifExpr.GetExprDataType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");

    pn->addIncoming(evTrueValue, trueBlock);
    pn->addIncoming(evFalseValue, falseBlock);

    PHINode *lengthPhi = nullptr;
    if (ifExpr.GetExprDataType() == STRINGD) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(evTrueLength, trueBlock);
        lengthPhi->addIncoming(evFalseLength, falseBlock);
    }

    this->value = make_shared<CodeGenValue>(pn, this->CreateConstantBool(false), lengthPhi);
}

void ExpressionCodeGen::Visit(InExpr &inExpr)
{
    InExpr *iExpr = &inExpr;
    Expr *toCompare = iExpr->arguments[0];
    llvm::Value *val = VisitExpr(*toCompare)->data;
    llvm::Value *valLen = this->value->length;

    llvm::Value *inArray = llvm::ConstantInt::get(*context, APInt(1, 0));
    // Handle types correctly
    for (int i = 1; i < iExpr->arguments.size(); i++) {
        // initialize tmpCmp
        llvm::Value *tmpCmp = this->CreateConstantBool(false);

        if (toCompare->GetExprDataType() == DataType::INT64D &&
            iExpr->arguments[i]->GetExprDataType() == DataType::INT32D) {
            Value *argInt64 =
                builder->CreateIntCast(VisitExpr(*(iExpr->arguments[i]))->data, Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(val, argInt64);
        } else if (toCompare->GetExprDataType() == DataType::INT32D &&
            iExpr->arguments[i]->GetExprDataType() == DataType::INT64D) {
            Value *valInt64 = builder->CreateIntCast(val, Type::getInt64Ty(*context), true);
            tmpCmp = builder->CreateICmpEQ(valInt64, VisitExpr(*(iExpr->arguments[i]))->data);
        } else {
            switch (iExpr->arguments[0]->dataType) {
                case INT32D:
                case INT64D: {
                    tmpCmp = builder->CreateICmpEQ(val, VisitExpr(*(iExpr->arguments[i]))->data);
                    break;
                }
                case DOUBLED: {
                    tmpCmp = builder->CreateFCmpOEQ(val, VisitExpr(*(iExpr->arguments[i]))->data);
                    break;
                }
                case STRINGD: {
                    tmpCmp = builder->CreateICmpEQ(this->StringCmp(val, valLen, VisitExpr(*(iExpr->arguments[i]))->data,
                                                                   this->value->length),
                        CreateConstantInt(0));
                    break;
                }
                default: {
                    LLVM_DEBUG_LOG("Unsupported data type in IN expr %d", iExpr->arguments[0]->dataType);
                    tmpCmp = this->CreateConstantBool(false);
                }
            }
        }

        inArray = builder->CreateOr(inArray, tmpCmp);
    }
    this->value = make_shared<CodeGenValue>(inArray, this->CreateConstantBool(false));
}

void ExpressionCodeGen::Visit(BetweenExpr &btExpr)
{
    BetweenExpr *bExpr = &btExpr;
    DataType biggerType = std::max(std::max(bExpr->lowerBound->GetExprDataType(), bExpr->upperBound->GetExprDataType()),
        bExpr->value->GetExprDataType());
    bExpr->lowerBound->dataType = biggerType;
    bExpr->upperBound->dataType = biggerType;
    bExpr->value->dataType = biggerType;

    llvm::Value *val = VisitExpr(*(bExpr->value))->data;
    llvm::Value *valLen = this->value->length;
    llvm::Value *lowerVal = VisitExpr(*(bExpr->lowerBound))->data;
    llvm::Value *lowerValLen = this->value->length;
    llvm::Value *upperVal = VisitExpr(*(bExpr->upperBound))->data;
    llvm::Value *upperValLen = this->value->length;

    if (bExpr->value->GetExprDataType() == DataType::INT32D || bExpr->value->GetExprDataType() == DataType::INT64D) {
        llvm::Value *cmpLeft = builder->CreateICmpSLE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpRight = builder->CreateICmpSLE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpLeft, cmpRight, "between_and");
        this->value = make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
        return;
    } else if (bExpr->value->GetExprDataType() == DOUBLED) {
        llvm::Value *cmpLeft = builder->CreateFCmpULE(lowerVal, val, "between_cmpleft");
        llvm::Value *cmpRight = builder->CreateFCmpULE(val, upperVal, "between_cmpright");
        llvm::Value *result = builder->CreateAnd(cmpLeft, cmpRight, "between_and");
        this->value = make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
        return;
    } else if (bExpr->value->GetExprDataType() == STRINGD) {
        llvm::Value *cmpLeft =
            builder->CreateICmpSLE(this->StringCmp(lowerVal, lowerValLen, val, valLen), CreateConstantInt(0));
        llvm::Value *cmpRight =
            builder->CreateICmpSLE(this->StringCmp(val, valLen, upperVal, upperValLen), CreateConstantInt(0));
        llvm::Value *result = builder->CreateAnd(cmpLeft, cmpRight, "between_and");
        this->value = make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
        return;
    } else if (bExpr->value->GetExprDataType() == DECIMAL128D) {
        llvm::Value *cmpLeft = builder->CreateICmpSLE(this->Decimal128Cmp(*lowerVal, *val), CreateConstantInt(0));
        llvm::Value *cmpRight = builder->CreateICmpSLE(this->Decimal128Cmp(*val, *upperVal), CreateConstantInt(0));
        llvm::Value *result = builder->CreateAnd(cmpLeft, cmpRight, "between_and");
        this->value = make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
        return;
    }

    LLVM_DEBUG_LOG("Error: unsupported data type for between %d", bExpr->value->GetExprDataType());
    this->value = make_shared<CodeGenValue>(this->CreateConstantBool(false), this->CreateConstantBool(false));
}

void ExpressionCodeGen::Visit(CoalesceExpr &cExpr)
{
    Expr *value1Expr = cExpr.value1;
    Expr *value2Expr = cExpr.value2;

    CodeGenValuePtr value1 = VisitExpr(*value1Expr);

    BasicBlock *isNullBlock = BasicBlock::Create(*context, "coalesceVal1IsNull", func);
    BasicBlock *isNotNullBlock = BasicBlock::Create(*context, "coalesceVal1IsNotNull");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "coalesceCont");

    // If cond evaluates to true, control flow goes to trueBlock, returning evTrue
    // Otherwise goes to falseBlock and returns evFalse
    builder->CreateCondBr(builder->CreateOr(builder->CreateLoad(this->codegenContext->isResultNull), value1->isNull),
        isNullBlock, isNotNullBlock);

    builder->SetInsertPoint(isNullBlock);
    auto value2 = VisitExpr(*value2Expr);
    Value *value2Data = value2->data;
    Value *value2Length = value2->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'true' can change the current block, update trueBlock for the PHI.
    isNullBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(isNotNullBlock);
    builder->SetInsertPoint(isNotNullBlock);
    Value *value1Data = value1->data;
    Value *value1Length = value1->length;
    builder->CreateBr(mergeBlock);
    // Codegen of 'false' can change the current block, update falseBlock for the PHI.
    isNotNullBlock = builder->GetInsertBlock();
    int32_t numReservedValues = 2;
    if (value2Expr->GetExprDataType() == INT32D && value1Expr->GetExprDataType() == INT64D) {
        value2Data = builder->CreateIntCast(value2Data, Type::getInt64Ty(*context), true);
    }
    if ((value2Expr->GetExprDataType() == INT32D || value2Expr->GetExprDataType() == INT64D) &&
        value1Expr->GetExprDataType() == DOUBLED) {
        value2Data = builder->CreateCast(Instruction::SIToFP, value2Data, Type::getDoubleTy(*context));
    }

    // Emit merge block.
    Type *phiType = this->ToLlvmType(cExpr.GetExprDataType());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);
    PHINode *pn = builder->CreatePHI(phiType, numReservedValues, "iftmp");

    PHINode *lengthPhi = nullptr;
    if (cExpr.GetExprDataType() == STRINGD) {
        lengthPhi = builder->CreatePHI(Type::getInt32Ty(*context), numReservedValues, "length");
        lengthPhi->addIncoming(value1Length, isNotNullBlock);
        lengthPhi->addIncoming(value2Length, isNullBlock);
    }

    pn->addIncoming(value1Data, isNotNullBlock);
    pn->addIncoming(value2Data, isNullBlock);

    this->value = make_shared<CodeGenValue>(pn, this->CreateConstantBool(false), lengthPhi);
}

void ExpressionCodeGen::Visit(IsNullExpr &isNullExpr)
{
    Expr *valueExpr = isNullExpr.value;
    Value *isNullValue = VisitExpr(*valueExpr)->isNull;

    Value *result = builder->CreateICmpEQ(isNullValue, CreateConstantBool(true), "isNullCompare");
    this->value =  make_shared<CodeGenValue>(result, this->CreateConstantBool(false));
}

// Handles all functions
// Only calls them; registration is done in function registry
void ExpressionCodeGen::Visit(FuncExpr &fExpr)
{
    if (fExpr.funcName == "abs") {
        this->FuncExprAbsHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "substr") {
        this->FuncExprSubstrHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "concat") {
        this->FuncExprConcatHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "CAST") {
        this->FuncExprCastHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "LIKE") {
        this->FuncExprLikeHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "combine_hash") {
        this->FuncExprCombineHashHelper(fExpr);
        return;
    }
    if (fExpr.funcName == "mm3hash") {
        this->FuncExprMm3HashHelper(fExpr);
        return;
    }
    // external functions
    if (IsFunctionName(fExpr.funcName)) {
        this->FuncExprExtHelper(fExpr);
        return;
    }
    LLVM_DEBUG_LOG("No function found with name %s", fExpr.funcName.c_str());
    this->value = make_shared<CodeGenValue>(CreateConstantInt(0), this->CreateConstantBool(false));
}
