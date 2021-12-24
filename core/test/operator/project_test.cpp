/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/projection/projection.h"
#include "../../src/vector/vector_helper.h"
#include <string>
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
namespace project_test {

VectorBatch* CreateInput(const int32_t numRows,
                         const int32_t numCols,
                         const int32_t* inputTypeIds,
                         int64_t* allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_SHORT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                for (int j = 0; j < numRows; ++j) {
                    // std::cout << "row: " << j << std::endl;
                    int64_t addr = ((int64_t *)(allData[i]))[j];
                    std::string s ((char *)(addr));
                    // std::cout << "s: " << s << std::endl;
                    ((VarcharVector *)vecBatch->GetVector(i))->SetValue(j, reinterpret_cast<const uint8_t *>(s.c_str()),
                                                                        s.length());
                }
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            default: {
                LogError("No such data type %d", inputTypeIds[i]);
                break;
            }
        }

    }
    return vecBatch;
}

int32_t* MakeInts(const int32_t size, const int32_t start = 0)
{
    auto* arr = new int32_t[size];
    int32_t idx = 0;
    for (int32_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

int64_t* MakeDecimals(const int32_t size, const int32_t start = 0)
{
    auto* arr = new int64_t[size * 2];
    int32_t idx = 0;
    for (int64_t i = start; i < start + size; i++) {
        if (i >= 0) {
            arr[2 * idx] = i;
            arr[2 * idx + 1] = 0;
        } else {
            arr[2 * idx] = i * -1;
            arr[2 * idx + 1] = -1;
        }
        idx++;
    }
    return arr;
}

int64_t* MakeLongs(const int32_t size, const int64_t start = 0)
{
    auto* arr = new int64_t[size];
    int32_t idx = 0;
    for (int64_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

double* MakeDoubles(const int32_t size, const double start = 0)
{
    auto* arr = new double[size];
    int32_t idx = 0;
    for (double i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

TEST(ProjectTest, Cast)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    int32_t *col2 = MakeInts(numRows);
    const int32_t numCols = 2;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_INT) };
    DataExpr *data1 = new DataExpr(0, INT64D);
    std::vector<Expr *> args1;
    args1.push_back(data1);
    FuncExpr *castExpr1 = new FuncExpr("CAST", args1, INT32D);

    DataExpr *data2 = new DataExpr(1, INT32D);
    std::vector<Expr *> args2;
    args2.push_back(data2);
    FuncExpr *castExpr2 = new FuncExpr("CAST", args2, INT64D);

    std::vector<Expr*> exprs = {castExpr1, castExpr2};

    VecTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
        t->GetVector(1)->SetValueNotNull(i);
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (ProjectTest, Simple) {
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows);
    const int32_t numCols = 1;
    DataExpr *addLeft = new DataExpr(0, INT32D);
    DataExpr *addRight = new DataExpr(5);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);
    std::vector<Expr*> exprs = {addExpr};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes inputTypes(vecOfTypes);
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNotNull(i);
        } else {
            t->GetVector(0)->SetValueNull(i);
        }
    }

    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        if (i % 2 == 0) {
            EXPECT_EQ(val0, i + 5);
            EXPECT_FALSE(t->GetVector(0)->IsValueNull(i));
        } else {
            EXPECT_EQ(val0, 0);
            EXPECT_TRUE(t->GetVector(0)->IsValueNull(i));
        }
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, WithNullValues) {
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows, -5);
    int64_t *col2 = MakeLongs(numRows, -5);
    const int32_t numCols = 2;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG)};
    DataExpr *data1 = new DataExpr(0, INT32D);
    std::vector<Expr *> args1;
    args1.push_back(data1);
    FuncExpr *absExpr1 = new FuncExpr("abs", args1, INT32D);

    DataExpr *data2 = new DataExpr(1, INT64D);
    std::vector<Expr *> args2;
    args2.push_back(data2);
    FuncExpr *absExpr2 = new FuncExpr("abs", args2, INT64D);


    std::vector<Expr*> exprs = {absExpr1, absExpr2};

    VecTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    for (int i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNull(i);
            t->GetVector(1)->SetValueNull(i);
        }
    }
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(ret[0]->GetVector(0)->IsValueNull(i));
            EXPECT_TRUE(ret[0]->GetVector(1)->IsValueNull(i));
        } else {
            EXPECT_FALSE(ret[0]->GetVector(0)->IsValueNull(i));
            EXPECT_FALSE(ret[0]->GetVector(1)->IsValueNull(i));
            int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
            int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
            EXPECT_EQ(val0, abs(i - 5));
            EXPECT_EQ(val1, abs(i - 5));
        }
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (ProjectTest, Negatives) {
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows, -5);
    const int32_t numCols = 1;
    DataExpr *subLeft = new DataExpr(0, INT32D);
    DataExpr *subRight = new DataExpr(500);
    BinaryExpr *subExpr = new BinaryExpr(SUB, subLeft, subRight, INT32D);
    std::vector<Expr*> exprs = {subExpr};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes inputTypes(vecOfTypes);
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
        op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 505);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Longs) {
    const int32_t numRows = 10000;
    int64_t* col = MakeLongs(numRows, -5000);
    const int32_t numCols = 1;
    DataExpr *mulLeft = new DataExpr(0, INT64D);
    DataExpr *mulRight = new DataExpr(5000000);
    mulRight->longVal = 5000000;
    BinaryExpr *mulExpr = new BinaryExpr(MUL, mulLeft, mulRight, INT64D);
    std::vector<Expr*> exprs = {mulExpr};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes inputTypes(vecOfTypes);
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
        op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < 1; i++) {
        int64_t val0 = ((LongVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (int64_t) (i - 5000) * 5000000);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Doubles) {
    const int32_t numRows = 10000;
    double* col = MakeDoubles(numRows, -5000.5);
    const int32_t numCols = 1;

    DataExpr *divLeft = new DataExpr(0, DOUBLED);
    DataExpr *divRight = new DataExpr(2);
    divRight->doubleVal = 2;
    BinaryExpr *divExpr = new BinaryExpr(DIV, divLeft, divRight, DOUBLED);
    std::vector<Expr*> exprs= {divExpr};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes inputTypes(vecOfTypes);
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
        op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < 1; i++) {
        double val0 = ((DoubleVector*) ret[0]->GetVector(0))->GetValue(i);
        double expected = (i - 5000.5) / 2;
        EXPECT_TRUE(val0 > expected - 0.1 && val0 < expected + 0.1);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, MultipleColumns) {
   const int32_t numRows = 1000;
   int32_t* col1 = MakeInts(numRows);
   int32_t* col2 = MakeInts(numRows, -100);
   int64_t* col3 = MakeLongs(numRows, -10);
   const int32_t numProject = 2;
    DataExpr *subLeft = new DataExpr(0, INT32D);
    DataExpr *subRight = new DataExpr(10);
    BinaryExpr *subExpr = new BinaryExpr(SUB, subLeft, subRight, INT32D);

    DataExpr *addLeft = new DataExpr(2, INT64D);
    DataExpr *addRight = new DataExpr(1);
    addRight->longVal = 1;
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT64D);

    std::vector<Expr*> exprs = {subExpr, addExpr};

   const int32_t numCols = 3;
   std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG) };
   VecTypes inputTypes(vecOfTypes);
   auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
       op->AddInput(t);
   vector<VectorBatch*> ret;
   int32_t numReturned = op->GetOutput(ret);
   for (int32_t i = 0; i < numReturned; i++) {
       int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
       EXPECT_EQ(val0, i - 10);
       int64_t val1 = ((LongVector*) ret[0]->GetVector(1))->GetValue(i);
       EXPECT_EQ(val1, i - 9);
   }

   VectorHelper::FreeVecBatch(t);
   VectorHelper::FreeVecBatches(ret);

   delete[] col1;
   delete[] col2;
   delete[] col3;

   delete op;
   delete factory;
}

TEST (ProjectTest, BenchmarkMultipleColumns) {
    const int32_t numRows = 1000;
    int32_t* col1 = MakeInts(numRows);
    int32_t* col2 = MakeInts(numRows, -100);
    int64_t* col3 = MakeLongs(numRows, -10);
    int64_t* col4 = new int64_t[numRows];

    vector<string*> strings;
    for (int32_t i = 0; i < numRows; i++) {
        std::string *s;
        if (i % 2 == 0) {
            s = new std::string("hello");
        }
        else if (i % 3 == 0) {
            s = new std::string("world");
        } else {
            s = new std::string("!!!!!");
        }
        col4[i] = (int64_t)(s->c_str());
        strings.push_back(s);
    }

    const int32_t numProject = 2;
    DataExpr *subLeft = new DataExpr(0, INT32D);
    DataExpr *subRight = new DataExpr(10);
    BinaryExpr *subExpr = new BinaryExpr(SUB, subLeft, subRight, INT32D);

    DataExpr *addLeft = new DataExpr(2, INT64D);
    DataExpr *addRight = new DataExpr(1);
    addRight->longVal = 1;
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT64D);

    std::vector<Expr*> exprs = {subExpr, addExpr};
    const int32_t numCols = 4;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                        VarcharVecType(1000) };
    VecTypes inputTypes(vecOfTypes);
    auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    std::cout << "[BenchmarkMultipleColumns Project without varchar]\n\n";
    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();

        op->AddInput(t);
        vector<VectorBatch *> ret;
        int32_t numReturned = op->GetOutput(ret);
        VectorHelper::FreeVecBatches(ret);

        auto end = std::chrono::system_clock::now();
        auto elapsed =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    delete op;
    delete factory;

    std::cout << "\n\n\n[BenchmarkMultipleColumns Project with varchar]\n\n";
    DataExpr *subLeft2 = new DataExpr(0, INT32D);
    DataExpr *subRight2 = new DataExpr(10);
    BinaryExpr *subExpr2 = new BinaryExpr(SUB, subLeft2, subRight2, INT32D);

    DataExpr *substData = new DataExpr(3, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(3);
    std::vector<Expr *> args;
    args.push_back(substData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    FuncExpr *substrExpr = new FuncExpr("substr", args, VARCHARD);

    std::vector<Expr*> exprs2 = {subExpr2, substrExpr};


    factory = new ProjectionOperatorFactory(exprs2, numProject, inputTypes, numCols);
    op = factory->CreateOperator();

    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();

        op->AddInput(t);
        vector<VectorBatch *> ret;
        int32_t numReturned = op->GetOutput(ret);
        VectorHelper::FreeVecBatches(ret);

        auto end = std::chrono::system_clock::now();
        auto elapsed =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    VectorHelper::FreeVecBatch(t);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    for (int32_t i = 0; i < numRows; i++) {
        delete strings[i];
    }
    delete[] col4;

    delete op;
    delete factory;
}

TEST (ProjectTest, DependOtherColumn) {
   const int32_t numRows = 1000;
   int32_t* col1 = MakeInts(numRows);
   int32_t* col2 = MakeInts(numRows, -100);
   int64_t* col3 = MakeLongs(numRows);
   const int32_t numProject = 2;
   DataExpr *mulLeft= new DataExpr(0, INT32D);
   DataExpr *mulRight = new DataExpr(1, INT32D);
   BinaryExpr *mulExpr = new BinaryExpr(MUL, mulLeft, mulRight, INT32D);

   DataExpr *conditionLeft= new DataExpr(0, INT32D);
   DataExpr *conditionRight = new DataExpr(500);
   BinaryExpr *condition = new BinaryExpr(LT, conditionLeft, conditionRight, BOOLD);

   DataExpr *texp = new DataExpr(4000000000);
   texp->longVal = 4000000000;

   DataExpr *fexp = new DataExpr(2, INT64D);

   IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

   std::vector<Expr*> exprs = {mulExpr, ifExpr};
   const int32_t numCols = 3;
   std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG)};
   VecTypes inputTypes(vecOfTypes);
   auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
       op->AddInput(t);
   vector<VectorBatch*> ret;
   int32_t numReturned = op->GetOutput(ret);
   for (int32_t i = 0; i < numReturned; i++) {
       int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
       EXPECT_EQ(val0, i * (i - 100));
       int64_t val1 = ((LongVector*) ret[0]->GetVector(1))->GetValue(i);
       EXPECT_EQ(val1, (int64_t) (i < 500 ? 4000000000 : i));
   }

   VectorHelper::FreeVecBatch(t);
   VectorHelper::FreeVecBatches(ret);

   delete[] col1;
   delete[] col2;
   delete[] col3;

   delete op;
   delete factory;
}

TEST(ProjectTest, ProjectString1) {
    vector<string*> strings;

    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = {VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
    int64_t* col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 40 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
        else {
            std::string *s = new std::string("abcdefghijklmnopqrstuvwxyz");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);


    const int32_t numProject = 2;

    DataExpr *substrData = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(3);
    std::vector<Expr *>args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    FuncExpr *substrExpr = new FuncExpr("substr", args, VARCHARD);

    DataExpr *col0 = new DataExpr(0, VARCHARD);
    std::vector<Expr*> exprs = {substrExpr, col0};

    auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    for (int32_t i = 0; i < numReturned; i += 20) {
        VarcharVector *vcVec = ((VarcharVector*) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr (*charArrCasted, 0, len);
    }


    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST (ProjectTest, DictionaryVecTest) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes vecTypes(inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    const int32_t numProject = 3;
    DataExpr *addLeft1 = new DataExpr(0, INT32D);
    DataExpr *addRight1 = new DataExpr(1);
    BinaryExpr *addExpr1 = new BinaryExpr(ADD, addLeft1, addRight1, INT32D);

    DataExpr *addLeft2 = new DataExpr(1, INT32D);
    DataExpr *addRight2 = new DataExpr(2);
    BinaryExpr *addExpr2 = new BinaryExpr(ADD, addLeft2, addRight2, INT32D);

    DataExpr *addLeft3 = new DataExpr(2, INT32D);
    DataExpr *addRight3 = new DataExpr(10);
    BinaryExpr *addExpr3 = new BinaryExpr(ADD, addLeft3, addRight3, INT32D);

    std::vector<Expr*> exprs = {addExpr1, addExpr2, addExpr3};

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, vecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryVector->GetInt(i) + 10);
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;

    delete op;
    delete factory;
}

TEST(ProjectTest, DictionaryVecDoubleTest)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    DoubleVector *col1 = new DoubleVector(vecAllocator, numRows);

    int32_t ids1[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *doubleDicVector = new DictionaryVector(col1, ids1, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, (i % 21) - 3);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {3};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes vecTypes(inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);

    batch->SetVector(0, doubleDicVector);

    const int32_t numProject = 1;
    DataExpr *addRight = new DataExpr(10);
    addRight->doubleVal = 10;
    std::vector<Expr*> exprs =  {new BinaryExpr(ADD, new DataExpr(0, DOUBLED), addRight, DOUBLED)};
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, vecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, doubleDicVector->GetDouble(i) + 10);
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
}

TEST(ProjectTest, DictionaryVecVarcharTest)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VarcharVector *col1 = new VarcharVector(vecAllocator, 5 * numRows, numRows);

    int32_t ids1[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    DictionaryVector *varcharDicVector = new DictionaryVector(col1, ids1, numRows);
    string str1 = "hello";
    string str2 = "world";
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col1->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
        } else {
            col1->SetValue(i, reinterpret_cast<const uint8_t *>(str2.c_str()), str2.length());
        }
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {15};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes vecTypes(inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);

    batch->SetVector(0, varcharDicVector);

    const int32_t numProject = 1;
    std:vector<Expr *> args;
    args.push_back(new DataExpr(0, VARCHARD));
    args.push_back(new DataExpr(1));
    args.push_back(new DataExpr(3));

    FuncExpr *substrExpr = new FuncExpr("substr", args, VARCHARD);
    std::vector<Expr*> exprs = {substrExpr};
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, vecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);

        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, "hel");
        } else {
            EXPECT_EQ(actualStr, "wor");
        }
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
}

TEST(ProjectTest, DictionaryVecDecimal128Test)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    Decimal128Vector *col1 = new Decimal128Vector(vecAllocator, numRows);

    int32_t ids1[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *decimal128DicVector = new DictionaryVector(col1, ids1, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 decimal128(0, i);
        col1->SetValue(i, decimal128);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {7};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes vecTypes(inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);

    batch->SetVector(0, decimal128DicVector);

    const int32_t numProject = 1;
    DataExpr *addRight = new DataExpr(20);
    addRight->longVal = 20;
    addRight->doubleVal = 20;
    BinaryExpr *addExpr = new BinaryExpr(ADD, new DataExpr(0, DECIMAL128D), addRight, DECIMAL128D);
    std::vector<Expr*> exprs = {addExpr};
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, vecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), decimal128DicVector->GetDecimal128(i).HighBits());
        EXPECT_EQ(val0.LowBits(), decimal128DicVector->GetDecimal128(i).LowBits() + 20);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
}

TEST(ProjectTest, DictionaryVecNestedTest)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, 3);
    int32_t data[] = {4, 5, 6};
    col3->SetValues(0, data, 3);
    int32_t ids[] = {1, 2};
    auto *dictionaryVector = new DictionaryVector(col3, ids, 2);
    int32_t nestedIds[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    auto *dictionaryNested = new DictionaryVector(dictionaryVector, nestedIds, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes vecTypes(inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    const int32_t numProjs = 3;
    DataExpr *addLeft1 = new DataExpr(0, INT32D);
    DataExpr *addRight1 = new DataExpr(1);
    BinaryExpr *addExpr1 = new BinaryExpr(ADD, addLeft1, addRight1, INT32D);

    DataExpr *addLeft2 = new DataExpr(1, INT32D);
    DataExpr *addRight2 = new DataExpr(2);
    BinaryExpr *addExpr2 = new BinaryExpr(ADD, addLeft2, addRight2, INT32D);

    DataExpr *addLeft3 = new DataExpr(2, INT32D);
    DataExpr *addRight3 = new DataExpr(10);
    BinaryExpr *addExpr3 = new BinaryExpr(ADD, addLeft3, addRight3, INT32D);

    std::vector<Expr*> exprs = {addExpr1, addExpr2, addExpr3};

    auto *factory = new ProjectionOperatorFactory(exprs, numProjs, vecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryNested->GetInt(i) + 10);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;
    delete dictionaryVector;

    delete op;
    delete factory;
}

TEST (ProjectTest, Decimal128Arithmetic) {
    const int32_t numRows = 10;
    int64_t* col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    DataExpr *addLeft = new DataExpr(0, DECIMAL128D);
    DataExpr *addRight = new DataExpr(20);
    addRight->longVal = 20;
    addRight->doubleVal = 20;
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, DECIMAL128D);

    std::vector<Expr*> exprs = {addExpr};
    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 20);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(ProjectTest, Decimal128Divide)
{
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    DataExpr *divRight = new DataExpr(20);
    divRight->longVal = 20;
    divRight->doubleVal = 20;
    BinaryExpr *divExpr = new BinaryExpr(DIV, new DataExpr(0, DECIMAL128D), divRight, DECIMAL128D);
    std::vector<Expr*> exprs = {divExpr};

    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i / 20);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(ProjectTest, MultipleDecimal128Columns)
{
    const int32_t numRows = 100;
    int64_t* col1 = MakeDecimals(numRows);
    int64_t* col2 = MakeDecimals(numRows, 100);
    const int32_t numProject = 2;
    DataExpr *addLeft = new DataExpr(0, DECIMAL128D);
    DataExpr *addRight = new DataExpr(50);
    addRight->longVal = 50;
    addRight->doubleVal = 50;
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, DECIMAL128D);

    DataExpr *mulLeft = new DataExpr(1, DECIMAL128D);
    DataExpr *mulRight = new DataExpr(20);
    mulRight->longVal = 20;
    mulRight->doubleVal = 20;
    BinaryExpr *mulExpr = new BinaryExpr(MUL, mulLeft, mulRight, DECIMAL128D);

    std::vector<Expr*> exprs = {addExpr, mulExpr};

    const int32_t numCols = 2;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 50);
    }
    int idx = 0;
    for (int32_t i = 100; i < 100 + 100; i++) {
        Decimal128 val0 = ((Decimal128Vector *) ret[0]->GetVector(1))->GetValue(idx);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 20);
        idx++;
    }


    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;

    delete op;
    delete factory;
}

TEST (ProjectTest, StringSubstr) {
    vector<string*> strings;

    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
    int64_t* col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("helloasdf");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
        else {
            std::string *s = new std::string("Bonjour");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);


    const int32_t numProject = 2;
    DataExpr *substrData = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(5);
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    FuncExpr *substrExpr = new FuncExpr("substr", args, VARCHARD);

    std::vector<Expr *> concatArgs;
    concatArgs.push_back(substrExpr);
    concatArgs.push_back(new DataExpr(new std::string(" world")));
    FuncExpr *concatExpr = new FuncExpr("concat", concatArgs, VARCHARD);

    DataExpr *col0 = new DataExpr(0, VARCHARD);
    std::vector<Expr*> exprs = {concatExpr, col0};
    auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    string expected1 = "hello world";
    string expected2 = "Bonjo world";
    for (int32_t i = 0; i < numReturned; i += 20) {
        VarcharVector *vcVec = ((VarcharVector*) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        string actualStr ((char*)actualChar, 0, len);
        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, expected1);
        } else {
            EXPECT_EQ(actualStr, expected2);
        }
        std::cout << "string " << i << ": '" << actualStr << "' has length " << len << std::endl;
    }


    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST (ProjectTest, SlicedDictionaryVecTest) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);
    delete col3;
    auto slicedCol1 = col1->Slice(5, 5);
    auto slicedCol2 = col2->Slice(5, 5);
    auto slicedCol3 = dictionaryVector->Slice(5, 5);
    delete col1;
    delete col2;
    delete dictionaryVector;

    VectorBatch *input = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    VecTypes inputVecTypes(inputTypes);
    input->NewVectors(vecAllocator, inputTypes);
    input->SetVector(0, slicedCol1);
    input->SetVector(1, slicedCol2);
    input->SetVector(2, slicedCol3);

    const int32_t numProject = 3;

    DataExpr *addLeft1 = new DataExpr(0, INT32D);
    DataExpr *addRight1 = new DataExpr(1);
    BinaryExpr *addExpr1 = new BinaryExpr(ADD, addLeft1, addRight1, INT32D);

    DataExpr *addLeft2 = new DataExpr(1, INT32D);
    DataExpr *addRight2 = new DataExpr(2);
    BinaryExpr *addExpr2 = new BinaryExpr(ADD, addLeft2, addRight2, INT32D);

    DataExpr *addLeft3 = new DataExpr(2, INT32D);
    DataExpr *addRight3 = new DataExpr(10);
    BinaryExpr *addExpr3 = new BinaryExpr(ADD, addLeft3, addRight3, INT32D);

    std::vector<Expr*> exprs = {addExpr1, addExpr2, addExpr3};
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputVecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i) + 10);
    }
    VectorHelper::FreeVecBatch(input);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST (ProjectTest, SlicedDictionaryVecWithNullTest) {
    const int32_t numCols = 1;
    const int32_t numRows = 10;

    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col1->SetValueNull(i);
        } else {
            col1->SetValue(i, i % 5);
        }
    }
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    auto dictionaryVector = new DictionaryVector(col1, ids, numRows);
    auto slicedCol1 = dictionaryVector->Slice(5, 5);
    delete col1;
    delete dictionaryVector;

    VectorBatch *input = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1};
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    input->NewVectors(vecAllocator, inputTypes);
    input->SetVector(0, slicedCol1);
    VecTypes inputVecTypes(inputTypes);
    const int32_t numProject = 1;
    //string exprs[numProject] = {"$operator$ADD:1(#0, #0)"};

    DataExpr *addLeft = new DataExpr(0, INT32D);
    DataExpr *addRight = new DataExpr(0, INT32D);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);

    std::vector<Expr*> exprs = {addExpr};

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputVecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    auto retVec = (IntVector *)(ret[0]->GetVector(0));
    for (int32_t i = 0; i < numReturned; i++) {
        if (i == 0) {
            EXPECT_TRUE(retVec->IsValueNull(i));
        } else {
            int64_t val0 = retVec->GetValue(i);
            EXPECT_EQ(val0, ((DictionaryVector *)slicedCol1)->GetInt(i) * 2);
        }
    }
    VectorHelper::FreeVecBatch(input);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}
}


