/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
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
                         int32_t* inputTypes,
                         int64_t* allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->NewVectors(inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypes[i]) {
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
                                                                        s.length() + 1);
                }
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            default: {
                DebugError("No such data type %d", inputTypes[i]);
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

TEST (ProjectTest, Simple) {
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$ADD:int(#0, 5)"};
    int32_t inputTypes[numCols] = {1};
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

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
        EXPECT_EQ(val0, i + 5);
    }

    for (int32_t i = 0; i < numReturned; i++) {
        if (i % 2 == 0) {
            EXPECT_FALSE(t->GetVector(0)->IsValueNull(i));
        } else {
            EXPECT_TRUE(t->GetVector(0)->IsValueNull(i));
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Negatives) {
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows, -5);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$SUBTRACT:int(#0, 500)"};
    int32_t inputTypes[numCols] = {1};
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    string exprs[numCols] = {"$operator$MULTIPLY:long(#0, 5000000)"};
    int32_t inputTypes[numCols] = {2};
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    string exprs[numCols] = {"$operator$DIVIDE:double(#0, 2)"};
    int32_t inputTypes[numCols] = {3};
    auto* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
   string exprs[numProject] = {"$operator$SUBTRACT:int(#0, 10)", "$operator$ADD:long(#2, 1)"};
   const int32_t numCols = 3;
   int32_t inputTypes[numCols] = {1, 1, 2};
   auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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

TEST (ProjectTest, DependOtherColumn) {
   const int32_t numRows = 1000;
   int32_t* col1 = MakeInts(numRows);
   int32_t* col2 = MakeInts(numRows, -100);
   int64_t* col3 = MakeLongs(numRows);
   const int32_t numProject = 2;
   string exprs[numProject] = {"$operator$MULTIPLY:int(#0, #1)", "IF:long($operator$LESS_THAN:boolean(#0, 500), 4000000000, #2)"};
   const int32_t numCols = 3;
   int32_t inputTypes[numCols] = {1, 1, 2};
   auto* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_VEC_TYPE_VARCHAR;

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
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);


    const int32_t numProject = 2;
    std::string exprs[numProject] = {"substr:varchar(#0, 1, 3)", "#0"};

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
        std::cout << "string " << i << ": '" << actualStr << "' has length " << len << std::endl;
    }


    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}

TEST (ProjectTest, DictionaryVecTest) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    IntVector *col1 = new IntVector(nullptr, numRows);
    IntVector *col2 = new IntVector(nullptr, numRows);
    IntVector *col3 = new IntVector(nullptr, numRows);
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypes[numCols] = {1, 1, 1};
    batch->NewVectors(inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    const int32_t numProject = 3;
    string exprs[numProject] = {"$operator$ADD:int(#0, 1)", "$operator$ADD:int(#1, 2)",
                                "$operator$ADD:int(#2, 10)"};
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
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
}

TEST (ProjectTest, DictionaryVecNestedTest) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    IntVector *col1 = new IntVector(nullptr, numRows);
    IntVector *col2 = new IntVector(nullptr, numRows);
    IntVector *col3 = new IntVector(nullptr, 3);
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
    int32_t inputTypes[numCols] = {1, 1, 1};
    batch->NewVectors(inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    const int32_t numProjs = 3;
    string exprs[numProjs] = {"$operator$ADD:int(#0, 1)", "$operator$ADD:int(#1, 2)",
                              "$operator$ADD:int(#2, 10)"};
    auto *factory = new ProjectionOperatorFactory(exprs, numProjs, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    VectorHelper::PrintVecBatch(ret[0]);
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
}

TEST (ProjectTest, Decimal128Arithmetic) {
    const int32_t numRows = 10;
    int64_t* col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    string exprs[numProject] = {"$operator$ADD:decimal(#0, 20)"};
    const int32_t numCols = 1;
    int32_t inputTypes[numCols] = {7};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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

TEST (ProjectTest, MultipleDecimal128Columns) {
    const int32_t numRows = 100;
    int64_t* col1 = MakeDecimals(numRows);
    int64_t* col2 = MakeDecimals(numRows, 100);
    const int32_t numProject = 2;
    string exprs[numProject] = {"$operator$ADD:decimal(#0, 50)", "$operator$MULTIPLY:decimal(#1, 20)"};
    const int32_t numCols = 2;
    int32_t inputTypes[numCols] = {7, 7};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
}
