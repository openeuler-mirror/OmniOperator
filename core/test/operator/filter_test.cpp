#include "gtest/gtest.h"
// #include "../src/jni/filter_api.h"
#include "../../src/operator/filter/filter.h"
#include <iostream>
#include <cstring>
#include <vector>

Table* createInput(const int32_t NUM_ROWS,
                    const int32_t NUM_COLS,
                    int32_t* inputTypes,
                    int64_t* allData)
{
    Table* t = new Table(NUM_ROWS, NUM_COLS);
    for (int i = 0; i < NUM_COLS; i++) {
        void* data = (void*) allData[i];
        ColumnType columnType = static_cast<ColumnType>(inputTypes[i]);
        Column* col = new Column(data, columnType, NUM_ROWS);
        t->setColumn(col, columnType);
    }
    return t;
}

bool checkOutput(Table* t, const int32_t NUM_ROWS, bool (*filter)(Table*, int32_t)) {
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool filter1(Table* t, int32_t index) {
    return *((int32_t*) t->getColumn(0)->getValue(index)) >= 5;
}

// Expects 2 columns of type int32, int64
bool filter2(Table* t, int32_t index) {
    int32_t val1 = *((int32_t*) t->getColumn(0)->getValue(index));
    int64_t val2 = *((int64_t*) t->getColumn(1)->getValue(index));
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool filter3(Table* t, int32_t index) {
    int32_t val1 = *((int32_t*) t->getColumn(0)->getValue(index));
    int64_t val2 = *((int64_t*) t->getColumn(1)->getValue(index));
    double val3 = *((double*) t->getColumn(2)->getValue(index));
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == (int64_t) 3e9 && val3 >= 0.4;
}

bool filter4(Table* t, int32_t index) {
    int32_t val0 = *((int32_t*) t->getColumn(0)->getValue(index));
    int32_t val2 = *((int32_t*) t->getColumn(1)->getValue(index));
    double val4 = *((double*) t->getColumn(2)->getValue(index));
    int64_t val5 = *((int64_t*) t->getColumn(3)->getValue(index));
    return (val0 != 1 && val2 > 4800 && val4 < 50.8) || val5 >= 52;
}

bool filter5(Table* t, int32_t index) {
    int32_t val0 = *((int32_t*) t->getColumn(0)->getValue(index));
    double val2 = *((double*) t->getColumn(2)->getValue(index));
    double val3 = *((double*) t->getColumn(3)->getValue(index));
    return val0 < 24 && val2 >= 0.05 && val2 <= 0.07 && val3 > 9766 && val3 < 9131;
}

bool filter6(Table* t, int32_t index) {
    // project order reversed
    int64_t val0 = *((int64_t*) t->getColumn(0)->getValue(index));
    int64_t val1 = *((int64_t*) t->getColumn(1)->getValue(index));
    int32_t val2 = *((int32_t*) t->getColumn(2)->getValue(index));
    int32_t val3 = *((int32_t*) t->getColumn(3)->getValue(index));
    return (val0 >= 0 || val1 <= -3e9) && (val2 == -12 || val3 < 50);
}

TEST (FilterTest, LessThan) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$LESS_THAN(#0, 2000)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_TRUE(val < 2000);
    }
}

TEST (FilterTest, GreaterThan) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 25;
        col2[i] = 3e9;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$GREATER_THAN(#0, 20)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 800);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        int64_t val1 = *((int64_t*) ret[0]->getColumn(1)->getValue(i));
        EXPECT_TRUE(val0 > 20);
        EXPECT_EQ(val1, (int64_t) 3e9);
    }
}

TEST (FilterTest, EqualTo) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    double* col2 = new double[NUM_ROWS];
    int64_t* col3 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col2[i] = col3[i] = i % 100;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {2, 1};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$EQUAL(#2, 50)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 50);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = *((double*) ret[0]->getColumn(0)->getValue(i));
        int64_t val1 = *((int32_t*) ret[0]->getColumn(1)->getValue(i));
        EXPECT_EQ(val0, 50);
        EXPECT_EQ(val0, val1);
    }
}

TEST (FilterTest, GreaterThanOrEqualTo) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col2[i] = (i * (i + 2)) % 40;
        col1[i] = i;
        if (i % 45 == 0) col2[i] = 30;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {1};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$GREATER_THAN_OR_EQUAL(#1, 30)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 834);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_TRUE(val0 >= 30);
    }

}

TEST (FilterTest, NotEqualTo) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 3;

    const int32_t NUM_ROWS = 5000;
    double* col1 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$NOT_EQUAL(#0, 0)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 4999);
    double cnt = 1;
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = *((double*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_EQ(val0, cnt++);
    }
}

TEST (FilterTest, AllPass) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 20000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = 9348;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$EQUAL(#0, 9348)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 20000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_EQ(val0, 9348);
    }
}

TEST (FilterTest, MultipleInputs) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    
    const int32_t NUM_ROWS = 1000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int32_t* data2 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i % 10;
        data2[i] = i % 6 + 1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<Table*> ret;
    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$LESS_THAN_OR_EQUAL(#0, 4)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();

    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter1));
    EXPECT_EQ(numReturned, 500);

    allData[0] = (int64_t) data2;
    Table* in2 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->addInput(in2, NUM_ROWS);
    numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[1], numReturned, filter1));
    EXPECT_EQ(numReturned, 668);

    op->close();
    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete factory;
    delete in1;
    delete in2;
    delete ret[0];
    delete ret[1];

}

TEST (FilterTest, NegativeValues) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    
    const int32_t NUM_ROWS = 10000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int64_t* data2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i * i % 100 + 1;
        if (i % 5 == 0) data1[i] = -data1[i];
        data2[i] = i % 100 + 3e9;
        if (i % 7 == 0) data2[i] = -data2[i];
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    std::vector<Table*> ret;

    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("AND($operator$LESS_THAN_OR_EQUAL(#0, -1), $operator$LESS_THAN_OR_EQUAL(#1, -1))", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter2));
    // Both values are negative for every multiple of 35.
    EXPECT_EQ(numReturned, 286);

    op->close();
    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete factory;
    delete in1;
    delete ret[0];
}

TEST (FilterTest, AllTypes) {

    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;
    
    const int32_t NUM_ROWS = 10000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int64_t* data2 = new int64_t[NUM_ROWS];
    double* data3 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i % 3;
        data2[i] = i % 2 ? 3e9 : 0;
        data3[i] = i % 10 / 10.0;
    }

    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    std::vector<Table*> ret;

    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    std::string expr = "AND($operator$EQUAL(#0, 0), AND($operator$EQUAL(#1, 1000000000), $operator$GREATER_THAN_OR_EQUAL(#2, 0.4)))";
    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter3));
    EXPECT_EQ(numReturned, 1000);

    op->close();
    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete factory;
    delete in1;
    delete ret[0];
}

TEST (FilterTest, Compile) {
    // simple unit test
    std::string filterExpression = "AND(AND($operator$GREATER_THAN(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND($operator$BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))";

    const int32_t NUM_COLS = 4;
    int32_t *inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 3;
    inputTypes[3] = 3;
    
    const int32_t DATA_SIZE = 1000;
    int32_t *data1 = new int32_t[DATA_SIZE];
    int32_t *data2 = new int32_t[DATA_SIZE];
    double *data3 = new double[DATA_SIZE];
    double *data4 = new double[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data4[i] = i;
        data3[i] = i % 10 / 100.0;
        data1[i] = i % 26;
        data2[i] = 6;
    }

    int64_t datas[4] = {(int64_t)data1, (int64_t)data2, (int64_t)data3, (int64_t)data4};
    int32_t *projectIdx = new int32_t[1];
    projectIdx[0] = 0;
    Table* t = createInput(DATA_SIZE, NUM_COLS, inputTypes, datas);

    NativeOmniFilterOperatorFactory *factory = new NativeOmniFilterOperatorFactory(filterExpression, inputTypes, NUM_COLS, projectIdx, 1);
    NativeOmniOperator *op = factory->createOmniOperator();
    op->addInput(t, DATA_SIZE);
    std::vector<Table*> ret;
    int32_t numSelectedRows = op->getOutput(ret);
    EXPECT_EQ(numSelectedRows, 100);
    EXPECT_TRUE(checkOutput(ret[0], DATA_SIZE, filter5));
    
    op->close();
    delete[] inputTypes;
    delete[] data1; 
    delete[] data2;
    delete[] data3;
    delete[] data4;
    delete[] projectIdx;
    delete t;
    delete factory;
    delete ret[0];
}

TEST (FilterTest, LogicalOperators1) {
    std::string expr = "OR($operator$GREATER_THAN_OR_EQUAL(#5, 52), AND($operator$LESS_THAN(#4, 50.8), AND(AND($operator$GREATER_THAN(#2, 4800), $operator$LESS_THAN_OR_EQUAL(#1, 9990)), AND($operator$NOT_EQUAL(#0, 1), $operator$EQUAL(#3, 3000000000)))))";

    const int32_t NUM_COLS = 6;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    // int int int long double long
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;
    inputTypes[3] = 2;
    inputTypes[4] = 3;
    inputTypes[5] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    int64_t* col4 = new int64_t[NUM_ROWS];
    double* col5 = new double[NUM_ROWS];
    int64_t* col6 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 3 ? 1 : 0;
        col2[i] = col3[i] = i;
        col4[i] = i % 2 ? 2999999999 : 3e9;
        col5[i] = 50 + i / 10.0;
        col6[i] = i % 55;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4, (int64_t) col5, (int64_t) col6};
    const int32_t PROJECT_COUNT = 4;
    int32_t projectIndices[PROJECT_COUNT] = {0, 2, 4, 5};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 543);
    EXPECT_TRUE(checkOutput(ret[0], NUM_ROWS, filter4));

}

TEST (FilterTest, LogicalOperators2) {
    std::string expr = "AND(OR($operator$LESS_THAN(#0, 50), $operator$EQUAL(#1, -12)), OR($operator$LESS_THAN_OR_EQUAL(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL(#3, 0)))";

    const int32_t NUM_COLS = 4;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 2;
    inputTypes[3] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int64_t* col3 = new int64_t[NUM_ROWS];
    int64_t* col4 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 100;
        col2[i] = i % 7 == 0 ? -12 : i;
        col3[i] = i % 8 == 0 ? -i - 3e9 : i + 3e9;
        col4[i] = i % 9 - 4;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4};
    const int32_t PROJECT_COUNT = 4;
    int32_t projectIndices[PROJECT_COUNT] = {3, 2, 1, 0};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 3498);
    EXPECT_TRUE(checkOutput(ret[0], NUM_ROWS, filter6));

}

TEST (FilterTest, LogicalOperators3) {
    std::string expr = "OR(OR(OR($operator$EQUAL(#0, 1), $operator$EQUAL(#0, 2)), $operator$EQUAL(#0, 3)), OR(OR(OR(OR($operator$EQUAL(55, #0), $operator$EQUAL(5, #0)), $operator$EQUAL(#0, 8)), $operator$EQUAL(#0, 13)), $operator$NOT_EQUAL(#1, 0)))";
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 3;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    double* col2 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = 0;
        col2[i] = 1.5;
    }
    col1[0] = 0;
    col1[1] = 1;
    col1[2] = 1;
    col1[3] = 2;
    col1[4] = 3;
    col1[5] = 5;
    col1[6] = 8;
    col1[7] = 13;
    col2[2] = 0;
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {1, 0};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int32_t i = 0; i < 6; i++) {
        double val0 = *((double*) ret[0]->getColumn(0)->getValue(i));
        int32_t val1 = *((int32_t*) ret[0]->getColumn(1)->getValue(i));
        EXPECT_TRUE(val0 != 0);
        EXPECT_TRUE(val1 == col1[i + 2]);
    }
}

TEST (FilterTest, ArithmeticAdd) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 5;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$GREATER_THAN(ADD(#0, 1), 4)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_EQ(val0, 5);
    }
}

TEST (FilterTest, ArithmeticSubtract) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 10;
        col2[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory("$operator$LESS_THAN(0, SUBTRACT(#0, 5))", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 5000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        EXPECT_TRUE(0 < val0 - 5);
    }
}

TEST (FilterTest, ArithmeticMultiply) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 10 + 1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    Table* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "AND($operator$EQUAL(0, MULTIPLY(#0, #0)), $operator$GREATER_THAN(7, MULTIPLY(2, #1)))";
    NativeOmniOperatorFactory* factory = new NativeOmniFilterOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    NativeOmniOperator* op = factory->createOmniOperator();
    op->addInput(t, NUM_ROWS);
    std::vector<Table*> ret;
    int32_t numReturned = op->getOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = *((int32_t*) ret[0]->getColumn(0)->getValue(i));
        int64_t val1 = *((int64_t*) ret[0]->getColumn(1)->getValue(i));
        EXPECT_EQ(val0, 0);
        EXPECT_TRUE(val1 * 2 < 7);
    }
}

TEST (FilterTest, ArithmeticDivide) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
}