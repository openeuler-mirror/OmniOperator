#include "../parser.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
using namespace std;


int main() {
    int numTests = 25;
    vector<string> simpleTest(numTests);
    // ComparisionExpr, BinaryExpr
    simpleTest[0] = "$operator$LESS_THAN_OR_EQUAL(#0, 14)";
    simpleTest[1] = "AND($operator$GREATER_THAN_OR_EQUAL(#2, 14), $operator$LESS_THAN_OR_EQUAL(#1, 12))";
    simpleTest[2] = "AND($operator$GREATER_THAN_OR_EQUAL(#0, 14), OR($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
    simpleTest[3] = "AND(OR($operator$LESS_THAN(#1, 12), $operator$GREATER_THAN(#2, 0)), $operator$NOT_EQUAL(#0, 14))";
    simpleTest[4] = "AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
    simpleTest[5] = "OR(AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), $operator$NOT_EQUAL(#2, 2)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
    simpleTest[6] = "OR(AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)), AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), $operator$NOT_EQUAL(#2, 2)))";
    simpleTest[7] = "AND(AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0))), AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0))))";
    simpleTest[8] = "OR(OR(OR($operator$EQUAL(#0, 1), $operator$EQUAL(#0, 2)), $operator$EQUAL(#0, 3)), OR(OR(OR(OR($operator$EQUAL(#0, 55), $operator$EQUAL(#0, 5)), $operator$EQUAL(#0, 8)), $operator$EQUAL(#0, 13)), $operator$NOT_EQUAL(#1, 0)))";
    simpleTest[9] = "OR($operator$GREATER_THAN_OR_EQUAL(#5, 52), AND($operator$LESS_THAN(#4, 50.8), AND(AND($operator$GREATER_THAN(#2, 4800), $operator$LESS_THAN_OR_EQUAL(#1, 9990)), AND($operator$NOT_EQUAL(#0, 1), $operator$EQUAL(#3, 4000000000)))))";
    simpleTest[10] = "AND(OR($operator$LESS_THAN(#0, 50), $operator$EQUAL(#1, -12)), OR($operator$LESS_THAN_OR_EQUAL(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL(#3, 0)))";
    // Between
    simpleTest[11] = "BETWEEN(#0, 1, 12)";
    simpleTest[12] = "AND(BETWEEN(#0, 1, 12), BETWEEN(#1, 10, 100))";
    simpleTest[13] = "AND(AND($operator$GREATER_THAN(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))";
    // Not
    simpleTest[14] = "NOT(BETWEEN(#2, 3, 10))";
    simpleTest[15] = "OR(NOT(BETWEEN(#3, '4', '11')), $operator$LESS_THAN(#2, 3))";
    simpleTest[16] = "not(AND(OR(not(BETWEEN(#2, 3, 10)), $operator$LESS_THAN(#4, 5)), not(BETWEEN(#6, 7, 9))))";
    // In
    simpleTest[17] = "IN(#4, 1, 2, 3, 4, 5, 6)";
    simpleTest[18] = "AND(NOT(IN(#3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), IN(#4, '21', '22', '23', '24', '25'))";
    // Data Types
    simpleTest[19] = "$operator$EQUAL(#4, 'brand34')";
    simpleTest[20] = "IN(#3, 'AIR', 'RAIL', 'MAIL')";
    simpleTest[21] = "OR(AND($operator$EQUAL(#4, 2.34), IN(#3, 'A', 'B', 'C', 'D')), IN(#2, 1, 2, 3, 4, 5))";
    // COALESCE (ex. COALESCE(#2, #3))
    simpleTest[22] = "COALESCE(#4, #6)";
    simpleTest[23] = "AND(COALESCE(#2, #3), not(COALESCE(#4, #5)))";
    // SUBSTR
    simpleTest[24] = "substr(#2, 1, 2)";
    // simpleTest[25] = "IN(substr(#1, 1, 2), '13', '31, '24, '42')"; // TODO: add support



    vector<string> expected(numTests);
    expected[0] = "Cmp(LTE, #0, 14)";
    expected[1] = "Bin(AND, Cmp(GTE, #2, 14), Cmp(LTE, #1, 12))";
    expected[2] = "Bin(AND, Cmp(GTE, #0, 14), Bin(OR, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
    expected[3] = "Bin(AND, Bin(OR, Cmp(LT, #1, 12), Cmp(GT, #2, 0)), Cmp(NEQ, #0, 14))";
    expected[4] = "Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
    expected[5] = "Bin(OR, Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Cmp(NEQ, #2, 2)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
    expected[6] = "Bin(OR, Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)), Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Cmp(NEQ, #2, 2)))";
    expected[7] = "Bin(AND, Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0))), Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0))))";
    expected[8] = "Bin(OR, Bin(OR, Bin(OR, Cmp(EQ, #0, 1), Cmp(EQ, #0, 2)), Cmp(EQ, #0, 3)), Bin(OR, Bin(OR, Bin(OR, Bin(OR, Cmp(EQ, 55, #0), Cmp(EQ, 5, #0)), Cmp(EQ, #0, 8)), Cmp(EQ, #0, 13)), Cmp(NEQ, #1, 0)))";
    expected[9] = "Bin(OR, Cmp(GTE, #5, 52), Bin(AND, Cmp(LT, #4, 50.800000), Bin(AND, Bin(AND, Cmp(GT, #2, 4800), Cmp(LTE, #1, 9990)), Bin(AND, Cmp(NEQ, #0, 1), Cmp(EQ, #3, 4000000000)))))";
    expected[10] = "Bin(AND, Bin(OR, Cmp(LT, #0, 50), Cmp(EQ, #1, -12)), Bin(OR, Cmp(LTE, #2, -3000000000), Cmp(GTE, #3, 0)))";
    expected[11] = "Between(#0, 1, 12)";
    expected[12] = "Bin(AND, Between(#0, 1, 12), Between(#1, 10, 100))";
    expected[13] = "Bin(AND, Bin(AND, Cmp(GT, #3, 8766), Cmp(LT, #3, 9131)), Bin(AND, Between(#2, 0.050000, 0.070000), Cmp(LT, #0, 24.000000)))";
    expected[14] = "Un(NOT, Between(#2, 3, 10))";
    expected[15] = "Bin(OR, Un(NOT, Between(#3, '4', '11')), Cmp(LT, #2, 3))";
    expected[16] = "Un(NOT, Bin(AND, Bin(OR, Un(NOT, Between(#2, 3, 10)), Cmp(LT, #4, 5)), Un(NOT, Between(#6, 7, 9))))";
    expected[17] = "In(#4, 1, 2, 3, 4, 5, 6)";
    expected[18] = "Bin(AND, Un(NOT, In(#3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), In(#4, '21', '22', '23', '24', '25'))";
    expected[19] = "Cmp(EQ, #4, 'brand34')";
    expected[20] = "In(#3, 'AIR', 'RAIL', 'MAIL')";
    expected[21] = "Bin(OR, Bin(AND, Cmp(EQ, #4, 2.340000), In(#3, 'A', 'B', 'C', 'D')), In(#2, 1, 2, 3, 4, 5))";
    expected[22] = "Coalesce(#4, #6)";
    expected[23] = "Bin(AND, Coalesce(#2, #3), Un(NOT, Coalesce(#4, #5)))";
    expected[24] = "Substr(#2, 1, 2)";
    // expected[25] = "In(Substr(#1, 1, 2), '13, '31', '24', '42')";

    Parser parserObj;
    
    // To test (cd to core/src/common): clang++-12 -g *.cpp parser/*.cpp parser/parsertest/*.cpp -o parsetest
    // To test with debugging output (cd to core/src/common): clang++-12 -g -D DEBUG *.cpp parser/*.cpp parser/parsertest/*.cpp -o parsetest

    for (int i = 0; i < numTests; i++) {
    //for (int i = 22; i <= 23; i++) {
        std::cout << "simpleTest[" << i << "]" << std::endl;
        std::cout << "RowExpression:::" << simpleTest[i] << std::endl;
        Expr* result = parserObj.parseRowExpression(simpleTest[i]);
        std::cout << "ExprType:::" << result->getType() << std::endl;
        std::cout << " Final expression tree:::";
        result->printExprTree();
        std::cout << std::endl << "Expect expression tree:::" << expected[i] << std::endl << std::endl;
        delete result;
    }
}