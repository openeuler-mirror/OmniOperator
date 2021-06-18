// #include "gtest/gtest.h"
// #include "../parser.h"
// #include <stdio.h>
// #include <iostream>
// #include <cstring>
// using namespace std;
// string exprTypeString(ExprType et) {
//     switch (et) {
//         case ExprType::DATA_E: return "data";
//         case ExprType::BINARY_E: return "binary";
//         case ExprType::UNARY_E: return "unary";
//         case ExprType::IN_E: return "in";
//         case ExprType::IF_E: return "if";
//         case ExprType::BETWEEN_E: return "between";
//         case ExprType::COALESCE_E: return "coalesce";
//         case ExprType::FUNC_E: return "function";
//         default: return "invalid";
//     }
// }
// TEST (ParserTest, ParseTest) {
//     Parser parserObj;
//     // Sample types (does not look at tests above)
//     int32_t inputTypes[15] = {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
//     int32_t vecCount = 15;
//     int numTests = 34;


//     vector<string> simpleTest(numTests);
//     // ComparisionExpr, BinaryExpr
//     simpleTest[0] = "$operator$LESS_THAN_OR_EQUAL(#0, 14)";
//     simpleTest[1] = "AND($operator$GREATER_THAN_OR_EQUAL(#2, 14), $operator$LESS_THAN_OR_EQUAL(#1, 12))";
//     simpleTest[2] = "AND($operator$GREATER_THAN_OR_EQUAL(#0, 14), OR($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
//     simpleTest[3] = "AND(OR($operator$LESS_THAN(#1, 12), $operator$GREATER_THAN(#2, 0)), $operator$NOT_EQUAL(#0, 14))";
//     simpleTest[4] = "AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
//     simpleTest[5] = "OR(AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), $operator$NOT_EQUAL(#2, 2)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)))";
//     simpleTest[6] = "OR(AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0)), AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), $operator$NOT_EQUAL(#2, 2)))";
//     simpleTest[7] = "AND(AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0))), AND(OR($operator$GREATER_THAN_OR_EQUAL(#0, 14), $operator$NOT_EQUAL(#2, 1)), AND($operator$LESS_THAN_OR_EQUAL(#1, 12), $operator$EQUAL(#2, 0))))";
//     simpleTest[8] = "OR(OR(OR($operator$EQUAL(#0, 1), $operator$EQUAL(#0, 2)), $operator$EQUAL(#0, 3)), OR(OR(OR(OR($operator$EQUAL(#0, 55), $operator$EQUAL(#0, 5)), $operator$EQUAL(#0, 8)), $operator$EQUAL(#0, 13)), $operator$NOT_EQUAL(#1, 0)))";
//     simpleTest[9] = "OR($operator$GREATER_THAN_OR_EQUAL(#5, 52), AND($operator$LESS_THAN(#4, 50.8), AND(AND($operator$GREATER_THAN(#2, 4800), $operator$LESS_THAN_OR_EQUAL(#1, 9990)), AND($operator$NOT_EQUAL(#0, 1), $operator$EQUAL(#3, 4000000000)))))";
//     simpleTest[10] = "AND(OR($operator$LESS_THAN(#0, 50), $operator$EQUAL(#1, -12)), OR($operator$LESS_THAN_OR_EQUAL(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL(#3, 0)))";
//     // Between
//     simpleTest[11] = "BETWEEN(#0, 1, 12)";
//     simpleTest[12] = "AND(BETWEEN(#0, 1, 12), BETWEEN(#1, 10, 100))";
//     simpleTest[13] = "AND(AND($operator$GREATER_THAN(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))";
//     // Not
//     simpleTest[14] = "NOT(BETWEEN(#2, 3, 10))";
//     simpleTest[15] = "OR(NOT(BETWEEN(#3, '4', '11')), $operator$EQUAL(#2, 'hello'))";
//     simpleTest[16] = "not(AND(OR(not(BETWEEN(#2, 3, 10)), $operator$LESS_THAN(#4, 5)), not(BETWEEN(#6, 7, 9))))";
//     // In
//     simpleTest[17] = "IN(#4, 1, 2, 3, 4, 5, 6)";
//     simpleTest[18] = "AND(NOT(IN(#3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), IN(#4, '21', '22', '23', '24', '25'))";
//     // Data Types
//     simpleTest[19] = "$operator$EQUAL(#4, 'brand34')";
//     simpleTest[20] = "IN(#3, 'AIR', 'RAIL', 'MAIL')";
//     simpleTest[21] = "OR(AND($operator$EQUAL(#4, 2.34), IN(#3, 'A', 'B', 'C', 'D')), IN(#2, 1, 2, 3, 4, 5))";
//     // COALESCE (ex. COALESCE(#2, #3))
//     simpleTest[22] = "COALESCE(#4, #6)";
//     simpleTest[23] = "AND(COALESCE(#2, #3), not(COALESCE(#4, 5)))";
//     // SUBSTR
//     simpleTest[24] = "substr(#2, 1, 2)";
//     simpleTest[25] = "AND($operator$GREATER_THAN(#13, #12), OR($operator$NOT_EQUAL(#2, 912), $operator$LESS_THAN_OR_EQUAL(#4, #5)))";
//     simpleTest[26] = "ADD(124.0, MULTIPLY(#14, 0.2))";
//     simpleTest[27] = "IN(substr(#1, 1, 2), '13', '31 sjdf;lkfdsj  djfsldk', '24', '42')";
//     // CAST
//     simpleTest[28] = "IN(CAST(#12), 1.0, 2.0, 3.0)";
//     // IF
//     simpleTest[29] = "IF(OR($operator$EQUAL(#0, 'abc'), $operator$EQUAL(#0, 'xyz')), 1, 0)";
//     // ABS and other functions
//     // TODO: fix this case
//     simpleTest[30] = "ADD(#1, DIVIDE(abs(SUBTRACT(#6, MULTIPLY(#7, 100))), #7))";
//     simpleTest[31] = "$operator$GREATER_THAN(IF($operator$GREATER_THAN(#6, 0), DIVIDE(abs(SUBTRACT(#5, #6)), #6), 0.0), 0.0)";
//     simpleTest[32] = "CAST('1994-01-01')";
//     simpleTest[33] = "LIKE(#2, '%green%')";


//     vector<string> expected(numTests);
//     expected[0] = "Cmp(LTE, #0, 14)";
//     expected[1] = "Bin(AND, Cmp(GTE, #2, 14), Cmp(LTE, #1, 12))";
//     expected[2] = "Bin(AND, Cmp(GTE, #0, 14), Bin(OR, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
//     expected[3] = "Bin(AND, Bin(OR, Cmp(LT, #1, 12), Cmp(GT, #2, 0)), Cmp(NEQ, #0, 14))";
//     expected[4] = "Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
//     expected[5] = "Bin(OR, Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Cmp(NEQ, #2, 2)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)))";
//     expected[6] = "Bin(OR, Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0)), Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Cmp(NEQ, #2, 2)))";
//     expected[7] = "Bin(AND, Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0))), Bin(AND, Bin(OR, Cmp(GTE, #0, 14), Cmp(NEQ, #2, 1)), Bin(AND, Cmp(LTE, #1, 12), Cmp(EQ, #2, 0))))";
//     expected[8] = "Bin(OR, Bin(OR, Bin(OR, Cmp(EQ, #0, 1), Cmp(EQ, #0, 2)), Cmp(EQ, #0, 3)), Bin(OR, Bin(OR, Bin(OR, Bin(OR, Cmp(EQ, 55, #0), Cmp(EQ, 5, #0)), Cmp(EQ, #0, 8)), Cmp(EQ, #0, 13)), Cmp(NEQ, #1, 0)))";
//     expected[9] = "Bin(OR, Cmp(GTE, #5, 52), Bin(AND, Cmp(LT, #4, 50.800000), Bin(AND, Bin(AND, Cmp(GT, #2, 4800), Cmp(LTE, #1, 9990)), Bin(AND, Cmp(NEQ, #0, 1), Cmp(EQ, #3, 4000000000)))))";
//     expected[10] = "Bin(AND, Bin(OR, Cmp(LT, #0, 50), Cmp(EQ, #1, -12)), Bin(OR, Cmp(LTE, #2, -3000000000), Cmp(GTE, #3, 0)))";
//     expected[11] = "Between(#0, 1, 12)";
//     expected[12] = "Bin(AND, Between(#0, 1, 12), Between(#1, 10, 100))";
//     expected[13] = "Bin(AND, Bin(AND, Cmp(GT, #3, 8766), Cmp(LT, #3, 9131)), Bin(AND, Between(#2, 0.050000, 0.070000), Cmp(LT, #0, 24.000000)))";
//     expected[14] = "Unary(NOT, Between(#2, 3, 10))";
//     expected[15] = "Bin(OR, Unary(NOT, Between(#3, '4', '11')), Cmp(EQ, #2, 'hello'))";
//     expected[16] = "Unary(NOT, Bin(AND, Bin(OR, Unary(NOT, Between(#2, 3, 10)), Cmp(LT, #4, 5)), Unary(NOT, Between(#6, 7, 9))))";
//     expected[17] = "In(#4, 1, 2, 3, 4, 5, 6)";
//     expected[18] = "Bin(AND, Unary(NOT, In(#3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), In(#4, '21', '22', '23', '24', '25'))";
//     expected[19] = "Cmp(EQ, #4, 'brand34')";
//     expected[20] = "In(#3, 'AIR', 'RAIL', 'MAIL')";
//     expected[21] = "Bin(OR, Bin(AND, Cmp(EQ, #4, 2.340000), In(#3, 'A', 'B', 'C', 'D')), In(#2, 1, 2, 3, 4, 5))";
//     expected[22] = "Coalesce(#4, #6)";
//     expected[23] = "Bin(AND, Coalesce(#2, #3), Unary(NOT, Coalesce(#4, 5)))";
//     expected[24] = "Substr(#2, 1, 2)";
//     expected[25] = "Bin(AND, Cmp(GT, #13, #12), Bin(OR, Cmp(NEQ, #2, 912), Cmp(LTE, #4, #5)))";
//     expected[26] = "Arith(ADD, 124.000000, Arith(MUL, #14, 0.200000))";
//     expected[27] = "In(substr(#1, 1, 2), '13', '31 sjdf;lkfdsj  djfsldk', '24', '42')";
//     expected[28] = "In(Cast(#12), 1.000000, 2.000000, 3.000000)";
//     expected[29] = "If(Bin(OR, Cmp(EQ, #0, 'abc'), Cmp(EQ, #0, 'xyz')), 1, 0)";
//     expected[30] = "Arith(ADD, #1, Arith(DIV, Abs(Arith(SUB, #6, Arith(MUL, #7, 100))), #7))";
//     expected[31] = "Cmp(GT, If(Cmp(GT, #6, 0), Arith(DIV, abs(Arith(SUB, #5, #6)), #6), 0.000000), 0.000000)";
//     expected[32] = "CAST('1994-01-01')";
//     expected[33] = "LIKE(#2, '.*green.*')";


//     for (int i = 0; i < numTests; i++) {
//     // for (int i = 32; i <= 32; i++) {
//         std::cout << "simpleTest[" << i << "]" << std::endl;
//         std::cout << "RowExpression:::" << simpleTest[i] << std::endl;
//         Expr* result = parserObj.parseRowExpression(simpleTest[i], inputTypes, vecCount);
//         std::cout << "ExprType:::" << exprTypeString(result->getType()) << std::endl;
//         std::cout << "DataType:::" << dataTypeString(result->dataType) << std::endl;
//         std::cout << "______________________________________" << std::endl;
//         std::cout << " Final expression tree:::";
//         result->printExprTree();
//         std::cout << std::endl << "Expect expression tree:::" << expected[i] << std::endl;
//         std::cout << "______________________________________" << std::endl << std::endl;
//         delete result;
//     }
// }