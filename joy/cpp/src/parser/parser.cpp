#include <stdio.h>
#include <iostream>
#include <cstring>
#include <stack>          // std::stack
using namespace std;

enum LogicalOperator { AND, OR };
enum ComparisionOperator { EQ, NEQ, LT, LTE, GT, GTE};
class Expr {

};

class BinaryExpr: public Expr {
    public:
        LogicalOperator op;
        Expr left;
        Expr right;
};

class ComparisionExpr: public Expr {
    public:
        ComparisionOperator op;
        int columnIdx;
        int columnData;

}

class Parser {

public:

    Expr parseRowExpression(string input) {
        int n = input.length();
        BinaryExpr root;
        // declaring character array
        char char_array[n + 1];
        bool shouldBeRight = false;
 
    // copying the contents of the
    // string to char array
        strcpy(char_array, input.c_str());
        stack<BinaryExpr> exprStack;
        printf("inside parser:::");
        int lastIdx = 0;
        for (int i = 0; i < n; i++) {
            char ch = char_array[i];
            switch(ch) {
                case '(':
                    string opStr = input.substr(lastIdx, i);
                    if(opStr == "AND") {
                        BinaryExpr expr;
                        expr.op = LogicalOperator::AND;
                        root = expr;
                        exprStack.push(expr);
                        lastIdx = i+1;
                    } else if(opStr.find("operator") == string::npos) {
                        string op = opStr.substr(10);
                        if (op == "EQ" || op == "LT" || op == "GT" || op == "LTE" || op == "GTE") {
                            i = i + 1;
                            ch = char_array[i];
                            int startIdx = i;
                            string result;
                            while ( ch != ')') {
                                result = result.append("" + ch);
                                i++;
                                ch = char_array[i];
                            }
                            
                            ComparisionExpr expr = generateComparisionExpr(opStr, startIdx, i);
                            return expr;
                            
                        }
                    } else {
                        string opStr = opStr.substr(lastIdx, i);

                        BinaryExpr parent = (BinaryExpr) exprStack.pop();
                        if(opStr == "AND") {
                            if(shouldBeRight) {
                                BinaryExpr newExpr;

                                parent.right = newExpr;
                                exprStack.push(parent);
                                exprStack.push(newExpr);
                                shouldBeRight = false;
                            } else {
                                BinaryExpr newExpr;

                                parent.left = newExpr;
                                exprStack.push(parent);
                                exprStack.push(newExpr);
                                shouldBeRight = false;
                            }

                        } else if(opStr.find("operator") > 0) {
                            string op = opStr.substr(10);
                            if (op == "EQ" || op == "LT" || op == "GT" || op == "LTE" || op == "GTE") {
                                i = i + 1;
                                ch = char_array[i];
                                int startIdx = i;
                                string result;
                                while ( ch != ')') {
                                    result = result.append("" + ch);
                                    i++;
                                    ch = char_array[i];
                                }
                                
                                ComparisionExpr expr = generateComparisionExpr(opStr, startIdx, i);
                                return expr;
                                
                            }
                        }
                    break;
                case ')':
                    break;
                case ' ':
                    break;
                case ',':
                    break;         
            }
        }
    }

    ComparisionExpr generateComparisionExpr(string exprStr, int startIdx, int endIdx) {
        string cmprStr = exprStr.substr(startIdx, endIdx);
        int position = cmprStr.find_first_of(",");
        string fieldIdx = cmprStr.substr(0, position);
        string fieldData = cmprStr.substr(position);
        ComparisionExpr expr;
        expr.op = ComparisionOperator::EQ;
        expr.columnIdx = stoi(fieldIdx);
        expr.columnData = stoi(fieldData);
        return expr;
    }

};


int main() {
    Parser parserObj;

    parserObj.parseRowExpression("AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))");
    return 0;
}