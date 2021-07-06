#ifndef __PARSER_H__
#define __PARSER_H__

#ifndef __EXPRESSION_H__
#include "../expressions.h"
#endif
#include <string>
#include <cstring>

using namespace std;
using namespace omniruntime::expressions;

enum OperatorReturnType {
    COMPARISON, 
    LOGICAL, 
    ARITHMETIC, 
    INVALIDRETURNTYPE
};



class Parser
{
public:
    Expr *parseRowExpression(string input, int32_t *inputTypes, int32_t veccount);
    DataExpr* generateData(string dataStr, int32_t *inputTypes, int32_t vecCount);
    static DataType colTypeTrans(int32_t colType) {
        switch(colType) {
            case 1:
                return DataType::INT32D;
            case 2:
                return DataType::INT64D;
            case 3:
                return DataType::DOUBLED;
                // not yet supported in table
            case 4 :
                return DataType::STRINGD;
        }
        return DataType::INVALIDDATAD;
    }
};


#endif