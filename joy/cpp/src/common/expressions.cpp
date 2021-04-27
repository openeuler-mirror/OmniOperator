#include "expressions.h"

BinaryExpr::BinaryExpr(LogicalOperator logOp, Expr leftExpr, Expr rightExpr){
    op = logOp;
    left = leftExpr;
    right = rightExpr;
} 