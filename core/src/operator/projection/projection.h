#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include <cstdint>
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"
#include "../../codegen/projection_codegen.h"
#include "../operator_factory.h"
#include "../operator.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_allocator_manager.h"

namespace omniruntime {
namespace op {

class Projection {
public:
    Projection(int32_t* inputTypes, int32_t nCols, std::string expr, bool filter);
    Projection(int32_t* inputTypes, int32_t nCols, Expr* expr, bool filter);
    ~Projection() {
        delete this->codegen;
    }

    Vector* project(VectorBatch* vecBatch, int32_t* selected, int32_t nSelected);
    Vector* project(VectorBatch* vecBatch);
    DataType getOutputType() {return this->expr->getExprDataType();}

private:
    int32_t* inputTypes;
    int32_t nCols;
    Expr* expr;
    ProjectionCodeGen* codegen;
    int32_t (*projector)(int64_t*, int32_t, int64_t, int32_t*, int32_t);
};

class ProjectionOperator : public Operator {

public:
    ProjectionOperator(Projection** proj, int32_t* inputTypes, int32_t nCols, int32_t nProj) :
    proj(proj), nCols(nCols), nProj(nProj) {
        this->sourceTypes = inputTypes;
        this->mutated = nullptr;
    }
    ~ProjectionOperator() {
        if (this->mutated != nullptr) delete this->mutated;
    }

    int32_t addInput(VectorBatch* vecBatch) override;
    int32_t getOutput(std::vector<VectorBatch*>& ret) override;

private:
    Projection** proj;
    int32_t nCols;
    int32_t nProj;
    VectorBatch* mutated;
};

class ProjectionOperatorFactory : public OperatorFactory {

public:
    ProjectionOperatorFactory(std::string* expression, int32_t nProj, int32_t* inputTypes, int32_t nCols);
    ProjectionOperatorFactory(Expr** exprs, int32_t nProj, int32_t* inputTypes, int32_t nCols);
    omniruntime::op::Operator* createOperator() override;

private:
    int32_t* inputTypes;
    int32_t nCols;
    Projection** proj;
    int32_t nProj;
};

}
}

#endif