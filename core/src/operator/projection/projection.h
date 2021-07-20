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
        delete this->expr;
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
    int64_t* getData(VectorBatch* &vecBatch, vector<int64_t *> &vcdataVec, vector<char *> &stringvalVec, bool* bitmap);

    // projector function is retrieved from ProjectionCodeGen
    // projector(data, rowCount, selectedRows, numSelectedRows, bitmap)
    // data: 2D array containing vector values
    // rowCount: number of rows in data
    // selectedRows: array of row numbers which pass the filter
    // numSelectedRows: number of rows which pass the filter
    // bitmap: boolean array where bitmap[numCols * row + col] is true if data[row][col] is null
    int32_t (*projector)(int64_t*, int32_t, int64_t, int32_t*, int32_t, bool*);
};

class ProjectionOperator : public Operator {

public:
    ProjectionOperator(Projection** proj, int32_t* inputTypes, int32_t nCols, int32_t nProj) :
    proj(proj), nCols(nCols), nProj(nProj) {
        this->sourceTypes = inputTypes;
        this->mutated = nullptr;
    }
    ~ProjectionOperator() {
    }

    int32_t AddInput(VectorBatch* vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch*>& ret) override;

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
    ~ProjectionOperatorFactory();
    omniruntime::op::Operator* CreateOperator() override;

private:
    int32_t* inputTypes;
    int32_t nCols;
    Projection** proj;
    int32_t nProj;
};

}
}

#endif