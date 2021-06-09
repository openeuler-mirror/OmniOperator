#ifndef __AGGREGATION_H__
#define __AGGREGATION_H__

#include "../operator_factory.h"
#include "aggregator.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"

#include <vector>
#include <stdint.h>
#include <thread>

namespace omniruntime {
namespace op {

class AggregationCommonOperator : public Operator {
public:
    AggregationCommonOperator(std::vector<Aggregator*> aggs) : aggregators(aggs) {}
    virtual ~AggregationCommonOperator() {}
protected:
    std::vector<Aggregator*> aggregators;
    
    void allocateVec(Table* table, const int32_t* types, const int32_t startIndex, const bool isAgg, const int32_t colSize, const int64_t rowSize)
    {
        if (table == nullptr || types == nullptr || colSize <= 0 || rowSize <= 0) {
            DebugError("Input parameters are invalid: colSize = %d, rowSize = %ld", colSize, rowSize);
            return;
        }
        for (int32_t i = 0; i < colSize; ++i) {
            if (isAgg) {
                if (aggregators[i]->getType() == COUNT) {
                    int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowSize * sizeof(int64_t)));
                    table->setColumn(new Column(c, INT64, rowSize), INT64);
                    continue;
                }
                if (aggregators[i]->getType() == AVG) {
                    double* c = reinterpret_cast<double*>(omni_allocate(rowSize * sizeof(double)));
                    table->setColumn(new Column(c, DOUBLE, rowSize), DOUBLE);
                    continue;
                }
            }
            switch (types[startIndex + i])
            {
                case 1: {
                    int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(rowSize * sizeof(int32_t)));
                    table->setColumn(new Column(c, INT32, rowSize), INT32);
                    break;
                }
                case 2: {
                    int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowSize * sizeof(int64_t)));
                    table->setColumn(new Column(c, INT64, rowSize), INT64);
                    break;
                }
                case 3: {
                    double* c = reinterpret_cast<double*>(omni_allocate(rowSize * sizeof(double)));
                    table->setColumn(new Column(c, DOUBLE, rowSize), DOUBLE);
                    break;
                }
                default: {
                    DebugError("Type %d doesn't support", types[i]);
                    break;
                }
            }
        }
    }
};

}
}
#endif