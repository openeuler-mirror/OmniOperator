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
    AggregationCommonOperator(std::vector<Aggregator*> aggs) : aggregators(aggs) { }
    virtual ~AggregationCommonOperator() {}
protected:
    std::vector<Aggregator*> aggregators;
    int inputRaw;
    int outputPartial;
};

class AggregationCommonOperatorFactory : public OperatorFactory {
public:
    AggregationCommonOperatorFactory(bool inputRaw, bool outputPartial) : inputRaw(inputRaw), outputPartial(outputPartial) {}
    virtual ~AggregationCommonOperatorFactory() {}

protected:
    int inputRaw;
    int outputPartial;
};
}
}
#endif