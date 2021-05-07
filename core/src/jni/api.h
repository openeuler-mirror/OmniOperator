#ifndef __API_H__
#define __API_H__
#include "../jit/hammer.h"
#include "../util/debug.h"
#include "../operator/aggregator/hash_groupby.h"
#include "../util/op_template_cache.h"

#include <thread>
#ifdef DEBUG
#include <string>
#endif

typedef void (*jit_module)(HashGroupBy*, Table*);
typedef HashGroupBy* (*opt_module) (std::vector<ColumnIndex>& groupByIndex, 
                                    std::vector<ColumnIndex>& aggIndex, 
                                    std::vector<Aggregator*>& aggs);

typedef struct JitContext
{
    LLJIT* jitter;
    opt_module func;
} JitContext;

typedef struct OperatorContext
{
    uint64_t jitAddr;
    uint64_t operatorAddr;
} OperatorContext;

uint64_t prepareHashGroupBy(PrepareContext groupByCols, 
                        PrepareContext groupByTypes, 
                        PrepareContext aggCols, 
                        PrepareContext aggTypes, 
                        PrepareContext aggFuncTypes,
                        PrepareContext retTypes);

void executeHashGroupByIntermediate(int64_t opId, Table* table);

uint64_t createOperator(int64_t moduleAddr,
                        PrepareContext groupByCols, 
                        PrepareContext groupByTypes, 
                        PrepareContext aggCols, 
                        PrepareContext aggTypes, 
                        PrepareContext aggFuncTypes,
                        PrepareContext retTypes);

uint64_t executeHashGroupByLlvm(int64_t operatorAddr, 
                                uint32_t* colTypes, 
                                uint32_t typeCount, 
                                void** t, 
                                uint32_t columnCount, 
                                uint32_t rowNum);

Table* executeAggFinal(int64_t opAddr);
int32_t executeAggFinal(int64_t opAddr, std::vector<Table*>& result); 
#endif