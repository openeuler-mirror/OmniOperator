#include "../harden/hammer.h"
#include <thread>
#include <string>
#include "../operator/hash_groupby.h"
#include "../util/op_template_cache.h"
#ifndef __API_H__
#define __API_H__
typedef void (*jit_module)(HashGroupBy*, Table*);
typedef HashGroupBy* (*opt_module) (std::vector<ColumnIndex>& groupByIndex, 
                                    std::vector<ColumnIndex>& aggIndex, 
                                    std::vector<Aggregator*>& aggs,
                                    OpTemplateCache<HashGroupBy*>& operatorCache,
                                    std::string& opId);

typedef struct JitContext
{
    LLJIT* jitter;
    opt_module func;
} JitContext;
void prepareHashGroupBy(std::string opId, 
                        PrepareContext groupByCols, 
                        PrepareContext groupByTypes, 
                        PrepareContext aggCols, 
                        PrepareContext aggTypes, 
                        PrepareContext aggFuncTypes,
                        PrepareContext retTypes);

void executeHashGroupByIntermediate(std::string opId, Table* table);
void executeHashGroupByLlvm(std::string opId, uint32_t* colTypes, uint32_t typeCount, void** table, uint32_t colCount, uint32_t rowNum);

Table* executeAggFinal(std::string opId);
#endif