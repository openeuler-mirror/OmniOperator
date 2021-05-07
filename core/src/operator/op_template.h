#ifndef __OP_TEMPLATE_H__
#define __OP_TEMPLATE_H__

#include "../vector/table.h"

class OpTemplate {
public:
    OpTemplate() {}
    virtual void preloop(Table* table) = 0;
    virtual void inloop(Table* table, uint32_t rowIdx) = 0;
    virtual void postloop(Table* table) = 0;
    // process page
    virtual void process(Table*, uint32_t) = 0;
    // construct result
    virtual Table* getResult() = 0;
    virtual ~OpTemplate(){}
};

#endif