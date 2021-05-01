#ifndef __FILTER_H__
#define __FILTER_H__

#include "../../codegen/codegen.h"

class Filter: public Codegen{
    public:
        void preloop(Generator* generator, Table* table){};
        void inloop(Generator* generator, Table* table, int rowIndex);
        void postloop(Generator* generator, Table* table){};
        
};

#endif