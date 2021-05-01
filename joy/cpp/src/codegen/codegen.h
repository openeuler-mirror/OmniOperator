#ifndef __CODEGEN_H__
#define __CODEGEN_H__
#include "../data/table.h"
#include "generator.h"

// this file equivalent to src/codegen/mod.rs
class Codegen {
    public:
        virtual void preloop(Generator* generator, Layout* table) = 0;
        virtual void inloop(Generator* generator, Layout* table, int rowIndex) = 0;
        virtual void postloop(Generator* generator, Layout* table) = 0;
};

class InSituOp {
    public:
        virtual void add_input() = 0;
        virtual void get_output() = 0;
};

#endif