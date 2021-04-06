//
// Created by kkrazy on 2021-03-23.
//
#include "Hammer.h"

#ifndef JOY_SORT_HAMMER_H
#define JOY_SORT_HAMMER_H

namespace codegen {
    class SortHammer : public Hammer {
    public:
        std::unique_ptr<LLJIT> create_jitter(std::list<Hammer *> deps = std::list<Hammer *>(),
                                             CodeGenOpt::Level level = CodeGenOpt::Aggressive);
    };
};

#endif //JOY_SORT_HAMMER_H
