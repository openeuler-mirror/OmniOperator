//
// Created by kkrazy on 2021-03-23.
//
#include "hammer.h"

#ifndef JOY_SORT_HAMMER_H
#define JOY_SORT_HAMMER_H

namespace omniruntime {
namespace codegen {

class SortHammer : public Hammer {
public:
    std::unique_ptr<LLJIT> create_jitter(std::list<Hammer *> deps = std::list<Hammer *>(),
                                            CodeGenOpt::Level level = CodeGenOpt::Aggressive);
};
} // end of namespace codegen
} // end of namespace omniruntime
#endif //JOY_SORT_HAMMER_H
