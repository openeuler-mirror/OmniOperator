#ifndef __SIMPLEOPTIMIZER_H__
#define __SIMPLEOPTIMIZER_H__
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/Error.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

class SimpleOptimizer {
public:
    SimpleOptimizer(unsigned OptLevel) { pmb.OptLevel = OptLevel; }

    llvm::Expected<llvm::orc::ThreadSafeModule>
    operator()(llvm::orc::ThreadSafeModule TSM,
               const llvm::orc::MaterializationResponsibility &);

private:
    llvm::PassManagerBuilder pmb;
};
#endif
