#ifndef __LOOP_BLOCK_H__
#define __LOOP_BLOCK_H__

#include <optional>

// rust stated as struct but I think class might be more suitable in cpp
// TODO:  complete migration from rust
class CodeBlock {
    private:
        BasicBlock block;
        std::optional<InstructionValue> last_instruction;
    public:
        CodeBlock(BasicBlock blk, std::optional<InstructionValue> last_ins){
            block = blk;
            last_instruction = last_ins;
        };
};

class LoopBlock {
    private:
        CodeBlock condition;
        CodeBlock body;
        CodeBlock increment;
        CodeBlock end;
        std::optional<int64_t> index_addr;
        std::optional<int32_t> index;
    public:
        LoopBlock(CodeBlock condition, CodeBlock body, CodeBlock increment, CodeBlock end, std::optional<int64_t )
        // TODO figure out function value and basicblock in cpp
        LoopBlock append(Generator* compiler, FunctionValue function, BasicBlock entry_blk);
        

}


#endif