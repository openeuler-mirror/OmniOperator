#include "loop_block.h"
#include <optional>

// TODO: figureout cpp equivalent on FunctionValue and BasicBlock
LoopBlock::append(Generator* compiler, FunctionValue function, BasicBlock entry_blk){
    auto condition_blk = compiler.context.append_basic_block(function, "condition");
    auto body_blk = compiler.context.append_basic_block(function, "body");
    auto increment_blk = compiler.context.append_basic_block(function, "increment");
    auto end_blk = compiler.context.append_basic_block(function, "end");

    auto loopblock = LoopBlock {
        CodeBlock(condition_blk, std::optioanl<InstructionValue>{}),
        CodeBlock(body_blk, std::optioanl<InstructionValue>{}),
        CodeBlock(condition_blk, std::optioanl<InstructionValue>{}),
        CodeBlock(condition_blk, std::optioanl<InstructionValue>{}),
        index_addr: None,
        index: None,
    };

    compiler.builder.position_at_end(entry_blk);

    loopblock.init_condition_block(compiler, 1024);
    loopblock.init_empty_body_block(compiler);
    loopblock.init_increment_block(compiler);

    loopblock
}

// TODO: complete the migration