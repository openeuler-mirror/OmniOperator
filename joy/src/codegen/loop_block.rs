use crate::codegen::generator::{Generator, InSituFunction};
use inkwell::basic_block::BasicBlock;
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::values::{FunctionValue, InstructionValue, IntValue, PointerValue};
use inkwell::IntPredicate;

// Represent a for loop
// for i in 0..<row_count> { }
//
#[derive(Copy, Clone)]
pub struct LoopBlock<'ctx> {
    condition: CodeBlock<'ctx>,
    pub body: CodeBlock<'ctx>,
    pub increment: CodeBlock<'ctx>,
    pub end: CodeBlock<'ctx>,
    pub index_addr: Option<PointerValue<'ctx>>,
    pub index: Option<IntValue<'ctx>>,
}

#[derive(Copy, Clone)]
pub struct CodeBlock<'ctx> {
    block: BasicBlock<'ctx>,
    pub last_instruction: Option<InstructionValue<'ctx>>,
}

impl<'ctx> CodeBlock<'ctx> {
    pub fn new(
        block: BasicBlock<'ctx>,
        last_instruction: Option<InstructionValue<'ctx>>,
    ) -> CodeBlock<'ctx> {
        CodeBlock {
            block,
            last_instruction,
        }
    }
    pub fn block(&self) -> BasicBlock<'ctx> {
        self.block
    }
}

impl<'ctx> LoopBlock<'ctx> {
    pub fn append(
        compiler: &Generator<'ctx>,
        function: FunctionValue<'ctx>,
        entry_blk: BasicBlock<'ctx>,
    ) -> LoopBlock<'ctx> {
        let condition_blk = compiler.context.append_basic_block(function, "condition");
        let body_blk = compiler.context.append_basic_block(function, "body");
        let increment_blk = compiler.context.append_basic_block(function, "increment");
        let end_blk = compiler.context.append_basic_block(function, "end");

        let mut loopblock = LoopBlock {
            condition: CodeBlock {
                block: condition_blk,
                last_instruction: None,
            },
            body: CodeBlock {
                block: body_blk,
                last_instruction: None,
            },
            increment: CodeBlock {
                block: increment_blk,
                last_instruction: None,
            },
            end: CodeBlock {
                block: end_blk,
                last_instruction: None,
            },
            index_addr: None,
            index: None,
        };

        compiler.builder.position_at_end(entry_blk);

        loopblock.init_condition_block(compiler, 1024);
        loopblock.init_empty_body_block(compiler);
        loopblock.init_increment_block(compiler);

        loopblock
    }
    fn init_condition_block(&mut self, compiler: &Generator<'ctx>, rowcount: i32) {
        let context = compiler.context;
        let builder = &compiler.builder;
        //create index variable;
        let index_addr = builder.build_alloca(context.i32_type(), "index_var");

        //initialize index varible to 0;
        builder.build_store(index_addr, context.i32_type().const_int(0, false));
        self.index_addr = Some(index_addr);
        builder.build_unconditional_branch(self.condition.block());

        builder.position_at_end(self.condition.block());
        let index = builder.build_load(index_addr, "index").into_int_value();
        self.index = Some(index);
        let cond = builder.build_int_compare(
            IntPredicate::SLT,
            index,
            context.i32_type().const_int(rowcount as u64, false),
            "loop_exit_condition",
        );
        self.end.last_instruction =
            Some(builder.build_conditional_branch(cond, self.body.block(), self.end.block()));
    }

    fn init_empty_body_block(&mut self, compiler: &Generator<'ctx>) {
        let builder = &compiler.builder;
        builder.position_at_end(self.body.block());
        self.body.last_instruction =
            Some(builder.build_unconditional_branch(self.increment.block()));
    }

    fn init_increment_block(&mut self, compiler: &Generator<'ctx>) {
        let builder = &compiler.builder;
        let context = &compiler.context;
        let i32_type = context.i32_type();

        builder.position_at_end(self.increment.block);
        let index_inc = builder.build_int_add(
            self.index.unwrap(),
            i32_type.const_int(1, false),
            "increment idx",
        );

        builder.build_store(self.index_addr.unwrap(), index_inc);
        self.increment.last_instruction =
            Some(builder.build_unconditional_branch(self.condition.block()));
    }

    pub fn condition(&self) -> CodeBlock<'ctx> {
        self.condition
    }
}
