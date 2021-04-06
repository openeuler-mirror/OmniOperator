use crate::codegen::loop_block::{CodeBlock, LoopBlock};
use crate::codegen::relational_types::{RelationBuilder, RelationContext};
use crate::codegen::CodeGen;
use crate::data::table::{ColumnF64, ColumnI16, ColumnI32, ColumnI64, Layout, Table};
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::execution_engine::{ExecutionEngine, JitFunction};
use inkwell::module::{Linkage, Module};
use inkwell::types::{BasicType, BasicTypeEnum, FloatType, FunctionType, PointerType, StructType};
use inkwell::values::{
    BasicValue, BasicValueEnum, FloatValue, FunctionValue, IntValue, PointerValue,
};
use inkwell::{AddressSpace, OptimizationLevel};
use std::cmp::min;
use std::ffi::c_void;

const TABLE_PARAM_INDEX: u32 = 0;

type InSituOpFunc = unsafe extern "C" fn(*const c_void) -> *const c_void;

pub struct Generator<'ctx> {
    pub context: &'ctx Context,
    pub builder: Builder<'ctx>,
    pub module: Module<'ctx>,
    pub exec_engine: ExecutionEngine<'ctx>,
    pub name: &'ctx str,
    pub function: Option<InSituFunction<'ctx>>,
}

#[derive(Copy, Clone)]
pub struct InSituFunction<'ctx> {
    pub function: FunctionValue<'ctx>,
    pub function_ty: FunctionType<'ctx>,
    pub entry: CodeBlock<'ctx>,
    pub forloop: LoopBlock<'ctx>,
}

pub trait InSituDebug {
    fn debug_func(&self) -> String;
}

impl<'ctx> InSituDebug for IntValue<'ctx> {
    fn debug_func(&self) -> String {
        let width = self.get_type().get_bit_width();
        format!("i{}_debug", width)
    }
}

impl<'ctx> InSituDebug for FloatValue<'ctx> {
    fn debug_func(&self) -> String {
        //FIXME: default to 64bit before we find a good way to find ot the bit width
        let width = 64;
        format!("f{}_debug", width)
    }
}

impl<'ctx> Generator<'ctx> {
    //call each in-situ operator to generate the code for pre_loop
    fn insert_pre_loop(
        &self,
        function: InSituFunction,
        layout: &Layout,
        op_stub: &Box<dyn CodeGen>,
    ) {
        self.builder
            .position_at_end(self.function.unwrap().entry.block());
        op_stub.as_ref().preloop(&self, layout);
    }

    //call each in-situ operator to generate the code for in_loop
    fn insert_in_loop(
        &self,
        function: InSituFunction,
        layout: &Layout,
        op_stub: &Box<dyn CodeGen>,
    ) {
        //FIXME: can be simplified
        self.builder.position_before(
            &self
                .function
                .unwrap()
                .forloop
                .body
                .last_instruction
                .unwrap(),
        );

        let row_index = self.function.unwrap().forloop.index.unwrap();
        op_stub.as_ref().inloop(&self, layout, row_index);
    }

    //call each in-situ operator to geneate the code for post loop
    fn insert_post_loop(&self, function: InSituFunction, layout: &Layout, stub: &Box<dyn CodeGen>) {
        stub.as_ref().postloop(&self, layout);
    }

    //returns a pointer to the table parameter passed into the function
    pub fn get_table_param(&self) -> PointerValue<'ctx> {
        self.function
            .unwrap()
            .function
            .get_nth_param(TABLE_PARAM_INDEX)
            .unwrap()
            .into_pointer_value()
    }

    //final logic to prepare the return value of the function
    fn gen_return(&self) {
        self.builder
            .position_at_end(self.function.unwrap().forloop.end.block());
        let ret_value = self.context.i32_type().const_int(255, false);
        self.builder.build_return(Some(&ret_value));
    }

    //creates a function with an empty loop
    fn init_func_with_loop(&self, args: StructType<'ctx>) -> InSituFunction<'ctx> {
        let func_ty = self
            .context
            .i64_type()
            .fn_type(&[args.ptr_type(AddressSpace::Generic).into()], false);
        let func = self.module.add_function(self.name, func_ty, None);

        //func() {}
        let func_blk = self.context.append_basic_block(func, "entry");

        //
        // func(){
        // ... preloop_generation ...
        // for.. { ... inloop_generation ... }
        // ... postloop_generation ...
        //
        let mut forloop = LoopBlock::append(self, func, func_blk);

        self.builder.position_at_end(forloop.end.block());

        InSituFunction {
            forloop,
            function_ty: func_ty,
            function: func,
            entry: CodeBlock::new(func_blk, None),
        }
    }

    pub fn build_debug(&self, msg: &str, value: BasicValueEnum) {
        let mut fn_name = None;
        let str_arg_ty = self.context.i8_type().ptr_type(AddressSpace::Generic);
        if (value.get_type().is_int_type()) {
            fn_name = Some(value.into_int_value().debug_func());
        }
        if (value.get_type().is_float_type()) {
            fn_name = Some(value.into_float_value().debug_func());
        }

        if fn_name == None {
            dbg!("unpported data type for debug", value.get_type());
            return;
        }

        let func_name_str = fn_name.expect("error getting function name");
        let func = self
            .module
            .get_function(func_name_str.as_str())
            .expect(format!("error getting debug func: {}", func_name_str.as_str()).as_str());

        let msg_arg = unsafe { self.builder.build_global_string(msg, "arg0") }.as_pointer_value();
        let str_arg = self.builder.build_pointer_cast(msg_arg, str_arg_ty, "arg0");

        let args = &[str_arg.as_basic_value_enum(), value.as_basic_value_enum()];
        self.builder.build_call(func, args, "debug");
    }

    pub fn init(&self) {
        let void_type = self.context.void_type();
        let i8_type = self.context.i8_type();
        let i16_type = self.context.i16_type();
        let i32_type = self.context.i32_type();
        let i64_type = self.context.i64_type();
        let f64_type = self.context.f64_type();

        //FIXME: only support 20 characters in the debug msg
        let string_type = i8_type.ptr_type(AddressSpace::Generic);

        // let args = &[string_type.as_basic_type_enum()];
        // let fn_type = void_type.fn_type(args, false);
        // self.module
        //     .add_function("puts", fn_type, Some(Linkage::Common));

        let args = &[
            //string_type.as_basic_type_enum(),
            i16_type.as_basic_type_enum(),
        ];
        let fn_type = void_type.fn_type(args, false);
        self.module
            .add_function("i16_debug", fn_type, Some(Linkage::Common));

        let args = &[
            //string_type.as_basic_type_enum(),
            i32_type.as_basic_type_enum(),
        ];
        let fn_type = void_type.fn_type(args, false);
        self.module
            .add_function("i32_debug", fn_type, Some(Linkage::Common));

        let args = &[
            //string_type.as_basic_type_enum(),
            i64_type.as_basic_type_enum(),
        ];
        let fn_type = void_type.fn_type(args, false);
        self.module
            .add_function("i64_debug", fn_type, Some(Linkage::Common));

        let args = &[
            //string_type.as_basic_type_enum(),
            f64_type.as_basic_type_enum(),
        ];
        let fn_type = void_type.fn_type(args, false);
        self.module
            .add_function("f64_debug", fn_type, Some(Linkage::Common));

        // let mut param_ty = [LLVMInt32Type()];
        // let func_ty = LLVMFunctionType(LLVMVoidType(), param_ty.as_mut_ptr(), 1, 0);
        // let d_func = LLVMAddFunction(self.module, b"dbg\0".as_ptr() as *const _, func_ty);
    }

    pub fn generate(&mut self, layout: &mut Layout, op_stub: &Box<dyn CodeGen>) -> &Module<'ctx> {
        let args = self.context.table_struct_type(layout);
        let function = self.init_func_with_loop(args);
        self.function = Some(function);

        self.insert_pre_loop(function, layout, op_stub);
        self.insert_in_loop(function, layout, op_stub);
        self.insert_post_loop(function, layout, op_stub);
        self.gen_return();

        self.module.verify();
        &self.module
    }

    pub fn start(name: &str, table: &Table, stub: &Box<dyn CodeGen>) {
        unsafe {
            super::builtins::init();
        }
        let name = "test_main";

        let context = Context::create();
        let module = context.create_module(name);
        let builder = context.create_builder();
        let mut layout = context.table_layout(table);
        let exec_engine = module
            .create_jit_execution_engine(OptimizationLevel::Aggressive)
            .unwrap();
        let mut generator = Generator {
            context: &context,
            builder,
            module,
            exec_engine,
            name,
            function: None,
        };

        generator.init();

        let module = generator.generate(&mut layout, stub);
        module.print_to_stderr();
        unsafe {
            let func: JitFunction<InSituOpFunc> = generator
                .exec_engine
                .get_function(name)
                .expect("error generating in-situ operator");

            let x = table.into_ffi_args();
            dbg!("about to call..");
            dbg!(func.call(x.as_ptr() as *const c_void));
        }
    }
}
