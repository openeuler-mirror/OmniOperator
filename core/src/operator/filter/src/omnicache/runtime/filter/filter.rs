use std::any::Any;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::time::Instant;

use inkwell::{AddressSpace, FloatPredicate, IntPredicate, OptimizationLevel};
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::execution_engine::JitFunction;
use inkwell::module::Module;
use inkwell::passes::PassManager;
use inkwell::types::{AnyType, BasicTypeEnum};
use inkwell::values::{BasicValue, BasicValueEnum, FloatValue, FunctionValue, IntValue, PointerValue, VectorValue};

use crate::omnicache::runtime::filter::compiler::{Compiler, Data, Expr};
use crate::omnicache::runtime::relational_types::{RelationBuilder, RelationContext};
use crate::omnicache::runtime::table::{ColumnBuilder, Layout, Table};

pub type FilterFuncType = unsafe extern "C" fn(*const c_void, usize) -> bool;

pub struct Filter;

impl Filter {
    pub fn compile<'ctx>(expression: &Expr, context: &'ctx Context, input_types: &Vec<i32>) -> JitFunction<'ctx, FilterFuncType> {
        // TODO: fix hardcoded column values
        let mut columns: Vec<Box<dyn Any>> = Vec::new();
        for vec_type in input_types {
            match vec_type {
                1 => {
                    let column = ColumnBuilder::ColumnI32("c1", ManuallyDrop::new(vec![1i32]));
                    columns.push(Box::new(column));
                }
                2 => {
                    // i64
                    let column = ColumnBuilder::ColumnI64("c1", ManuallyDrop::new(vec![1i64]));
                    columns.push(Box::new(column));
                }
                3 => {
                    // f64
                    let column = ColumnBuilder::ColumnF64("c3", ManuallyDrop::new(vec![1.0f64]));
                    columns.push(Box::new(column));
                }
                _ => {
                    panic!("Unsupported input type");
                }
            }
        }
        let table = Table::new("test_table", columns);

        let module = context.create_module("filter");
        let builder = context.create_builder();
        let mut layout = context.table_layout(&table);

        // Create FPM
        let fpm = PassManager::create(&module);

        fpm.add_instruction_combining_pass();
        fpm.add_reassociate_pass();
        fpm.add_gvn_pass();
        fpm.add_cfg_simplification_pass();
        fpm.add_basic_alias_analysis_pass();
        fpm.add_promote_memory_to_register_pass();
        fpm.add_instruction_combining_pass();
        fpm.add_reassociate_pass();

        fpm.initialize();

        let now = Instant::now();
        match Compiler::compile(context, &builder, &fpm, &module, &mut layout, &expression) {
            Ok(function) => {
                // println!("-> Expression compiled to IR:");
                // function.print_to_stderr();
            }
            Err(err) => {
                panic!("!> Error compiling function: {}", err);
            }
        }

        let ee = module.create_jit_execution_engine(OptimizationLevel::None).unwrap();
        let maybe_fn = unsafe { ee.get_function::<FilterFuncType>("filter") };

        let elapsed = now.elapsed();
        println!("Compile elapsed: {}", elapsed.as_micros());

        let compiled_fn = match maybe_fn {
            Ok(f) => f,
            Err(err) => {
                println!("!> Error during execution: {:?}", err);
                panic!()
            }
        };
        compiled_fn
    }
}