use std::borrow::Borrow;
use std::collections::HashMap;
use std::ffi::c_void;
use std::time::Instant;

use inkwell::{AddressSpace, FloatPredicate, IntPredicate, OptimizationLevel};
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::module::Module;
use inkwell::passes::PassManager;
use inkwell::types::{AnyType, BasicTypeEnum};
use inkwell::values::{BasicValue, BasicValueEnum, FloatValue, FunctionValue, IntValue, PointerValue, VectorValue};

use crate::omnicache::runtime::relational_types::{RelationBuilder, RelationContext};
use crate::omnicache::runtime::table::{ColumnBuilder, Layout, Table};
use std::mem::ManuallyDrop;

#[derive(Debug)]
pub struct Prototype {
    pub name: String,
    pub args: Vec<String>,
}

pub enum Expr {
    Binary {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    Constant(Data),

    Variable(String),

    InputReference(u32),
}

pub enum Data {
    Int(i32),
    Long(i64),
    Float(f64),
}

pub struct Compiler<'a, 'ctx> {
    pub context: &'ctx Context,
    pub builder: &'a Builder<'ctx>,
    pub fpm: &'a PassManager<FunctionValue<'ctx>>,
    pub module: &'a Module<'ctx>,
    pub layout: &'a mut Layout<'ctx>,
    pub expression: &'a Expr,

    variables: HashMap<String, PointerValue<'ctx>>,
    fn_value_opt: Option<FunctionValue<'ctx>>,
}

impl<'a, 'ctx> Compiler<'a, 'ctx> {
    /// Gets a defined function given its name.
    #[inline]
    fn get_function(&self, name: &str) -> Option<FunctionValue<'ctx>> {
        self.module.get_function(name)
    }

    /// Returns the `FunctionValue` representing the function being compiled.
    #[inline]
    fn fn_value(&self) -> FunctionValue<'ctx> {
        self.fn_value_opt.unwrap()
    }

    fn get_variable(&self, name: &str) -> Result<BasicValueEnum<'ctx>, &'static str> {
        match self.variables.get(name) {
            Some(var) => Ok(self.builder.build_load(*var, name)),
            None => Err("Could not find a matching variable.")
        }
    }

    //returns a pointer to the table parameter passed into the function
    pub fn get_nth_param(&self, index: u32) -> BasicValueEnum<'ctx> {
        self.fn_value_opt
            .expect("Function shouldn't be null")
            .get_nth_param(index)
            .expect(format!("{}th param is not null", index).as_str())
    }

    fn compile_expr(&mut self, expr: &Expr) -> Result<BasicValueEnum<'ctx>, &'static str> {
        match expr {
            Expr::InputReference(index) => {
                let table_ptr = self.get_nth_param(0).into_pointer_value();
                let row_index = self.get_nth_param(1).into_int_value();

                let vec = self.builder.build_get_nth_column(table_ptr, *index);
                let value = self.builder.build_get_value(vec, row_index);
                return Ok(value);
            }

            Expr::Constant(data) => {
                return match data {
                    Data::Int(data) => Ok(BasicValueEnum::IntValue(self.context.i32_type().const_int(*data as u64, true))),
                    Data::Long(data) => Ok(BasicValueEnum::IntValue(self.context.i64_type().const_int(*data as u64, true))),
                    Data::Float(data) => Ok(BasicValueEnum::FloatValue(self.context.f64_type().const_float(*data))),
                };
            }

            Expr::Variable(ref name) => {
                match self.variables.get(name.as_str()) {
                    Some(var) => Ok(BasicValueEnum::IntValue(self.builder.build_load(*var, name.as_str()).into_int_value())),
                    None => Err("Could not find a matching variable.")
                }
            }

            Expr::Binary { op, ref left, ref right } => {
                if op == "AND" {
                    let lhs = self.compile_expr(left)?;
                    let rhs = self.compile_expr(right)?;
                    if !lhs.is_int_value() || !rhs.is_int_value() {
                        // println!("lhs: {}, rhs: {}", lhs.get_type().print_to_string(), rhs.get_type().print_to_string());
                        return Err("And can only be applied to logical expressions");
                    }
                    Ok(BasicValueEnum::IntValue(self.builder.build_and(lhs.into_int_value(), rhs.into_int_value(), "tmpand")))
                } else {
                    let lhs = self.compile_expr(left)?;
                    let rhs = self.compile_expr(right)?;

                    return match op.as_ref() {
                        "$operator$LESS_THAN" => {
                            if lhs.is_int_value() && rhs.is_int_value() {
                                let cmp = self.builder.build_int_compare(IntPredicate::ULT, lhs.into_int_value(), rhs.into_int_value(), "tmplt");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else if lhs.is_float_value() && rhs.is_float_value() {
                                let cmp = self.builder.build_float_compare(FloatPredicate::ULT, lhs.into_float_value(), rhs.into_float_value(), "tmplt");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else {
                                println!("lhs {}, rhs {}", lhs.get_type().print_to_string(), rhs.get_type().print_to_string());
                                Err("Invalid value type")
                            }
                        }
                        "$operator$LESS_THAN_OR_EQUAL" => {
                            if lhs.is_int_value() && rhs.is_int_value() {
                                let cmp = self.builder.build_int_compare(IntPredicate::ULE, lhs.into_int_value(), rhs.into_int_value(), "tmplte");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else if lhs.is_float_value() && rhs.is_float_value() {
                                let cmp = self.builder.build_float_compare(FloatPredicate::ULE, lhs.into_float_value(), rhs.into_float_value(), "tmplte");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else {
                                println!("lhs {}, rhs {}", lhs.get_type().print_to_string(), rhs.get_type().print_to_string());
                                Err("Invalid value type")
                            }
                        }
                        "$operator$GREATER_THAN" => {
                            if lhs.is_int_value() && rhs.is_int_value() {
                                let cmp = self.builder.build_int_compare(IntPredicate::UGT, lhs.into_int_value(), rhs.into_int_value(), "tmpgt");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else if lhs.is_float_value() && rhs.is_float_value() {
                                let cmp = self.builder.build_float_compare(FloatPredicate::UGT, lhs.into_float_value(), rhs.into_float_value(), "tmpgt");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else {
                                // println!("lhs {}, rhs {}", lhs.get_type().print_to_string(), rhs.get_type().print_to_string());
                                Err("Invalid value type")
                            }
                        }
                        "$operator$GREATER_THAN_OR_EQUAL" => {
                            if lhs.is_int_value() && rhs.is_int_value() {
                                let cmp = self.builder.build_int_compare(IntPredicate::UGE, lhs.into_int_value(), rhs.into_int_value(), "tmpge");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else if lhs.is_float_value() && rhs.is_float_value() {
                                let cmp = self.builder.build_float_compare(FloatPredicate::UGE, lhs.into_float_value(), rhs.into_float_value(), "tmpge");
                                Ok(BasicValueEnum::IntValue(cmp))
                            } else {
                                Err("Invalid value type")
                            }
                        }
                        _ => {
                            Err("Unsupported operator")
                        }
                    };
                }
            }
        }
    }

    /// Compiles the specified `Prototype` into an extern LLVM `FunctionValue`.
    fn compile_prototype(&mut self, proto: &Prototype) -> Result<FunctionValue<'ctx>, &'static str> {
        let args_types = vec![self.context.table_struct_type(self.layout).ptr_type(AddressSpace::Generic).into(), self.context.i32_type().into()];
        let args_types = args_types.as_slice();

        let fn_type = self.context.bool_type().fn_type(args_types, false);

        let fn_val = self.module.add_function(proto.name.as_str(), fn_type, None);

        // set arguments names
        for (i, arg) in fn_val.get_param_iter().enumerate() {
            arg.set_name(proto.args[i].as_str());
            // arg.into_int_value().set_name(proto.args[i].as_str());
        }

        // finally return built prototype
        Ok(fn_val)
    }

    /// Compiles the specified `Function` into an LLVM `FunctionValue`.
    fn compile_fn(&mut self) -> Result<FunctionValue<'ctx>, &'static str> {
        // let proto = &self.function.prototype;
        let proto = Prototype { name: String::from("filter"), args: vec![String::from("table"), String::from("row_index")] };
        let function = self.compile_prototype(&proto)?;

        let entry = self.context.append_basic_block(function, "entry");

        self.builder.position_at_end(entry);

        // update fn field
        self.fn_value_opt = Some(function);

        // build variables map
        // self.variables.reserve(proto.args.len());

        // for (i, arg) in function.get_param_iter().enumerate() {
        //     let arg_name = proto.args[i].as_str();
        //     let alloca = self.create_entry_block_alloca(arg_name);
        //
        //     self.builder.build_store(alloca, arg);
        //
        //     self.variables.insert(proto.args[i].clone(), alloca);
        // }

        // compile body
        let body = self.compile_expr(&self.expression)?;

        self.builder.build_return(Some(&body.into_int_value()));

        // return the whole thing after verification and optimization
        if function.verify(true) {
            self.fpm.run_on(&function);

            Ok(function)
        } else {
            unsafe {
                function.delete();
            }

            Err("Invalid generated function.")
        }
    }

    /// Compiles the specified `Function` in the given `Context` and using the specified `Builder`, `PassManager`, and `Module`.
    pub fn compile(
        context: &'ctx Context,
        builder: &'a Builder<'ctx>,
        pass_manager: &'a PassManager<FunctionValue<'ctx>>,
        module: &'a Module<'ctx>,
        layout: &'a mut Layout<'ctx>,
        expression: &Expr,
    ) -> Result<FunctionValue<'ctx>, &'static str> {
        let mut compiler = Compiler {
            context,
            builder,
            fpm: pass_manager,
            module,
            layout,
            expression,
            fn_value_opt: None,
            variables: HashMap::new(),
        };

        compiler.compile_fn()
    }
}

pub fn main() {
    let num_pages = 10;
    let num_rows = 3000;

    let c1 = ColumnBuilder::ColumnI32("c1", ManuallyDrop::new(vec![1i32; num_rows]));
    let c2 = ColumnBuilder::ColumnI64("c2", ManuallyDrop::new(vec![2i64; num_rows]));
    let c3 = ColumnBuilder::ColumnF64("c3", ManuallyDrop::new(vec![30.2f64; num_rows]));
    let table = Table::new("test_table", vec![Box::new(c1), Box::new(c2), Box::new(c3)]);

    let context = Context::create();
    let module = context.create_module("filter");
    let builder = context.create_builder();
    let mut layout = context.table_layout(&table);

    // Create FPM
    let fpm = PassManager::create(&module);

    // fpm.add_instruction_combining_pass();
    // fpm.add_reassociate_pass();
    // fpm.add_gvn_pass();
    // fpm.add_cfg_simplification_pass();
    // fpm.add_basic_alias_analysis_pass();
    // fpm.add_promote_memory_to_register_pass();
    // fpm.add_instruction_combining_pass();
    // fpm.add_reassociate_pass();

    fpm.initialize();

    let constant = Expr::Constant(Data::Int(50i32));
    let variable = Expr::InputReference(0);
    let constant2 = Expr::Constant(Data::Long(1i64));
    let variable2 = Expr::InputReference(1);
    let constant3 = Expr::Constant(Data::Float(5.8f64));
    let variable3 = Expr::InputReference(2);
    let lt = Expr::Binary { op: String::from("$operator$LESS_THAN"), left: Box::new(variable), right: Box::new(constant) };
    let gt = Expr::Binary { op: String::from("$operator$GREATER_THAN"), left: Box::new(variable2), right: Box::new(constant2) };
    let gt2 = Expr::Binary { op: String::from("$operator$GREATER_THAN"), left: Box::new(variable3), right: Box::new(constant3) };

    let and = Expr::Binary { op: String::from("AND"), left: Box::new(lt), right: Box::new(gt) };
    let expr = Expr::Binary { op: String::from("AND"), left: Box::new(and), right: Box::new(gt2) };

    let now = Instant::now();
    match Compiler::compile(&context, &builder, &fpm, &module, &mut layout, &expr) {
        Ok(function) => {
            println!("-> Expression compiled to IR:");
            function.print_to_stderr();

            // (function.get_name().to_str().unwrap().to_string(), true)
        }
        Err(err) => {
            panic!("!> Error compiling function: {}", err);
        }
    }

    let ee = module.create_jit_execution_engine(OptimizationLevel::None).unwrap();
    let maybe_fn = unsafe { ee.get_function::<unsafe extern "C" fn(*const c_void, usize) -> bool>("filter") };

    let elapsed = now.elapsed();
    println!("Compile elapsed: {}", elapsed.as_micros());

    let compiled_fn = match maybe_fn {
        Ok(f) => f,
        Err(err) => {
            println!("!> Error during execution: {:?}", err);
            panic!()
        }
    };

    unsafe {
        let x = table.into_ffi_args();
        for _ in 0..num_pages {
            let mut selected_rows = vec![false; num_rows];
            let now = Instant::now();
            for row in 0..num_rows - 1 {
                selected_rows[row] = compiled_fn.call(x.as_ptr() as *const c_void, row);
            }
            let elapsed = now.elapsed();
            println!("Execution elapsed: {}", elapsed.as_micros());
            println!("=> {}", compiled_fn.call(x.as_ptr() as *const c_void, 0));
            assert_eq!(compiled_fn.call(x.as_ptr() as *const c_void, 0), true);
        }

        println!("=> {}", compiled_fn.call(x.as_ptr() as *const c_void, 0));
        assert_eq!(compiled_fn.call(x.as_ptr() as *const c_void, 0), true);
        // assert_eq!(compiled_fn.call(70i32), false);
        // assert_eq!(compiled_fn.call(30i32), true);
    }

    println!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testCompiler() {
        main();
    }
}