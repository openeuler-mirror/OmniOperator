extern crate llvm_sys as llvm;

use std::ffi::CStr;
use std::ffi::{c_void, CString};
use std::mem;
use std::ops::Deref;

use llvm_sys::core::{LLVMContextCreate, LLVMCreateBuilderInContext, LLVMModuleCreateWithName};

use joy::data::table::{ColumnF64, ColumnI16, ColumnI32, ColumnI64, Table};

use self::llvm::analysis::LLVMVerifierFailureAction::LLVMAbortProcessAction;
use self::llvm::analysis::LLVMVerifyModule;
use self::llvm::core::{
    LLVMAddFunction, LLVMAppendBasicBlock, LLVMAppendBasicBlockInContext, LLVMBuildAdd,
    LLVMBuildAlloca, LLVMBuildBr, LLVMBuildCondBr, LLVMBuildGEP, LLVMBuildGEP2, LLVMBuildICmp,
    LLVMBuildIntCast, LLVMBuildLoad, LLVMBuildMul, LLVMBuildRet, LLVMBuildStore, LLVMConstFPCast,
    LLVMConstInt, LLVMConstString, LLVMDoubleType, LLVMDumpModule, LLVMFloatType, LLVMFunctionType,
    LLVMGetGlobalContext, LLVMGetNamedFunction, LLVMGetNamedGlobalIFunc, LLVMGetParam,
    LLVMInt16Type, LLVMInt32Type, LLVMPointerType, LLVMPositionBuilderAtEnd,
    LLVMSetFunctionCallConv, LLVMSetLinkage, LLVMVoidType,
};
use self::llvm::execution_engine::{
    LLVMAddGlobalMapping, LLVMCreateGenericValueOfFloat, LLVMCreateGenericValueOfPointer,
    LLVMCreateJITCompilerForModule, LLVMCreateMCJITCompilerForModule, LLVMFindFunction,
    LLVMGenericValueToFloat, LLVMGetFunctionAddress, LLVMInitializeMCJITCompilerOptions,
    LLVMLinkInMCJIT, LLVMMCJITCompilerOptions, LLVMOpaqueGenericValue,
    LLVMRecompileAndRelinkFunction,
};
use self::llvm::prelude::LLVMValueRef;
use self::llvm::support::{LLVMAddSymbol, LLVMLoadLibraryPermanently};
use self::llvm::target::{
    LLVM_InitializeAllAsmPrinters, LLVM_InitializeNativeAsmParser, LLVM_InitializeNativeTarget,
};
use self::llvm::target_machine::LLVMCodeModel;
use self::llvm::LLVMCallConv::LLVMCCallConv;
use self::llvm::LLVMIntPredicate::LLVMIntSLT;
use self::llvm::{
    LLVMBasicBlock, LLVMBuilder, LLVMCallConv, LLVMContext, LLVMLinkage, LLVMModule, LLVMType,
    LLVMValue,
};
use joy::codegen::func_builder::FunctionBuilder;
use std::borrow::Borrow;

#[macro_export]
macro_rules! into_cstr {
    ($input:expr) => {{
        CString::new($input).expect("error").as_ptr()
    }};
}

fn to_cstr(str: &str) -> CString {
    CString::new(str).expect("error")
}

macro_rules! column_ffi_ptr {
    ($column: ident) => {{
        $column.get_data().as_ptr() as *mut c_void
    }};
}

pub trait CodeGen {
    unsafe fn pre_loop_gen(&mut self, builder: &mut FunctionBuilder, table: &Vec<*mut LLVMValue>) {}
    //generate the code to process the row
    unsafe fn in_loop_gen(
        &mut self,
        builder: &mut FunctionBuilder,
        table: &Vec<*mut LLVMValue>,
        index: *mut LLVMValue,
    );
    unsafe fn post_loop_gen(
        &self,
        builder: &FunctionBuilder,
        table: &Vec<*mut LLVMValue>,
        index: *mut LLVMValue,
    ) {
    }
}

pub unsafe fn exec(
    module: *mut LLVMModule,
    llvm_function: LLVMValueRef,
    function: &str,
    table: &mut Table,
) {
    dbg!("calling exec");
    let mut engine = mem::MaybeUninit::uninit();
    let mut error = mem::MaybeUninit::uninit();
    let mut compile_options = get_compile_options();
    let options_size = mem::size_of::<LLVMMCJITCompilerOptions>();
    let compile_options_size = 0;

    let ret_code =
        LLVMCreateJITCompilerForModule(engine.as_mut_ptr(), module, 2, error.as_mut_ptr());

    if ret_code != 0 {
        let c_str: &CStr = CStr::from_ptr(error.assume_init());
        let str_slice = c_str.to_str().unwrap();
        let str = str_slice.to_owned();
        panic!("error creating execution engine {:#?}", str);
    }

    let func_addr = LLVMGetFunctionAddress(engine.assume_init(), function.as_ptr() as *const _);
    let f: extern "C" fn(*const c_void) -> *mut i32 = mem::transmute(func_addr);

    let mut args = table.to_ffi_ptr();
    let d = f(args.as_ptr() as *mut _);
    let mut result = Vec::from_raw_parts(d as *mut i32, 10, 10);
    dbg!(&result);
}

// the signature of the generated function is:
// generated_func(*columns, columns_size) -> *results
pub unsafe fn gen(
    name: &str,
    table: &mut Table,
    codegens: &mut Vec<Box<CodeGen>>,
) -> (*mut LLVMModule, *mut LLVMValue) {
    let mut builder = FunctionBuilder::get();

    let arg_type = LLVMPointerType(LLVMPointerType(LLVMInt32Type(), 0), 0);
    let mut func_args = [arg_type];
    let return_type = LLVMPointerType(LLVMInt32Type(), 0);
    let func_type = LLVMFunctionType(return_type, func_args.as_mut_ptr(), 1, 0);
    let func = LLVMAddFunction(builder.module, name.as_ptr() as *const _, func_type);

    let func_basic_blk =
        LLVMAppendBasicBlockInContext(builder.context, func, into_cstr!("func_entry"));

    let (condition, body, increment, end) = init_loop_blocks(func);
    LLVMPositionBuilderAtEnd(builder.builder, func_basic_blk);
    {
        let columns = into_columns_ptr(func, builder.builder, &gen_input_types(table));

        // invoking the code generator to insert the right code
        for i in 0..codegens.len() {
            codegens[i].pre_loop_gen(&mut builder, &columns);
        }

        let index_var = LLVMBuildAlloca(builder.builder, LLVMInt32Type(), into_cstr!("index_var"));

        LLVMBuildStore(
            builder.builder,
            LLVMConstInt(LLVMInt32Type(), 0, 1),
            index_var,
        );
        LLVMBuildBr(builder.builder, condition);

        LLVMPositionBuilderAtEnd(builder.builder, condition);
        {
            let mut index = LLVMBuildLoad(
                builder.builder,
                index_var,
                b"[index_1]\0".as_ptr() as *const _,
            );
            let cond = LLVMBuildICmp(
                builder.builder,
                LLVMIntSLT,
                index,
                LLVMConstInt(LLVMInt32Type(), 10, 1),
                into_cstr!("index<len"),
            );
            LLVMBuildCondBr(builder.builder, cond, body, end);

            LLVMPositionBuilderAtEnd(builder.builder, body);
            {
                //invoking the code generator to insert the right code
                for i in 0..codegens.len() {
                    codegens[i].in_loop_gen(&mut builder, &columns, index);
                }
                LLVMBuildBr(builder.builder, increment);
            }

            LLVMPositionBuilderAtEnd(builder.builder, increment);
            {
                let index_inc = LLVMBuildAdd(
                    builder.builder,
                    index,
                    LLVMConstInt(LLVMInt32Type(), 1, 1),
                    into_cstr!("index++"),
                );
                LLVMBuildStore(builder.builder, index_inc, index_var);
                LLVMBuildBr(builder.builder, condition);
            }

            LLVMPositionBuilderAtEnd(builder.builder, end);
            LLVMBuildRet(builder.builder, *columns.get(2).expect("error").deref());
        }
    }

    {
        dbg!("verifying module");
        LLVMDumpModule(builder.module);
        let mut msg: *mut std::os::raw::c_char = std::mem::zeroed();
        LLVMVerifyModule(builder.module, LLVMAbortProcessAction, &mut msg);
        dbg!("module verified");
    }

    (builder.module, func)
}

unsafe fn to_func_args(input_types: &Vec<*mut LLVMType>) -> *mut LLVMType {
    LLVMPointerType(LLVMInt32Type(), 0)
    // let mut args = Vec::new();
    // for input_type in input_types {
    //     args.push(LLVMPointerType(*input_type, 0));
    // }
    // args
}

unsafe fn get_compile_options() -> LLVMMCJITCompilerOptions {
    dbg!("creating compile options");

    let mut compile_options = mem::MaybeUninit::<LLVMMCJITCompilerOptions>::uninit();

    let options_size = mem::size_of::<LLVMMCJITCompilerOptions>();
    LLVMInitializeMCJITCompilerOptions(compile_options.as_mut_ptr(), options_size);

    let mut compile_options: LLVMMCJITCompilerOptions = compile_options.assume_init();
    compile_options.OptLevel = 2; //default to optimization level 2
    compile_options.CodeModel = LLVMCodeModel::LLVMCodeModelDefault;
    compile_options
}

// returns a pointer to the beginning of each column
// which can be used to access the column content
unsafe fn into_columns_ptr(
    func: *mut LLVMValue,
    builder: *mut LLVMBuilder,
    input_types: &Vec<*mut LLVMType>,
) -> Vec<*mut LLVMValue> {
    let mut columns = Vec::new();
    let mut column_index = 0u64; //column index is 0 based
    let columns_arg = LLVMGetParam(func, 0); //argument is 0 based

    for input in input_types {
        let column_name = format!("_col_{}_param\0", column_index);
        let column_addr = LLVMBuildGEP2(
            builder,
            *input,      //column type, pointer to i32, i64, etc.
            columns_arg, //pointer to the array of columns
            &mut LLVMConstInt(LLVMInt32Type(), column_index, 0),
            1,
            column_name.as_ptr() as *const _,
        );

        let column_ptr_name = format!("_col_{}_param_ptr\0", column_index);
        let col_ptr = LLVMBuildLoad(builder, column_addr, column_ptr_name.as_ptr() as *const _);
        columns.push(col_ptr);
        column_index += 1;
    }
    columns
}

unsafe fn gen_input_types(table: &Table) -> Vec<*mut LLVMType> {
    let mut input_types = vec![];
    for column in &table.columns {
        let mut dtype = None; //default type
        match column.downcast_ref::<ColumnI32>() {
            Some(column_value) => {
                dtype = Option::Some(LLVMInt32Type());
            }
            None => { /*ignore*/ }
        }

        match column.downcast_ref::<ColumnI16>() {
            Some(column_value) => {
                dtype = Option::Some(LLVMInt16Type());
            }
            None => { /*ignore*/ }
        }
        match column.downcast_ref::<ColumnF64>() {
            Some(column_value) => {
                dtype = Option::Some(LLVMFloatType());
            }
            None => { /*ignore*/ }
        }

        if dtype == None {
            panic!("invalid column type");
        }
        let col_type = LLVMPointerType(dtype.expect("error"), 0);
        input_types.push(col_type);
    }
    input_types
}

unsafe fn into_input_arg(table: &Table) -> *mut *mut LLVMOpaqueGenericValue {
    let mut input_args = vec![];
    for column in &table.columns {
        let mut arg_value = None; //default type
        match column.downcast_ref::<ColumnI32>() {
            Some(column_value) => {
                arg_value = Option::Some(LLVMCreateGenericValueOfPointer(column_ffi_ptr!(
                    column_value
                )));
            }
            None => { /* ignore, will be processed later */ }
        }
        match column.downcast_ref::<ColumnI16>() {
            Some(column_value) => {
                arg_value = Option::Some(LLVMCreateGenericValueOfPointer(column_ffi_ptr!(
                    column_value
                )));
            }
            None => { /* ignore, will be processed later */ }
        }
        match column.downcast_ref::<ColumnI64>() {
            Some(column_value) => {
                arg_value = Option::Some(LLVMCreateGenericValueOfPointer(column_ffi_ptr!(
                    column_value
                )));
            }
            None => { /* ignore, will be processed later */ }
        }
        if arg_value == None {
            panic!("invalid column type");
        }
        input_args.push(arg_value.expect("error"));
    }
    input_args.as_mut_ptr()
}

unsafe fn init_loop_blocks(
    function: *mut LLVMValue,
) -> (
    *mut LLVMBasicBlock, /* loop condition */
    *mut LLVMBasicBlock, /* loop body */
    *mut LLVMBasicBlock, /* loop increment */
    *mut LLVMBasicBlock, /* loop end */
) {
    let condition = LLVMAppendBasicBlock(function, b"loop_condition\0".as_ptr() as *const _);
    let body = LLVMAppendBasicBlock(function, b"loop_body\0".as_ptr() as *const _);
    let increment = LLVMAppendBasicBlock(function, b"loop_increment".as_ptr() as *const _);
    let end = LLVMAppendBasicBlock(function, b"loop end".as_ptr() as *const _);

    (condition, body, increment, end)
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;
    use std::ops::Deref;

    use crate::codegen::compiler::{exec, gen};
    use joy::data::table::{ColumnI16, Table};

    #[test]
    fn test_gen_array_param() {
        let c1 = ColumnI16::new("c1".to_string(), vec![2i16; 10]);
        let c2 = ColumnI16::new("c2".to_string(), vec![2i16; 10]);
        let c3 = ColumnI16::new("c3".to_string(), vec![1i16; 10]); //used to store result

        let mut t1 = Table::new(
            "t1".to_string(),
            vec![Box::new(c1), Box::new(c2), Box::new(c3)],
        );

        unsafe {
            //vec1 * vec2;
            let func_name = "test_gen_array_param";
            let (module, function) = gen(func_name, &t1);
            exec(module, function, func_name, &mut t1);

            let result = t1
                .columns
                .get(2)
                .expect("err")
                .downcast_ref::<ColumnI16>()
                .expect("error");
        }
    }
}
