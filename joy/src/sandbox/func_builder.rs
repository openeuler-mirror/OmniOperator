use crate::into_cstr;
use llvm_sys::core::{
    LLVMAddFunction, LLVMBuildGEP, LLVMBuildLoad, LLVMBuildStore, LLVMContextCreate,
    LLVMCreateBuilderInContext, LLVMFunctionType, LLVMInt32Type, LLVMModuleCreateWithName,
    LLVMPointerType, LLVMVoidType,
};
use llvm_sys::prelude::LLVMValueRef;
use llvm_sys::{LLVMBuilder, LLVMContext, LLVMModule, LLVMValue};
use std::ffi::CString;

pub struct FunctionBuilder {
    pub context: *mut LLVMContext,
    pub builder: *mut LLVMBuilder,
    pub module: *mut LLVMModule,
    pub dbg_func: Option<LLVMValueRef>,
    pub vec_func: Option<LLVMValueRef>,
}

impl FunctionBuilder {
    pub unsafe fn get() -> FunctionBuilder {
        let context = LLVMContextCreate();
        let module = LLVMModuleCreateWithName(into_cstr!("uuid"));
        let builder = LLVMCreateBuilderInContext(context);

        FunctionBuilder {
            context,
            module,
            builder,
            dbg_func: None,
            vec_func: None,
        }
    }

    // build get_value instructions to retrieve value from columns[column_index][row_index]
    pub unsafe fn build_get_value(
        &self,
        columns: &Vec<*mut LLVMValue>,
        column_index: usize,
        row_index: *mut LLVMValueRef,
    ) -> *mut LLVMValue {
        let name_addr = format!("col{}_addr", column_index);
        let name = format!("col{}[index]", column_index);
        let value_addr = LLVMBuildGEP(
            self.builder,
            columns[column_index], //1st column
            row_index,
            1,
            into_cstr!(name_addr.as_str()),
        );

        let result = LLVMBuildLoad(self.builder, value_addr, into_cstr!(name.as_str()));
        result
    }

    //build set_value instructions to set the value to the columns[column_index][row_index]
    pub unsafe fn build_set_value(
        &self,
        columns: &Vec<*mut LLVMValue>,
        column_index: usize,
        row_index: *mut LLVMValueRef,
        value: *mut LLVMValue,
    ) {
        let result_addr = LLVMBuildGEP(
            self.builder,
            columns[column_index], //3rd column
            row_index,
            1,
            into_cstr!("result_addr"),
        );
        LLVMBuildStore(self.builder, value, result_addr);
    }

    pub unsafe fn DBG(&mut self, args: *mut LLVMValueRef) {
        if self.dbg_func == None {
            let mut param_ty = [LLVMInt32Type()];
            let func_ty = LLVMFunctionType(LLVMVoidType(), param_ty.as_mut_ptr(), 1, 0);
            let d_func = LLVMAddFunction(self.module, b"dbg\0".as_ptr() as *const _, func_ty);
            self.dbg_func = Some(d_func);
        }
        llvm_sys::core::LLVMBuildCall(
            self.builder,
            self.dbg_func.expect("error"),
            args,
            1,
            b"\0".as_ptr() as *const _,
        );
    }

    pub unsafe fn VEC(&mut self, args: *mut LLVMValueRef) -> *mut LLVMValue {
        if self.vec_func == None {
            let mut param_ty = [LLVMInt32Type()];
            let func_ty = LLVMFunctionType(
                LLVMPointerType(LLVMInt32Type(), 0),
                param_ty.as_mut_ptr(),
                1,
                0,
            );
            let v_func =
                LLVMAddFunction(self.module, b"create_vec\0".as_ptr() as *const _, func_ty);
            self.vec_func = Some(v_func);
        }
        llvm_sys::core::LLVMBuildCall(
            self.builder,
            self.vec_func.expect("error"),
            args,
            1,
            b"vec_state\0".as_ptr() as *const _,
        )
    }

    pub unsafe fn INIT_VEC(&mut self, array: LLVMValueRef, size: LLVMValueRef) -> *mut LLVMValue {
        if self.vec_func == None {
            let mut param_ty = [LLVMInt32Type()];
            let func_ty = LLVMFunctionType(
                LLVMPointerType(LLVMInt32Type(), 0),
                param_ty.as_mut_ptr(),
                1,
                0,
            );
            let v_func =
                LLVMAddFunction(self.module, b"create_vec\0".as_ptr() as *const _, func_ty);
            self.vec_func = Some(v_func);
        }

        let mut args = [array, size, size];
        llvm_sys::core::LLVMBuildCall(
            self.builder,
            self.vec_func.expect("error"),
            args.as_mut_ptr(),
            1,
            b"vec_state\0".as_ptr() as *const _,
        )
    }
}
