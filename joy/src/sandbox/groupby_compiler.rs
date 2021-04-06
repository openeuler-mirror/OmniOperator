use llvm_sys::error_handling::LLVMEnablePrettyStackTrace;
use std::any::Any;
use std::ffi::CString;

use super::compiler::CodeGen;
use crate::codegen::func_builder::FunctionBuilder;
use crate::into_cstr;
use joy::data::table::Table;
use llvm_sys::core::{
    LLVMAddFunction, LLVMArrayType, LLVMBuildAdd, LLVMBuildAlloca, LLVMBuildArrayAlloca,
    LLVMBuildArrayMalloc, LLVMBuildBitCast, LLVMBuildCall, LLVMBuildCall2, LLVMBuildCast,
    LLVMBuildGEP, LLVMBuildGEP2, LLVMBuildIntCast, LLVMBuildLandingPad, LLVMBuildLoad,
    LLVMBuildLoad2, LLVMBuildMemSet, LLVMBuildMul, LLVMBuildStore, LLVMConstArray, LLVMConstInt,
    LLVMConstNull, LLVMDoubleType, LLVMFloatType, LLVMFunctionType, LLVMGetElementAsConstant,
    LLVMGetFunctionCallConv, LLVMInt32Type, LLVMInt64Type, LLVMInt8Type, LLVMPointerType,
    LLVMVectorType, LLVMVoidType,
};
use llvm_sys::prelude::LLVMValueRef;
use llvm_sys::LLVMOpcode::LLVMAlloca;
use llvm_sys::LLVMValue;
use std::borrow::Borrow;
use std::ops::Deref;

pub struct GroupBy {
    groupby_column_idx: Vec<usize>,
    output_column_idx: i32,
    aggregators: Vec<Box<dyn AggGenerator>>,
    //mut outputs: Vec<>
}

impl GroupBy {
    pub fn new(
        groupby_column_idx: Vec<usize>,
        output_column_idx: i32,
        aggregators: Vec<Box<dyn AggGenerator>>,
    ) -> GroupBy {
        GroupBy {
            groupby_column_idx,
            output_column_idx,
            aggregators,
        }
    }

    //when adding aggregator, we will check if there is any optimization can be done
    //e.g. if sum and average is applied to the same column, the average can be calculated later on
    //
    pub fn add_aggregator(&mut self, aggreator: Box<AggGenerator>) {
        dbg!("adding aggregator");
        self.aggregators.push(aggreator);
        dbg!(self.aggregators.len());
    }
}

impl CodeGen for GroupBy {
    unsafe fn pre_loop_gen(
        &mut self,
        builder: &mut FunctionBuilder,
        columns: &Vec<*mut LLVMValue>,
    ) {
        dbg!("initializing aggregator state");
        // FIXME: assume cardinality <100000
        // FIXME: assume perfect identity hash, e.g. the value is used as the index of the array
        // FIXME: will need statistic to ensure correctness,
        // FIXME: will also need a `dictionary` type to convert non-integer and multiple column scenario
        // FIXME: worst case is to fall back to hash function using HashMap
        for aggregate in &mut self.aggregators {
            dbg!("setting state");
            // Declare variables that is needed to maintain aggregation state, the array length is > group count

            //FIXME: need to initialize the array properly
            let mut state_alloca = LLVMBuildArrayAlloca(
                builder.builder,
                LLVMArrayType(LLVMInt32Type(), 100),
                LLVMConstNull(LLVMInt32Type()),
                into_cstr!(""),
            );

            let state = LLVMBuildLoad(
                builder.builder,
                state_alloca.as_mut().unwrap(),
                into_cstr!("state"),
            );

            llvm_sys::core::LLVMBuildInsertValue(
                builder.builder,
                state,
                LLVMConstInt(LLVMInt32Type(), 0, 0),
                0,
                into_cstr!(""),
            );
            llvm_sys::core::LLVMBuildInsertValue(
                builder.builder,
                state,
                LLVMConstInt(LLVMInt32Type(), 0, 0),
                1,
                into_cstr!(""),
            );
            llvm_sys::core::LLVMBuildInsertValue(
                builder.builder,
                state,
                LLVMConstInt(LLVMInt32Type(), 0, 0),
                2,
                into_cstr!(""),
            );

            //LLVMBuildStore(builder.builder, zero, state);

            aggregate.set_state(state);
            dbg!("state set");
        }
    }
    //for groupby, the logic is to update the corresponding aggregator with the properly value
    //1. find the right group
    //2. update the corresponding aggregator
    //3. codegen should act as a glue layer and leave most of the implementation in a function to be invoked
    unsafe fn in_loop_gen(
        &mut self,
        builder: &mut FunctionBuilder,
        columns: &Vec<*mut LLVMValue>,
        mut index: LLVMValueRef,
    ) {
        //1. get all the values needed for groupby based on groupby_column_idx
        //2. get the other values required for AGG
        //3. invoke the AGG
        //4. store the value

        //BENEFIT:
        // 1. No `for loop` over the columns since we know the # of columns
        // 2. No `for loop` over the AGG since we know the # of AGG

        let mut row = Vec::new();
        for i in 0..columns.len() {
            let column = builder.build_get_value(&columns, i, &mut index);
            row.push(column);
        }

        for i in 0..self.aggregators.len() {
            dbg!("calling agg gen()");
            //FIXME: use x, the first column's value, as the group_index, perfect identity hash assumed
            let mut group_index = row[0];
            self.aggregators[i].gen(builder, &row, &mut group_index);
        }
        builder.build_set_value(&columns, 2, &mut index, row[0]);
    }
}

pub trait AggGenerator {
    fn set_state(&mut self, state: LLVMValueRef);
    fn get_state(&self) -> Option<LLVMValueRef>;
    fn gen(
        &mut self,
        builder: &mut FunctionBuilder,
        row: &Vec<LLVMValueRef>,
        group_index: *mut LLVMValueRef,
    );
}

pub struct SumAggregator {
    input: usize, //only allow one
    agg_state: Option<LLVMValueRef>,
}

impl SumAggregator {
    pub fn new(input: usize, agg_state: Option<LLVMValueRef>) -> SumAggregator {
        SumAggregator { input, agg_state }
    }
}
//Marker Aggregator
impl AggGenerator for SumAggregator {
    fn set_state(&mut self, state: LLVMValueRef) {
        self.agg_state = Some(state);
    }

    fn get_state(&self) -> Option<*mut LLVMValue> {
        self.agg_state
    }

    fn gen(
        &mut self,
        builder: &mut FunctionBuilder,
        row: &Vec<LLVMValueRef>,
        group_index: *mut LLVMValueRef, //FIXME: the index of the group is calculated based on the value of the group column,e.g. perfect identity hash is assumed
    ) {
        let existing_state = format!("sum_agg_existing{}", self.input);
        let updated_state = format!("sum_agg_updated{}", self.input);
        unsafe {
            builder.DBG(&mut LLVMConstInt(LLVMInt32Type(), 11111111, 0));
            builder.DBG(group_index);

            // FIXME: BE CAREFUL WITH THE ARRAY INDEX AND GROUP_INDEX
            //let mut group_index = LLVMConstInt(LLVMInt32Type(), 2, 0);

            let mut group_agg_state_addr = LLVMBuildGEP2(
                builder.builder,
                LLVMInt32Type(),
                self.get_state().unwrap(),
                group_index,
                1,
                into_cstr!(existing_state.as_str()),
            );

            let mut group_agg_state = LLVMBuildLoad(
                builder.builder,
                group_agg_state_addr,
                existing_state.as_ptr() as *const _,
            );

            builder.DBG(&mut group_agg_state);

            //FIXME: check type mismatch and add cast if necessary
            let mut casted_rhs = LLVMBuildIntCast(
                builder.builder,
                row[self.input],
                LLVMInt32Type(),
                into_cstr!("casted_rhs"),
            );
            builder.DBG(&mut casted_rhs);
            let mut updated_group_agg_state = LLVMBuildAdd(
                builder.builder,
                group_agg_state,
                casted_rhs,
                into_cstr!(updated_state.as_str()),
            );

            LLVMBuildStore(
                builder.builder,
                updated_group_agg_state,
                group_agg_state_addr,
            );

            builder.DBG(&mut updated_group_agg_state);
        }
    }
}

//single in single out
//multiple in single out
//single in multiple out
//multiple in multiple out
pub fn gen(table: &Table, groupby: Vec<i32>, out: i32) {

    //for row in rows {
    //
    //
    //
    //}
}

//generates a single row in, single value out operator
pub fn siso_gen() {
    //let result = LLVMBuildAdd(builder);
    // a = op(b, c, d, ..)
}
