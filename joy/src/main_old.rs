#![feature(test)]
#![feature(vec_into_raw_parts)]

extern crate test;
use crate::codegen::compiler::gen;
use crate::codegen::compiler::{exec, CodeGen};
use crate::codegen::groupby_compiler::{GroupBy, SumAggregator};

use crate::data::table::{ColumnF64, ColumnI16, ColumnI32, Table};
use std::mem;
use test::Bencher;

pub fn main() {
    codegen::init();
    test_jit_groupby();
}

fn test_jit_groupby() {
    let c1 = ColumnI32::new("c1", vec![1, 1, 1, 1, 2, 2, 2, 2, 2, 1]);
    let c2 = ColumnI16::new("c2", vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
    let c3 = ColumnI32::new("c3", vec![1i32; 10]); //used to store result

    let mut t1 = Table::new("t1", vec![Box::new(c1), Box::new(c2), Box::new(c3)]);

    unsafe {
        //vec1 * vec2;
        let func_name = "main";
        let mut codegens = Vec::new();
        let mut groupby = GroupBy::new(vec![1, 2], 3, Vec::new());
        groupby.add_aggregator(Box::new(SumAggregator::new(1, None)));
        codegens.push(Box::new(groupby) as Box<dyn CodeGen>);
        let (module, function) = gen(func_name, &mut t1, &mut codegens);

        exec(module, function, func_name, &mut t1);

        mem::forget(&t1);

        println!(
            "result: {:#?}",
            t1.columns.get(2).expect("err").downcast_ref::<ColumnI16>()
        );
    }
}
