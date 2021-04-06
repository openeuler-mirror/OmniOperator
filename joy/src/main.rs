use crate::codegen::generator::Generator;
use crate::codegen::{CodeGen, InSituOp};
use crate::data::table::{ColumnBuilder, ColumnF64, ColumnI16, ColumnI32, Table};
use crate::operator::filter::filter::Filter;
use crate::operator::groupby::perfect_id_groupby::PerfectIdentityGroupBy;
use inkwell::context::Context;
use inkwell::OptimizationLevel;
use std::ffi::c_void;

mod codegen;
mod data;
mod operator;

pub fn main() {
    test_gen();
    //test_char2_u8();
}

fn test_char2_u8() {
    let str = "abcd";
    let bytes = str.as_bytes();
    let mut result = String::new();
    for i in 0..bytes.len() {
        result.push(bytes[i] as char);
    }

    dbg!(str, result);
}
fn test_gen() {
    unsafe {
        let c1 = ColumnBuilder::ColumnI16("c1", vec![1i16; 1024]);
        let c2 = ColumnBuilder::ColumnI32("c1", vec![2i32; 1024]);
        let c3 = ColumnBuilder::ColumnF64("c1", vec![3.2f64; 1024]);
        let table = Table::new("test_table", vec![Box::new(c1), Box::new(c2), Box::new(c3)]);
        //let table = Table::new("test_table", vec![Box::new(c3)]);
        //let table = Table::new("test_table", vec![Box::new(c1)]);

        let groupby = PerfectIdentityGroupBy::new();

        let mut insitu_op: Box<dyn CodeGen> = Box::new(groupby);

        let func = Generator::start("test_main", &table, &mut insitu_op);
        //groupby.add_input();
    }
}
