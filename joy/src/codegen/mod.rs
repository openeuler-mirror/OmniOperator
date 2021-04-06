use inkwell::values::{ArrayValue, IntValue};

use crate::codegen::generator::Generator;
use crate::codegen::relational_types::RelationBuilder;
use crate::data::table::{Layout, Table};

pub mod builtins;
pub mod generator;
pub mod loop_block;
pub mod relational_types;

//all startup initializations are done here
//do not expose unsafe to upper level code

pub trait CodeGen {
    fn preloop(&self, generator: &Generator, table: &Layout);
    //FIXME: add mechanism to share the context so that columns/rows  doesn't need to be retrieved again and again
    fn inloop(&self, generator: &Generator, table: &Layout, row_index: IntValue);
    fn postloop(&self, generator: &Generator, table: &Layout);
}

pub trait InSituOp {
    fn add_input(&self);
    fn get_output(&self);
}
