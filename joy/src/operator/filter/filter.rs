use crate::codegen::generator::Generator;
use crate::codegen::relational_types::RelationBuilder;
use crate::codegen::CodeGen;
use crate::data::table::{Layout, Table};
use inkwell::values::IntValue;

pub struct Filter {
    inputs: Vec<i32>,
}

impl Filter {
    pub fn new() -> Filter {
        Filter { inputs: vec![1, 2] }
    }
}
impl CodeGen for Filter {
    fn preloop(&self, generator: &Generator, layout: &Layout) {}

    fn inloop(&self, generator: &Generator, layout: &Layout, row_index: IntValue) {
        let table_ptr = generator.get_table_param();
        for i in &self.inputs {
            //retrieve the value of the column to be used for groupby
            let column = generator.builder.build_get_nth_column(table_ptr, *i as u32);
            generator.builder.build_get_value(column, row_index);
        }
    }

    fn postloop(&self, generator: &Generator, layout: &Layout) {}
}
