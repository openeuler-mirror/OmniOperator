use crate::codegen::generator::{Generator, InSituDebug};
use crate::codegen::relational_types::RelationBuilder;
use crate::codegen::{CodeGen, InSituOp};
use crate::data::table::{Layout, Table};
use crate::operator::groupby::Aggregator;
use inkwell::types::BasicTypeEnum;
use inkwell::values::{BasicValue, IntValue};

// this groupby is usable when
// 1. cardinality is small <1000000
// 2. use array/vector to store the aggregation state
// 3. groupby columns can be translated into index in the state using simple calculation
pub struct PerfectIdentityGroupBy {
    inputs: Vec<i32>, //grouby columns
    aggregators: Vec<Box<dyn Aggregator>>,
}

impl PerfectIdentityGroupBy {
    //allow passing stats to optimize groupby
    pub fn new() -> PerfectIdentityGroupBy {
        PerfectIdentityGroupBy {
            inputs: vec![0, 1, 2],
            aggregators: Vec::new(),
        }
    }
}

impl CodeGen for PerfectIdentityGroupBy {
    fn preloop(&self, generator: &Generator, layout: &Layout) {
        //setup state
    }

    fn inloop(&self, generator: &Generator, layout: &Layout, row_index: IntValue) {
        let table_ptr = generator.get_table_param();
        println!("table_ptr: {:?}", table_ptr);
        //retrieve the value of the column to be used for groupby
        for i in &self.inputs {
            let column = generator.builder.build_get_nth_column(table_ptr, *i as u32);
            let value = generator.builder.build_get_value(column, row_index);

            generator.build_debug("debug message in loop:", value);

            //generator.build_debug(debug_func, "debug message in loop:", value);
        }

        generator.build_debug(
            "finish processing all of the columns",
            row_index.as_basic_value_enum(),
        );

        //retrieve the existing state

        //call each aggregator to process the new row

        //
    }

    fn postloop(&self, generator: &Generator, layout: &Layout) {}
}

impl InSituOp for PerfectIdentityGroupBy {
    fn add_input(&self) {
        //calls the generated function to prcoess the input
        unimplemented!()
    }

    fn get_output(&self) {
        //retrieves the output when available
        unimplemented!()
    }
}
