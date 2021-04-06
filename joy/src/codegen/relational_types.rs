use crate::data::table::{ColumnF64, ColumnI16, ColumnI32, ColumnI64, Layout, Table};
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::types::{BasicType, FloatType, IntType, StructType};
use inkwell::values::{ArrayValue, BasicValueEnum, IntValue, PointerValue, VectorValue};
use inkwell::AddressSpace;
pub trait RelationContext {
    fn table_struct_type(&self, layout: &mut Layout) -> StructType;
    fn table_layout<'ctx>(&'ctx self, table: &Table<'ctx>) -> Layout<'ctx>;
}

pub trait ColumnType {
    fn column_type(&self);
}

pub struct ColumnValue<'ctx> {
    data: VectorValue<'ctx>,
    index: u32,
}

pub trait RelationBuilder<'ctx> {
    fn build_get_nth_column(&self, table: PointerValue<'ctx>, col_idx: u32) -> VectorValue<'ctx>;
    fn build_get_value(
        &self,
        table: VectorValue<'ctx>,
        row_idx: IntValue<'ctx>,
    ) -> BasicValueEnum<'ctx>;

    fn alloca_int_column(&self, data_ty: IntType, size: u32);
    fn alloca_float_column(&self, column_ty: FloatType, size: u32);
}

impl RelationContext for Context {
    fn table_struct_type(&self, layout: &mut Layout) -> StructType {
        let table_struct = self.opaque_struct_type("table");
        //the table MUST contains pointers to arrays since this is how it's passed to accommodate different types.
        let mut result = layout.columns();
        table_struct.set_body(result.as_mut(), false);

        println!("{:?}", table_struct);
        //table_struct.set_body();
        table_struct
    }
    fn table_layout<'ctx>(&'ctx self, table: &Table<'ctx>) -> Layout<'ctx> {
        let mut layout = Layout::new(&table.name());

        for mut column in &table.columns {
            if let Some(mut column_value) = column.downcast_ref::<ColumnI16>() {
                let size = column_value.get_data().len() as u32;
                let data_type = self
                    .i16_type()
                    .vec_type(size)
                    .ptr_type(AddressSpace::Generic)
                    .as_basic_type_enum();
                println!("{:?}", data_type);
                layout.columns().push(data_type);
            } else if let Some(mut column_value) = column.downcast_ref::<ColumnI32>() {
                let size = column_value.get_data().len() as u32;
                let data_type = self
                    .i32_type()
                    .vec_type(size)
                    .ptr_type(AddressSpace::Generic)
                    .as_basic_type_enum();
                println!("{:?}", data_type);
                layout.columns().push(data_type);
            } else if let Some(column_value) = column.downcast_ref::<ColumnI64>() {
                let size = column_value.get_data().len() as u32;
                let data_type = self
                    .i64_type()
                    .vec_type(size)
                    .ptr_type(AddressSpace::Generic)
                    .as_basic_type_enum();
                println!("{:?}", data_type);
                layout.columns().push(data_type);
            } else if let Some(mut column_value) = column.downcast_ref::<ColumnF64>() {
                let size = column_value.get_data().len() as u32;
                let data_type = self
                    .f64_type()
                    .vec_type(size)
                    .ptr_type(AddressSpace::Generic)
                    .as_basic_type_enum();
                println!("{:?}", data_type);

                layout.columns().push(data_type);
            }
        }

        layout
    }
}

impl<'ctx> RelationBuilder<'ctx> for Builder<'ctx> {
    fn build_get_nth_column(&self, table: PointerValue<'ctx>, col_idx: u32) -> VectorValue<'ctx> {
        let column_ptr = self
            .build_struct_gep(table, col_idx, format!("col_{}", col_idx).as_str())
            .unwrap();
        println!("column_ptr: {:?}", column_ptr);
        let column_vec_ptr = self
            .build_load(column_ptr, format!("col_{}_vec_ptr", col_idx).as_str())
            .into_pointer_value();
        println!("column_vec_ptr: {:?}", column_vec_ptr);

        let col0_vec = self
            .build_load(column_vec_ptr, format!("col_{}_vec", col_idx).as_str())
            .into_vector_value();
        println!("column_vec: {:?}", col0_vec);
        col0_vec
    }

    fn build_get_value(
        &self,
        column: VectorValue<'ctx>,
        row_idx: IntValue<'ctx>,
    ) -> BasicValueEnum<'ctx> {
        self.build_extract_element(
            column,
            row_idx,
            format!("{}", column.get_name().to_str().expect("error")).as_str(),
        )
    }

    fn alloca_int_column(&self, data_ty: IntType, size: u32) {
        let column_ty = data_ty.array_type(size);
        let column_alloca = self.build_alloca(column_ty, "column_alloca"); //allocate column on stack
        let mut array = self
            .build_load(column_alloca, "array_load")
            .into_array_value();
    }

    fn alloca_float_column(&self, data_ty: FloatType, size: u32) {
        let column_ty = data_ty.array_type(size);
        let column_alloca = self.build_alloca(column_ty, "column_alloca"); //allocate column on stack
        let mut array = self
            .build_load(column_alloca, "array_load")
            .into_array_value();
    }
}
