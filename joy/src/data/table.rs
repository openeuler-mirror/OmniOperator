use core::ffi::c_void;
use inkwell::types::{BasicTypeEnum, VectorType};
use std::any::Any;
use std::fmt::Debug;

#[macro_export]
#[macro_use]
macro_rules! make_column {
    ($name: ident, $ty: ty) => {
        #[derive(Debug)]
        pub struct $name<'ctx> {
            name: &'ctx str,
            pub data: Vec<$ty>,
        }
        impl<'ctx> $name<'ctx> {
            pub fn get_data(&self) -> &Vec<$ty> {
                &self.data
            }
        }

        impl ColumnBuilder {
            pub fn $name(name: &str, data: Vec<$ty>) -> $name {
                $name { name, data }
            }
        }
    };
}

make_column!(ColumnI64, i64);
make_column!(ColumnI32, i32);
make_column!(ColumnI16, i16);
make_column!(ColumnF64, f64);
make_column!(ColumnVarchar, String);

#[derive(Debug)]
pub struct Table<'ctx> {
    name: &'ctx str,
    pub columns: Vec<Box<dyn Any>>,
}
impl<'ctx> Table<'ctx> {
    pub fn new(name: &str, columns: Vec<Box<dyn Any>>) -> Table {
        Table { name, columns }
    }
    pub fn into_ffi_args(&self) -> Vec<*const c_void> {
        let mut result = Vec::new();
        {
            //TODO: move the following to a function and maintain the lifetime
            for mut column in &self.columns {
                let mut column_ptr = None;
                if let Some(mut column_value) = column.downcast_ref::<ColumnI16>() {
                    column_ptr = Some(column_value.get_data().as_ptr() as *const _);
                } else if let Some(mut column_value) = column.downcast_ref::<ColumnI32>() {
                    column_ptr = Some(column_value.get_data().as_ptr() as *const _);
                } else if let Some(column_value) = column.downcast_ref::<ColumnI64>() {
                    column_ptr = Some(column_value.get_data().as_ptr() as *const _);
                } else if let Some(mut column_value) = column.downcast_ref::<ColumnF64>() {
                    column_ptr = Some(column_value.get_data().as_ptr() as *const _);
                }
                if column_ptr == None {
                    panic!("invalid column type");
                }
                result.push(column_ptr.unwrap());
            }
        }
        result
    }

    pub fn name(&self) -> &'ctx str {
        self.name
    }
}

pub struct ColumnBuilder {}

pub struct ColumnType<'ctx> {
    name: &'ctx str,
    VectorType: &'ctx str,
}

//stores the information about the table layout
pub struct Layout<'ctx> {
    name: &'ctx str,
    columns: Vec<BasicTypeEnum<'ctx>>,
}

impl<'ctx> Layout<'ctx> {
    pub fn new(name: &'ctx str) -> Self {
        Layout {
            name,
            columns: Vec::new(),
        }
    }

    pub fn columns(&mut self) -> &mut Vec<BasicTypeEnum<'ctx>> {
        &mut self.columns
    }
}

#[cfg(test)]
mod tests {
    use crate::data::table::{ColumnI16, ColumnI32, ColumnI64, Table};
    use crate::table::{ColumnI16, ColumnI32, ColumnI64, Table};

    #[test]
    fn test_table() {
        let c1 = ColumnI16::new("c1", vec![0i16; 3]);
        let c2 = ColumnI32::new("c1", vec![0i32; 3]);
        let c3 = ColumnI64::new("c1", vec![0i64; 3]);
        let t1 = Table::new("t1", vec![Box::new(c1), Box::new(c2)]);
        dbg!(c3);
    }
}
