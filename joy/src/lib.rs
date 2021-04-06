pub mod codegen;
pub mod data;
pub mod operator;
mod sandbox;

#[no_mangle]
pub extern "C" fn test_external_func() -> i32 {
    100
}
