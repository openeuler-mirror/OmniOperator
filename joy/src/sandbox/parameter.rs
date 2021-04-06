use std::ffi::c_void;

pub trait Parameter {
    ///convert the input vector into ffi compatible pointer
    fn as_ffi_ptr(&mut self) -> *mut c_void;
}
