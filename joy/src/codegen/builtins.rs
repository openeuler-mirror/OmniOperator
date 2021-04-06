use llvm_sys::support::LLVMAddSymbol;
use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
use std::os::raw::c_void;

macro_rules! make_debug_func {
    ($(($name: ident, $ty: ty)),+) => {
        $(
            #[no_mangle]
            pub extern "C" fn $name(msg: *const c_char, i: $ty) {
                unsafe {
                    let debug_msg = CStr::from_ptr(msg).to_string_lossy().into_owned();
                    println!("DEBUG: {} {}", debug_msg, i);
                }
            }
        )+
    }
}

macro_rules! register_debug_func {
    ($(($name: ident, $ty: ty)),+) => {
        #[no_mangle]
        pub unsafe extern "C" fn register_debug_func() {
            $(
                dbg!(format!("registring: {}", stringify!($name)));
                add_symbol(stringify!($name), $name as *mut c_void);
            )+
        }
    }
}

//initialize all built-in functions
pub unsafe fn init() {
    //add_symbol("dbg", i32_dbg as *mut c_void);
    dbg!("initializing symbols");
    register_debug_func();
}

unsafe fn add_symbol(symbol: &str, func_ptr: *mut c_void) {
    LLVMAddSymbol(CString::new(symbol).expect("error").as_ptr(), func_ptr);
}

make_debug_func!(
    (i16_debug, i16),
    (i32_debug, i32),
    (i64_debug, i32),
    (f64_debug, f64)
);
register_debug_func!(
    (i16_debug, i16),
    (i32_debug, i32),
    (i64_debug, i32),
    (f64_debug, f64)
);
