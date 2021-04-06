use joy::codegen::function_registry;

pub mod compiler;
pub mod func_builder;
mod groupby;
mod groupby_col;
pub mod groupby_compiler;
mod groupby_research;
mod groupyby_macro;
mod loopfusion;
pub mod parameter;
mod simd_groupby;
mod simd_test;
mod type_compaction;

pub fn init() {
    unsafe {
        llvm_init();
        function_registry::init();
    }
}

//initialize the llvm
unsafe fn llvm_init() {
    LLVM_InitializeNativeTarget();
    LLVM_InitializeNativeAsmParser();
    LLVM_InitializeAllAsmPrinters();
    LLVMLinkInMCJIT();
    LLVMEnablePrettyStackTrace();
}
