#include "filter_runtime.h"
#include <regex>


int64_t Filter_runtime::filter_compile_runtime_with_parser(Context* context, Expr expression, int32_t* input_type) {
    auto filter_module = Filter::compile(&expr, &context, &input_type);
        
    return (int64_t) &filter_module;
}

// todo, add table 
// void filter_runtime::filter_execute(JitFunction filterModule, Table inputData, int rowNumber, bool* selectedRows) {
//     for (int rowIndex=0; rowIndex < rowNumber; rowIndex ++) {
//         selected_rows[row_index] = filterModule.call(inputData.into_ffi_args().as_ptr(), rowIndex);
//     }
// }

