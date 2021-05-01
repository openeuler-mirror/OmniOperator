#include "filter_api.h"
#include "../common/expressions.h"
#include "../operator/filter/filter_runtime.h"
#include "../parser/parser.h"
#include <iostream>
#include <thread>
#include <string>

// TODO: combine the following three into one
int32_t* createIntVec(int64_t address, int32_t count)
{
    int32_t* inputVec;
    inputVec = (int32_t*) malloc(count*sizeof(int32_t));
    for (int i=0; i<count; i++) {
      *((int32_t*)(inputVec+i)) = *((int32_t*)(address+i));
    }
    return inputVec;
}

int64_t* createLongVec(int64_t address, int32_t count)
{
    int64_t* inputVec;
    inputVec = (int64_t*) malloc(count*sizeof(int64_t));
    for (int i=0; i<count; i++) {
      *((int64_t*)(inputVec+i)) = *((int64_t*)(address+i));
    }
    return inputVec;
}

// TODO: double to something like double64_t?
double* createFloatVec(int64_t address, int32_t count)
{
    double* inputVec = (double*) malloc(count*sizeof(double));
    for (int i=0; i<count; i++) {
      *((double*)(inputVec+i)) = *((double*)(address+i));
    }
    return inputVec;
}

int64_t filterCompile(
    std::string filterExpression,
    int64_t inputType,
    int32_t vecCount)
{
    // TODO: replace with pointer wrapper to avoid extra copying  
    int32_t* inputTypes = createIntVec(inputType, vecCount);
    Parser parserObject;
    Expr parsed = parserObject.parseRowExpression(filterExpression);
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Context context;
    Filter_runtime runtime;
    int64_t filter_ptr = runtime.filter_compile_runtime_with_parser(&context, parsed, inputTypes);
    free(inputTypes);
    return filter_ptr;
}

int32_t filterExecute(
    int64_t filterPtr,
    int64_t *inputData,
    int64_t inputTypes,
    int32_t vecCount,
    int64_t selectedRowsAddr,
    int32_t rowNumber
    )
{   
    // convert to vector on inputData
    int32_t *inputTypesArray = createIntVec(inputTypes, vecCount);
    // TODO: fix issue here
    int32_t *selectedRows = createIntVec(selectedRowsAddr, rowNumber);
    Table table(0, vecCount);

    for (int i; i<vecCount; i++) {
        opt::ColumnType vecType;
        switch(*(inputTypesArray+i))
        {
            case 1: vecType = INT32; break;
            case 2: vecType = INT64; break;
            case 3: vecType = DOUBLE; break;
            default:
            // TODO: raise error
                break;
        }
        int64_t address = inputData[i];
        Column column((void *)address, vecType, (size_t) rowNumber);
        table.setColumn(&column, vecType);
    }

    int index = 0;
    for (int row=0;row<rowNumber;row++) {
        // TODO: fix the issue here, transform filterPtr to filterModule
        if (filterModule->call(&table, row)) {
            selectedRows[index] = row;
            index += 1;
        }
    }


    free(inputTypesArray);
    free(selectedRows);
    return index;
}

int32_t filterExecuteV1(
    int64_t filterPtr,
    int64_t *inputData,
    int64_t inputTypes,
    int32_t vecCount,
    int32_t rowNumber,
    int64_t *projectVecAddress,
    int32_t *projectIdx,
    int32_t projectVecCount
    )
{   
    int32_t *inputTypesArray = createIntVec(inputTypes, vecCount);
    // TODO: fix issue here
    int32_t selectedRows[rowNumber];

    Table table(0, vecCount);

    for (int i; i<vecCount; i++) {
        opt::ColumnType vecType;
        switch(*(inputTypesArray+i))
        {
            case 1: vecType = INT32; break;
            case 2: vecType = INT64; break;
            case 3: vecType = DOUBLE; break;
            default:
            // TODO: raise error
                break;
        }

        int64_t address = inputData[i];
        Column column((void *)address, vecType, (size_t) rowNumber);
        table.setColumn(&column, vecType);
    }

    int index = 0;
    for (int row=0;row<rowNumber;row++) {
        // TODO: fix the issue here, transform filterPtr to filterModule
        if (filterModule->call(&table, row)) {
            selectedRows[index] = row;
            index += 1;
        }
    }


    // TODO: add projection part
    // let mut output_position = 0;
    // let mut project_vec_count = j_project_vec_count as usize;
    // let mut project_vec_addrs = vec![0i64; project_vec_count];
    // env.get_long_array_region(j_project_vec_address, 0, project_vec_addrs.as_mut());
    // let mut project_idx = vec![-1i32;project_vec_count];
    // env.get_int_array_region(j_project_idx, 0, project_idx.as_mut());
    // if index < row_number {
    //     for project_index in 0..project_idx.len() {
    //         // todo:handle different data type
    //         let mut input_vector = Vec::from_raw_parts(vec_addrs[project_idx[project_index as usize] as usize] as *mut i64, row_number, row_number);
    //         let mut copy_vector = Vec::from_raw_parts(project_vec_addrs[project_index] as *mut i64, index, index);
    //         for selected_index in 0..index {
    //                 let element = selected_rows[selected_index] as usize;
    //             copy_vector[selected_index] = input_vector[element];
    //         }
    //         mem::forget(input_vector);
    //         mem::forget(copy_vector);
    //     }
    // }

    free(inputTypesArray);
    free(selectedRows);
    return index;
}


