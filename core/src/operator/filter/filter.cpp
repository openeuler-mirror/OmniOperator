#include "stdio.h"
#include "chrono"
#include <vector>
#include "filter.h"
#include "filter_compiler.h"
#include "../projection/projection.h"
#include "../../common/expressions.h"
#include "../../common/parser/parser.h"

using namespace std;

// int main() {
//     int count = 102400;
//     auto c1 = new int[count];
//     auto c2 = new int[count];
//     auto c3 = new int[count];
//     auto c4 = new bool[count];
//     for (int i=0; i<1023; i++) {
//         c1[i] = 12;
//         c2[i] = 2;
//         c3[i] = 6;
//     }

//     typedef std::chrono::high_resolution_clock Time;
//     typedef std::chrono::microseconds us;
//     typedef std::chrono::duration<float> fsec;

//     auto t1 = Time::now();
//     double result = 0;
//     for (int i=0; i<count; i++) {
//         c4[i] = c1[i] > 10 && c2[i] < 5 && c3[i] ==6;
//     }
//     auto t0 = Time::now();
//     fsec fs = t0 - t1;
//     us d = std::chrono::duration_cast<us>(fs);
//     printf(" filter duration time: %lu\n", d.count());
//     printf(" filter result: %d\n", c4[1]);
//     return 12345;
// }

NativeOmniFilterOperatorFactory::NativeOmniFilterOperatorFactory(std::string expression, int32_t *inputTypes, int32_t vecCount, int32_t *projectIndex, int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->vecCount = vecCount;
    this->projectIndex = projectIndex;
    this->projectVecCount = projectVecCount;

    Parser parserObject;
    std::cout << "parsing: " << expression << std::endl;
    Expr* parsedExpr = parserObject.parseRowExpression(expression);
    ComparisionExpr *c_expr =  (ComparisionExpr *) parsedExpr;
    std::cout << c_expr->columnIdx << " " << c_expr->columnData << std::endl;
    // might want to check if parsed suceed?
    //TODO: replace the placeholder context
    Compiler *compiler = new Compiler(parsedExpr, inputTypes, vecCount);
    this->filter = compiler->compile();
    delete compiler;
}

NativeOmniFilterOperatorFactory::~NativeOmniFilterOperatorFactory()
{
    delete this->filter;
}

NativeOmniOperator * NativeOmniFilterOperatorFactory::createOmniOperator()
{
    return new NativeOmniFilterOperator(this->filter, this->inputTypes, this->vecCount, this->projectIndex, this->projectVecCount);
}

int32_t NativeOmniFilterOperator::addInput(Table* data, int32_t rowCount)
{
    int32_t *selectedRows = new int32_t[rowCount];
    int32_t numSelectedRows = this->filter->filter(data, rowCount, selectedRows);
    Projection *projection = new Projection(this->inputTypes, this->vecCount, rowCount, this->projectIndex, this->projectVecCount);
    Table *projectedData = projection->project(selectedRows, numSelectedRows, data);
    this->projectedVecs = projectedData;

    delete[] selectedRows;
    delete projection;
    return numSelectedRows;
}

int32_t NativeOmniFilterOperator::getOutput(std::vector<Table*>& data)
{
    if (this->projectedVecs == nullptr) {
        return 0;
    }

    data.push_back(this->projectedVecs);
    // this->projectedVecs = nullptr;
    // TODO: cleanup memory in old tables
    return projectedVecs->getPositionCount();
}

Filter::Filter(LLVMCodeGen* codeGen, Expr* expr)
{
    this->codeGen = codeGen;
    this->expr = expr;
}

int32_t Filter::filter(Table *table, int32_t rowNumber, int32_t *selectedRows)
{
    int numSelectedRows = 0;
    ComparisionExpr *c_expr =  (ComparisionExpr *) expr;

    Column *column = table->getColumn(c_expr->columnIdx);

    for (int index = 0; index < rowNumber; index++)
    {
        switch (column->getType())
        {
            case INT32:{
                if (codeGen->execute(expr, *((int32_t*) column->getValue(index)))) {
                    selectedRows[numSelectedRows++] = index;
                }
                break;
            }
            case INT64:{
                // TODO: add Support for type
                break;
            }
            case DOUBLE:{
                // TODO: add Support for type
                break;
            }
            default:
                break;
        }
        
    }
    return numSelectedRows;
}

// long compile(Expr expression, Context context, int32_t* inputTypes, int count){
    // TODO: complete migration
    // return 0;
    // vector<int> col;
    // let mut columns: Vec<Box<dyn Any>> = Vec::new();
    // for (int i=0; i<count; i++)vec_type in input_types {
    //     auto vecType = inputTypes[i];
    //     switch(vecType) {
    //         case 1:
    //             let column = ColumnBuilder::ColumnI32("c1", ManuallyDrop::new(vec![1i32]));
    //             columns.push(column);
    //         case 2:
    //             // i64
    //             let column = ColumnBuilder::ColumnI64("c1", ManuallyDrop::new(vec![1i64]));
    //             columns.push(column);
    //         case 3:
    //             // f64
    //             let column = ColumnBuilder::ColumnF64("c3", ManuallyDrop::new(vec![1.0f64]));
    //             columns.push(column);
    //         default:
    //             panic!("Unsupported input type");
    //     }
    // }
    // let table = Table::new("test_table", columns);

    // let module = context.create_module("filter");
    // let builder = context.create_builder();
    // let mut layout = context.table_layout(&table);

    // // Create FPM
    // let fpm = PassManager::create(&module);

    // fpm.add_instruction_combining_pass();
    // fpm.add_reassociate_pass();
    // fpm.add_gvn_pass();
    // fpm.add_cfg_simplification_pass();
    // fpm.add_basic_alias_analysis_pass();
    // fpm.add_promote_memory_to_register_pass();
    // fpm.add_instruction_combining_pass();
    // fpm.add_reassociate_pass();

    // fpm.initialize();

    // let now = Instant::now();
    // match Compiler::compile(context, &builder, &fpm, &module, &mut layout, &expression) {
    //     Ok(function) => {
    //         // println!("-> Expression compiled to IR:");
    //         // function.print_to_stderr();
    //     }
    //     Err(err) => {
    //         panic!("!> Error compiling function: {}", err);
    //     }
    // }

    // let ee = module.create_jit_execution_engine(OptimizationLevel::None).unwrap();
    // let maybe_fn = unsafe { ee.get_function::<FilterFuncType>("filter") };

    // let elapsed = now.elapsed();
    // println!("Compile elapsed: {}", elapsed.as_micros());

    // let compiled_fn = match maybe_fn {
    //     Ok(f) => f,
    //     Err(err) => {
    //         println!("!> Error during execution: {:?}", err);
    //         panic!()
    //     }
    // };
    // compiled_fn
//}