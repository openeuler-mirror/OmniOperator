# joy

Joy is project hoping to make it easier to create high performance data processing logic.

* Joy leverage LLVM to create the code dynamically.
* Joy doesn't define any intermediate IR, since 
1. SQL can be the IR
2. the number of operations in SQL is not that many, we can create optimised operators for all of them
* Joy provides a API, hopefully simplified, , without requiring LLVM knowledge, to create and optimise the needed operators

#### Introduction
Bring happiness to data processing. :)

API overview:

table api
groupby api

code gen api

#### Architecture



#### Usage

###### requirements:
1. provide high performance `atom operators`, which can be combined into `task`
2. SqlJit compiler `fusion` capability to performan `task` level optimization such as Weld, 
    optimizations:
    * dynamic vector size: the compiler should take into account the CPU capabilities and decide on for example `vector size`, to leverage SIMD and at the same reduce CPU cache miss
    * type compaction, requires statistics, for example long -> int -> short, phone -> long
3. ensure cacheline alignment

Use TPC-H Q1 as an example to create the group by aggregator functionality


the purpose of this project is to create a sql processing engine using miri

https://github.com/rust-lang/miri
Rust miri provides a mid-level IR which can be used to interpret and run rust code

The idea is to compile sql into rust closure and run using miri

Weld: maintains it's own language
Joy: use closure instead

1. crate a new SQL JIT compiler leveraging llvm-sys
2. take over optimizer from Weld?

Weld:

1. use closure syntax with its own parser -- lots of code to maintain
2. require more time to parse and visit the AST, which could impact the JIT performance
3. We can potentially borrow the optimizer passes and implement more


##### Data Types

The type system should be as transparent as possible, ideally we should be able to 
use the native data types such  as `i32`, `i64`, `f32`, `u8` directly.

Column::create()  

##### Code Gen
1. Direct LLVM code gen without parser to reduce code gen latency
2. direct optimization of the code to reduce the time needed for optimization pass
3. expose high level data processing codegen API for the community to create optimized data processing logic (vs Weld expose IR)

MCJit?? --> ORCJit (On request compiler)

##### Code Gen Simplified

The Joy project target to

1. provide a codegen framework requires NO knowledge of LLVM
2. zero overhead codegen: framework should not bring any overhead to the generated code. 



1. A framework which provides a `uniop` (single input) and a `binop` (2 input)
can we provide a trait for each of the operator type?
how is the trait plug into the codegen?
what's the benefit of using codegen for join?
2. built-in Vectorize input support
3. pluggable logic such as groupby, join

###### Debug using Visual Studio Code
1. Install the RUST and LLDB plugin for the vscode
2. Config the debug launch.json and input attach under `configurations` in launch.json. The `LLDB attach` is automatically displayed.
3. In the debug panel, click `Launch` in debug window and select the process to debug.
4. You can add breakpoints in the vscode and debug the code.


##### gen()
* The gen function provides boilerplate code which loops over each row.
* allows generate code while looping over each row
* allows composition of generated code processing each row

the context of the generated code:
    1. has access to all of the columns
    2. knows what columns needed is needed
    3. all columns are access via column index
    3. which column to store the output

##### C++ Building
* Build with llvm options(-S -O3 -emit-llvm -fno-discard-value-names)
> ./build.sh release
* Build without llvm option
> ./build.sh debug
