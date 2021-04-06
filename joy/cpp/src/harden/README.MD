##### Hardening/Solidify Overview - incorporating data statistics into the codegen process

the process of hardening is to translate data/statistics information to remove any runtime branching.

##### hardening targets:

#### harden a parameter

#### harden expression

#### harden if

#### harden loop

1. Produces same results when a parameter is given
2. Invoked repetitively at runtime, repetitiveness should be large enough to overcome the hardening overhead

potential hardening operation:

1. remove parameter and make it a local const variable to trigger optimization
2. unnecessariy branch,
   - e.g. `if a > 5`, once a is known to be 4, `if 4 > 5` becomes unreachable block which can be removed
   - switch
3. unnecessary check
   a[i] will result in boundary check, once `i` is known, we can remove the check
4. more efficient operation
   - a \* b, once we know b is power of 2, we can switch over to shift

##### How?

1. As a pass which would take a parameter information and convert the corresponding variables into const?
2. As a pass which would convert operations into a more efficient one?
3. Pragma to help a pass?

- use a module pass to initialize built-in functions
- use a function pass to harden parameters and logics

-- multiple templates are written in c/c++ to generate the required template IR code
-- data processing logic can be written in any language that can emit LLVM IR, this would allow any developers to participate in the ecosystem
-- all code is fusioned/linked together dynamically at runtime and passed to LLVM for optimization based on statistics
-- custom passes to further optimize based on statistics, operator semantics, operator characteristics, vectorize, and target specific optimization, for example ARM

Quick PoC??

##### TODO:

1. need a way to specify what can be hardened??

OmniCache Purpose:

A common runtime for analytics workload with Set operations such as filter, union, join, etc.

1. Is the only value with LLVM to provide a faster expression evaluator??

Design:

1. some experssions in SQL are compiled into native code using LLVM to operate on off-heap memory, which will evetually be moved to memory fabric
2. the off-heap memory should hold stardard constructs such as "vector" which would allow us to take advantage of any optimizations around these constructs. e.g. clang 12 is adding vectorization. vectors are stored in continuous memory location.
3. optimizer to "fusion" expressions into "one single" expression to avoid memory copy between JVM and the off-heap memory

Other options:

1. replace ASM?? Delta Engine is a c/c++ engine (photon)

UDF would be pre-registered?? but is it really a good idea since it would require a function call?? how to remove function call?

expression --> a function --> typically a function with a loop over the input vector
data --> args

Current problems in presto code generation:

1. bytecode is generated directly
2. no optimization such as constant folding, deadcode removal, loop fusion, vectorization, etc. is applied
3.

### Design

we will use "assume" attribute to indicate:

1. the function in an operator template to be instrumented **kk_func**
2. the `oplet` to be injected `__kk_oplet__<id>.ll`
3. each `oplet` have a `unique id`, which will be used in place require uniqueness, for example variables added by the `oplet` will be prefixed by the `oplet` unique id
4. any template would have expose variables to be referenced by each snippet by name
5. variables contributed to the sampe location, regardless from which `ople`t`, will be defined in the same scope, in the order of the contribution
6. previsously defined/contributed variables are visible to the later oplets

#### Sample

##### Sample Templet

Sample GroupBy Templet

```c
const long TEMPLET_ID = <id> //must be unique
void preloop(int size) __attribute__((assume("kkinject_preloop"))) {
    HashMap
}

void inloop(int i) __attribute__((assume("kkinject_inloop_assumption")))  {
}

void postloop(int size) __attribute__((assume("kkinject_inloop_assumption"))) {
}

#pragma
void process(int size) {
    int mystate = 0; //visible to all optlets
    preloop(size);
    for (int i=0; i<size; i++) {
        inloop(i);
    }
    postloop(size);
}
```

##### Sample Oplet

SUM Aggregator, will be saved in a file `oplet_<id>.c` which is compiled into file `oplet_<id>.ll`. a list of `oplet_<id>` will be passed to the `opfuser` to inject the `oplet` code into the `templet`. Each oplet will require the following information:

- oplet\_<id>
- layout, which contains information about the input and output data type, this will be used to change the data type of the corresponding variables.

```c
const long TARGET_TEMPLET_ID=<id>
void preloop(int size) {
    //the following code will be injected into the final operator
    long result[10000];
}

void inloop(int size, int id, row??) {
    //the following code will be injected into the final operator
    result[i] = result[i] + row[input];
}

```

##### Testing

```c

```

#### Prepare Templet and Oplet

1. clang-12 is required to support `assume` attribute
2. compile using the following command:
   `clang-12 -S -emit-llvm -fno-discard-value-names op_template.c`

### UNIT TESTS

3. parameter conversion test for differen types
4. ensure attribute removal test
5. instruction reduce test
6. find out what function pass can be used
7. test different # of columns
8. test different type of columns

#### PROBLEMS:

1. class method will have a `this` parameter added, parameter offset need to be added by 1, should change to use `parameter name` instead
2. `LLVM ERROR: Cannot emit physreg copy instruction` --> indicate wrong parameter type


### Optimization Pipeline:

    1. loop unswitch -> switch if and loop order
    2. loop fusion to reduce memory bandwidth requirements
    
##### Aggregator
    1. function inlining first
    2. loop fusion?
    3. inductoin variable optimization

- function inlining, opens up opportunity for optimizatoin
- calling context analysis to make function inlineing more powerful