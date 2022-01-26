# Adding new external functions
To add new functions to omni-runtime, follow the steps below. You will need to modify: `externalfunctions.h`, `externalfunctions.cpp`, `externalregistration.conf`.

## Steps to create function
1.  If needed, create new `.h` and `.cpp` file for function types, otherwise put new functions in the `externalfunctions.h` and `externalfunctions.cpp` files  .
2. Add the function declaration of the  
   function in the `externalfunctions.h` file. Argument types can be `int32_t`,  `int64_t`, or `double` (pointers to a `string` can be represented with `int64_t`).
   ex:
   ```c++
   extern "C" DELLEXPORT int32_t add1(int32_t x);  
   ```
3. Write function in C++ in the `externalfunctions.cpp` file.
   ex:
   ```c++
   extern "C" DLLEXPORT int32_t add1(int32_t x) {
       return x + 1; 
   }  
   ```
​
6. Write the function argument types and return type in `externalregistration.conf`, in the following format:
   ```
   add1_int32: (INT32)->INT32 // add 1 to an integer
   // distance_double(x1, y1, x2, y2)
   // Finds the distance between (x1, y1) and (x2, y2)
   distance_double: (DOUBLE, DOUBLE, DOUBLE, DOUBLE) -> DOUBLE
   length_str: (INT64) -> INT32 // length of a string
   ```
First write the name of the function, (this must be the same as the name of the function in the filter/projection expression) followed by a colon. Place the argument types in parentheses, and then write the return type after the arrow `->`. You may add comments with `//` after the declaration. Allowed types are `INT32`, `INT64`, `DOUBLE`.
​
Blank lines and lines with only comments are also allowed, and will simply be ignored.

Make sure you place the updated `externalregistration.conf` in the `/<OMNI_HOME>/conf/` folder for the functions to be registered.

# Adding new in-built functions
## OmniRuntime Function Class
```
Function(void *address, string &fnName, vector<string> &aliases, vector<DataType> &paramTypes, DataType &retType, bool generateFuncID);
```
Constructs a omni-runtime `Function` object that contains the functionality and attributes of an omni-runtime function including function signature to facilitate registration and `funcID` to uniquely identify built-in or external functions in the `functions` dir.

- `fnName` denotes the name the function will be referenced by the query. `substr`, `LIKE`, `abs`, etc. are examples of fnNames for base functions.
- `generateFuncID` is used to generate a `funcID` that is used to identify the corresponding function from the `functions` dir that the omniruntime `Function` refers to.
- `aliases` allows us to specify multiple names for the same function
- `address` is the void function pointer
- `paramTypes` is a vector of datatypes of arguments - `VARCHAR` and `CHAR` are expanded to their corresponding function signature equivalent types to contain value, length for VARCHAR and value, length and width for `CHAR`
- `retType' is the datatype of the return value

## Modified Function Registry
- Instead of a single function registry class, each xxxfunctions.cpp file has a corresponding xxx_func_registry.cpp that appends the omniruntime `Function` to the single static vector `functionRegistry`.
- `LookupFunction` returns the omniruntime `Function` corresponding to the `funcID` provided.

## Modified Parser
- Old Implementation - Before creating a `FuncExpr`, we check the function signature to ensure that the data types of arguments and the number of arguments are correct.
- New Implementation - We use a helper function in parser helper to generate a funcID that matches the funcID used in the respective `func_registry_xxx` file where its corresponding omniruntime `Function` was created. We use this funcID to lookup the omniruntime Function that is passed to the `FuncExpr` constructor after checking that the function signature is correct.
- Instead of hardcoding the number of arguments we expect from each type of function, we use a map called FUNC_TO_NUM_ARGS in function.h that contains the number of functions acceptable for a given base function.

## Modified FuncExpr
`FuncExpr` contains a new field called `function` that stores the omniruntime `Function` object. This is used down the line in CodeGen to register the function using its function signature and create an llvm function call

## Modified Expression CodeGen
- `RegisterFunctions` method in codegen is used to register all the functions used in the query upfront.
- We get the list of functions that need to be registered from `ExprInfoExtractor`'s functions vector member variable.
```
llvm::FunctionType* ft = llvm::FunctionType::get(ToLlvmType(funcSignature.GetReturnType(), frContext), args, false);
llvm::FunctionCallee callee = module->getOrInsertFunction(funcSignature.GetName(), ft);
auto f = module->getFunction(funcName);
llvm::Value*  builder->CreateCall(f, argVals, funcName);
```
## Example
To create a new function called `CAST` that casts an argument of type `VARCHAR` to `DECIMAL128`, make the following changes.
- Add a new function under the mathfunctions.h/.cpp files that contains its functionality. Assume it's named CastStringToDecimal.
- Under its the func_registry_math.h/.cpp files, append a new omniruntime `Function` object. We check that the funcID used to lookup `CAST` functions is generated in the default format of basefunc_param1_retType. We don't need to add any special cases to generate funcID in the parser helper function, `GetFnIdentifier` because the generated `funcID` is different from the pre-existing funcIDs.
- We set the generateFuncID flag to true so that we can access the `Function` using this name in the parser. So we pass the following values to the new `Function` object,
```
Function(reinterpret_cast<void*>(CastStringToDecimal), castFnStr, {}, {VARCHAR}, DECIMAL128, true)

```
- The omniruntime constructor converts `VARCHAR` to `INT8PTRD` and `INT32` and `DECIMAL128` to `INT64D` when creating the function signature and stores it to the `signatures` member variable.
- Since the `CAST` function still accepts 1 argument. No change in needed in the `FUNC_TO_NUM_ARGS` map.
- Since this `Function` can be accessed through the `FuncExpr` member variable `function`, no change is need in the `ExprInfoExtractor`. Using the current code, `functions` vector will contain this new `Function` when used in a query.
- Finally, we create a unit test in the `CodegGenTest` suite to check if the function performs as expected.
