# Adding new external functions
To add new functions to omni-runtime, follow the steps below. You will need to modify: `externalfunctions.cpp`, `func_registry.cpp`.

## Steps to create function
1. If needed, create new `.cpp` file for function types such as `mathfunctions.cpp`, otherwise put new functions in the `externalfunctions.cpp` file.
2. Write function in C++ in the `externalfunctions.cpp` file(If you want to use template for your functions you can put your implementation in header).
   ex:
   ```c++
   extern "C" int32_t add1(int32_t x) {
       return x + 1; 
   }  
   ```
   Parameter types can be `int32_t`,  `int64_t`, `double`, `boolean`, `string` or `decimal`.
   * For variable length `string` parameter aka `VARCHAR`, it's passed in as `char*`(pointer to the data) and `int32_t`(length of the string)
   * For fixed length `string` parameter aka `CHAR(width)`, it's passed in as `char*`(pointer to the data) `int32_t`(width) and `int32_t`(length of the string)
   * For `decimal` 128bit parameter, it's passed in as `int64_t`(high 64 bit) and `int64_t`(low 64 bit) values.
   * If you need to allocate memory for returning `string` values, you can also pass in a `int64_t` in the beginning of parameter list as the pointer address to an `ExecutionContext` object, and use this object to allocate new memory for better performance and void memory leak

   Return type can also be `int32_t`,  `int64_t`, `double`, `boolean`, `string` or `decimal`.
   * For `string` return type, the function return type should be `char*`, but a pointer to the return string length will also be passed in at the end of the param list
   * For `decimal` 128bit return type, the pointers to the high bits and low bits must be passed in at the end of the param list.

3. Register your functions in Function Registry:
   You can either register the functions in `external_func_registry.cpp` or create your own registry
   
   * If adding to `external_func_registry`, you only need to add your function in the `GetFunctions()` method.
   * If creating your own registry, you need to implement the `BaseFunctionRegistry` interface in `func_registry_base.h` and add all your functions in `GetFunctions()` method.

   ```c++
   vector<Function> ExternalFunctionRegistry::GetFunctions()
   {
       std::vector<Function> externalFunctionRegistry = {
               Function("Increment_int32", {}, {OMNI_INT}, OMNI_INT),
               Function("Increment_int32", {}, {OMNI_LONG}, OMNI_LONG),
       };
       return externalFunctionRegistry;
   }
   ```
   
   The return types and parameter types in function signature registered can only be the data types, currently supporting:
   * OMNI_INT
   * OMNI_LONG
   * OMNI_DOUBLE
   * OMNI_BOOLEAN
   * OMNI_VARCHAR
   * OMNI_CHAR
   * OMNI_DECIMAL64
   * OMNI_DECIMAL128
   

4. Finally, if you are adding a new function registry, register it in the `FunctionRegistry` class in `func_registry` by adding it to the registries list in `GetFunctionRegistries()` method:

   ```c++
   vector<unique_ptr<BaseFunctionRegistry>> FunctionRegistry::GetFunctionRegistries()
   {
      vector<unique_ptr<BaseFunctionRegistry>> functionRegistries;
      // Other registries...
      // External functions
      functionRegistries.push_back(make_unique<ExternalFunctionRegistry>());
      // Put your registry here
   
      return functionRegistries;
   }
   ```

# Adding new functions
## OmniRuntime Function Class
```
Function(string &name, vector<string> &aliases, vector<DataType> &paramTypes, DataType &retType, bool setExecutionContext);
```
Constructs a omni-runtime `Function` object that contains the functionality and attributes of an omni-runtime function including function signature to facilitate registration and `funcID` to uniquely identify built-in or external functions in the `functions` dir.

- `name` function name, needs to be exact same as the function that will be called.
- `aliases` allows us to specify multiple names for the same function
- `paramTypes` is a vector of data types of arguments - `VARCHAR` and `CHAR` are expanded to their corresponding function signature equivalent types to contain value, length for VARCHAR and value, length and width for `CHAR`
- `retType` is the data type of the return value
- `setExecutionContext` if true - pass the execution context to func signature as a param, it will always be the first parameter in your function, default to false

## Function Registry
- Instead of a single function registry class, each xxxfunctions.cpp file has a corresponding xxx_func_registry.cpp that appends the omniruntime `Function` to the single static vector `functionRegistry`.
- `LookupFunction` returns the omniruntime `Function` corresponding to the `funcID` provided.
