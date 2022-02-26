## Parser and Expressions

### Parser Specifications
The current parser must take in a `string` as input, and return a `Expr*` in a method with the following signature: 
```c++
Expr *parseRowExpression(string input, DataType *inputDataTypes, int32_t veccount);
```
`DataType` is an enum which is defined in `core/src/expression/expressions.h`, and covers all the possible types for a column. These are: `STRINGD`, `INT32D`, `INT64D`, `DOUBLED`, `BOOLD`. The `inputVecTypes` array contains the type of each column in order, and there are `veccount` columns in total.

 All the different kinds of `Expr*` are listed in `core/src/expression/expressions.h`. When parsing, each `Expr*` must have its `dataType` member set correctly (usually with the constructor for the `Expr` which takes in a data type. The different types of `Expr` are:
 `DataExpr`: Holds a piece of literal data or a column index.
 `UnaryExpr`: Represents a unary expression (such as NOT).
 `BinaryExpr`: Represents a binary expression (such as +, -, *, /, %, AND, OR, <, <=, >, >=, =, <>)
 `InExpr`: Represents an `IN` SQL operator. Holds a vector of arguments which are `Expr*`, and the first argument is the value to be compared to every other argument.
 `BetweenExpr`: Represents a `BETWEEN` SQL operator. Holds a value, lower bound, and upper bound, all of which are `Expr*`.
 `IfExpr`: Represents an SQL conditional. Holds a condition, an expression to be evaluated if the condition is true, and an expression to be evaluated if the condition is false; all of these are `Expr*`.
`CoalesceExpr`: Represents an SQL `COALESCE`. Has a value 1 and a value 2. If value 1 has a null value then value 2 is returned, otherwise value 1 is returned.
`FuncExpr`: Represents any function, whether internal or user-defined. Has a name and an argument vector of `Expr*`.

### Using `parserhelper`
In `core/src/expression/parserhelper.h` and its corresponding `.cpp` file, there are a few methods to help with parsing.

```c++
DataType FuncRetTypeMap(string funcID, vector<Expr*> args);
```
The `FuncRetTypeMap` method takes in a function name `funcID` and a vector containing its arguments (which are all `Expr*`), and returns a `DataType` denoting the returned type of the function.

```
bool HasValidArguments(string funcID, vector<Expr*> args, bool checkTypes);
```

The `HasValidArguments` method checks whether or not a `FuncExpr*` (i.e. an expression representing a function call) is valid, in terms of whether the number of arguments match up with the function name. If `true` is passed to the `checkTypes` argument, then the types will be checked as well, although types will not be checked for external developer functions.
