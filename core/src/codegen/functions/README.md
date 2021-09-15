## Adding new functions
To add new functions to omni-runtime, follow the steps below. You will need to modify: `externalfunctions.h`, `externalfunctions.cpp`, `externalregistration.conf`.
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

4. Write the function argument types and return type in `externalregistration.conf`, in the following format: 
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

Finally, compile the `externalfunctions.h` and `externalfunctions.cpp` file to a shared library (ex. using the command below), and ensure that the `.so` file is placed in the same directory as `externalregistration.conf`, and is named `externalfunctions.so`. Place `externalregistration.conf` in the `/etc/externalfunctions/` folder, and place `externalfunctions.so` in the omni lib folder. 
```bash
clang+-12 -shared -fPIC -o externalfunctions.so externalfunctions.cpp
```

If there are more sets of `.h` and `.cpp` files which need to be added, simply add them to the compile command. For example, if there are also files for `extmathfunctions` and `extstringfunctions` with declarations and implementations in `.h` and `.cpp` files with the same name, run the following command: 
```bash
clang+-12 -shared -fPIC -o externalfunctions.so externalfunctions.cpp extmathfunctions.cpp extstringfunctions.cpp
```
Make sure to move the resulting `externalfunctions.so` file to omni lib folder. 