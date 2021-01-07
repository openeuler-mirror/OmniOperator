#### How to compile

1. run the src/main/cpp/vector/build.sh, which will generate the c header and compiles the c/c++ code into a .so
2. copy the .so into the right location, this location should be the same as the `System.load("/opt/kkrazy/hetu-core/omni-cache/src/main/java/omnicache.so");` 
3. run the sample code in `DoubleVec`


#### Overview
The OmniCache is designed to

* Fast OmniCache data exchange, potential via TCP/UPD/RDMA to directly and efficiently transfer vectors into the memory
* Support inter-language zero-copy
* SIMD optimization via Weld/LLVM

The OmniCache is library used to implement higher level data analytics logics

##### OMVector - 
The OmniCache provides c/c++ interface similar to `vector` with Java binding using JNI. The `OMVector` also provides SIMD enabled
operations called 'in-situ operation', which normally takes another `vector` as parameter. For scalar parameters, it will be
a `vector` with single value.

We try to allocate the memory needed for the vector in continuous space as much as possible. Not requiring all
elements stored in continuous memory space allows us to expand the `vectors` when needed. Separated allocated 
memory spaces are referred to as `chunk`, which is the smallest unit of operator for allocation and de-allocation.

To enable fast computation, the `vector` stores metadata such as `min`/`max`/`average`/`sum`/`bitmap`, this will help with
aggregation and locating a specific element in the `vector`. The `vector` will also keep track of the `last used timestamp` of the chunks
in the `vector`. 

##### OMCache
OmniCache will also provide a `OMCache` API which keep tracks of all the loaded `OMVector`. The OMCache also maintains the mapping between 
the `OMVector` and the `Table`, e.g. the schema informtion to support SQL alike operations


##### Transport
TO BE ADDED

##### Java Binding
MORE DETAILS TO BE ADDED

###### Vector - the base java class
###### LongVector - long data type
###### VarcharVector - variable length string
...