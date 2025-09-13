##### Requirements
groupby is one of the building block of BI which is widely used to generate high level insights of the data.

We aim to create a set of groupby algorithms which can taking into account the metadata before generating the ASM code. 
The following metadata will be taken into account:

`sorted`: an `O(n)` algorithm can be easily achieved via scan through the data
`cardinality`: when cardinality lower than a threshold which allows the ptrs to fit in memory without impacting the system, we can use `perfect identify hash` and use an array to directory 
store the aggregation result, this would also allow `O(n)` complexity
`data dictionary`: which can be used to transform strings into numerical values

##### Optimizations
###### Sorted field groupby

