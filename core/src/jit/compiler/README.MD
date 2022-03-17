#### Overview of JIT dependency loading
Some dependencies are needed by the OmniRuntime JIT in order to compile specialized functions. These libraries are found by recursively searching all files in the colon separated paths in the `LD_LIBRARY_PATH` environment variable. The only exception to this is stdc++, whos library file does not need to be in `LD_LIBRARY_PATH`, in which case it will attempt to load from the `libstdc++.so` file depended on by the Omniruntime process itself. Note that any `libstdc++.so` file in `LD_LIBRARY_PATH` will take precedence over this. By default, the following libraries in the format `library_name (file_name)` are searched for and loaded:  
- vector (libomni_vector.so)
- jemalloc (libjemalloc.so)
- stdc++ (libstdc++.so)
In addition, the following shared object libraries, compiled from OmniRuntime code, are also depended on by specialized functions. The build script places them in `jit_libs` folder in omni lib folder by default.
- aggregator (libaggregator.so)
- group_aggregation (libgroup_aggregation.so)
- hash_builder (libhash_builder.so)
- hash_util (libhash_util.so)
- join_hash_table (libjoin_hash_table.so)
- lookup_join (liblookup_join.so)
- memory_pool (libmemory_pool.so)
- non_group_aggregation (libnon_group_aggregation.so)
- pages_hash_strategy (libpages_hash_strategy.so)
- pages_index (libpages_index.so)
- sort (libsort.so)
- topn (libtopn.so)
- window_function (libwindow_function.so)
- window_partition (libwindow_partition.so)
- window (libwindow.so)

#### Adding dependencies
In addition to the libraries listed above, additional dependencies are read in from a config file, loaded from `dependencies.txt` in omni lib folder by default. Additional dependencies should be specified for any newly created specialized functions that rely on them. These additional dependencies will be loaded in _after_ all the dependencies listed above and in the order that they appear in the config file, unless it is an override of any of those libraries (more on this below), in which case it will be loaded in place of the original. This means that if library A relies on library B, library B should appear first in the config file, before library A. Any of these added dependencies may also rely on any of the libraries listed above to be present and already loaded. The dependencies are read in line by line with whitespace separated arguments. The dependencies should be formatted as follows:  
- The first argument is the name of the library, this is how it will appear in any debug logs.  
- The second argument is the name of the file to search for in `LD_LIBRARY_PATH`. Any `.so` file whos name _contains_ this argument will be considered candidates to satisfy this dependency. This means that if the argument was `jemalloc`, all of `jemalloc.so`, `libjemalloc.so`, and `libjemalloc.so.2` will be candidates for this dependency. This argument can be omitted, in which case the first argument, the library name, will be used as the file name.  
An optional third argument can be provided to represent the preferred path to load the library from. If provided, this path should exist within `LD_LIBRARY_PATH`. The loader will prioritize the preferred path when selecting which candidate file to load, if multiple are found. The preferred path may be a partial  path, in which case any file whos path ends in this argument will be considered a match. For instance, if `lib/libomni_vector.so` was the argument, it would match both of `/opt/lib/libomni_vector.so`, `/usr/local/lib/libomni_vector.so` but not `/home/lib/vector/libomni_vector.so`. An absolute path can also be used. If no candidates are from the preferred path, or if no path is specified, the first file the loader encounters that satisfies the file name will be used.  
For example, to load a library called `dummy`, stored in a file `libdummy.so`, one could specify the following in `dependencies.txt`:  
`dummy libdummy.so`  
This will load a `.so` file with `libdummy.so` in its name, if it exists in `LD_LIBRARY_PATH`. If multiple such files are found, suppose in `/opt/lib/libdummy.so` and `/home/libdummy.so`, one could specify which one to use:  
`dummy libdummy.so /opt/lib/libdummy.so`  
This will choose the one located at `/opt/lib/libdummy.so`. If for some reason it is missing or not in `LD_LIBRARY_PATH`, it will fallback to the one at `/home/libdummy.so`.  
If no candidates are found for a given library, a message will be printed to stderr and the loader will continue. If there are any libraries that could not be found, `LibraryLoader::FinishedLoading()` will return false.

#### Overriding dependencies
If there is a specific file that should be loaded to satisfy any of the libraries listed above, instead of the default behavior of searching for the first matching file name, it can be overriden in `dependencies.txt`. Simply specify the first argument, the library name, to be the same as one of the names listed, and this dependency will be used in place of it. For instance, if multiple versions of jemalloc exist in `LD_LIBRARY_PATH`, suppose in `/opt/lib/libjemalloc.so` and `/usr/local/lib/libjemalloc.so`, and the default behavior loaded in the first one, an override can be used to load in the second one instead as follows:  
`jemalloc libjemalloc.so /usr/local/lib/libjemalloc.so`  
In addition, the file name argument can be changed in case the desired library to use is named differently. For instance, if the file names were instead `jemalloc.so`, the following override can be used:  
`jemalloc jemalloc.so /usr/local/lib/jemalloc.so`
