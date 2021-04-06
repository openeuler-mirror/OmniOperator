aux_source_directory(. HAMMER)
add_library(harden ${HAMMER})

include_directories(/usr/lib/llvm-12/include)
include_directories(/usr/lib/llvm-12-c/include)
