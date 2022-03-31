message("Precompile codegen functions...")

set(PRECOMPILED_SRCS
        context_helper.cpp
        decimalfunctions.cpp
        dictionaryfunctions.cpp
        externalfunctions.cpp
        mathfunctions.cpp
        murmur3_hash.cpp
        stringfunctions.cpp
        varcharVectorfunctions.cpp)

set(COMPILE_OPTION -S -O2 -emit-llvm -fno-discard-value-names)
set(LLVM_INCLUDE -I/usr/lib/llvm-12/include)

# Create bitcode for each of the source files
foreach(SRC_FILE ${PRECOMPILED_SRCS})
    get_filename_component(SRC_BASE ${SRC_FILE} NAME_WE)
    get_filename_component(ABSOLUTE_SRC ${SRC_FILE} ABSOLUTE)
    set(BC_FILE ${PROJECT_BINARY_DIR}/${SRC_BASE}.bc)
    set(PRECOMPILE_COMMAND)
    list(APPEND
         PRECOMPILE_COMMAND
         COMMAND
         ${IR_COMPILER}
         ${COMPILE_OPTION}
         ${LLVM_INCLUDE}
         -c
         ${ABSOLUTE_SRC}
         -o
         ${BC_FILE}
         -I${CMAKE_SOURCE_DIR}/src)
    add_custom_command(OUTPUT ${BC_FILE}
            COMMAND ${PRECOMPILE_COMMAND}
            DEPENDS ${SRC_FILE})
    list(APPEND BC_FILES ${BC_FILE})
endforeach()

# link all the bitcode files to a single file
set(PRECOMPILED_BC_PATH "${PROJECT_BINARY_DIR}/functionsIR.bc")
# program used to link bc files is in home/llvm/bin
find_program(LLVM_LINK_EXECUTABLE NAMES llvm-link llvm-link-12 HINTS $ENV{PWD})
add_custom_command(OUTPUT ${PRECOMPILED_BC_PATH}
        COMMAND ${LLVM_LINK_EXECUTABLE} -o ${PRECOMPILED_BC_PATH}
        ${BC_FILES}
        DEPENDS ${BC_FILES})

# convert bitcode file into a C++ static char array var
set(PRECOMPILED_C_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../codegen/precompiled_bitcode.cpp")
set(PRECOMPILED_TMP_PATH ${PRECOMPILED_C_PATH}.tmp)
add_custom_command(
        OUTPUT ${PRECOMPILED_C_PATH}
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/get_precompiled_functions.sh ${PRECOMPILED_BC_PATH} > ${PRECOMPILED_TMP_PATH}
        COMMAND mv ${PRECOMPILED_TMP_PATH} ${PRECOMPILED_C_PATH}
        DEPENDS $ENV{get_precompiled_functions.sh}
        DEPENDS ${PRECOMPILED_BC_PATH}
)

add_custom_target(functions ALL DEPENDS
        ${PRECOMPILED_BC_PATH}
        ${PRECOMPILED_C_PATH})