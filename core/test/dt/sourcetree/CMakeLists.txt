## modulefuzz
set(TARGET_LIBRARY modulefuzz)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${ROOT_SRCS})
add_library(${TARGET_LIBRARY} SHARED ${SRCS})
# dependent library
target_link_libraries(${TARGET_LIBRARY} jit jit_context memory util type ${OMNI_CODEGEN_SO} ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
install(TARGETS ${TARGET_LIBRARY} LIBRARY DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../installtree/product/lib/)