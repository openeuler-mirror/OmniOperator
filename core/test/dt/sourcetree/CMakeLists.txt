## modulefuzz
set(TARGET_LIBRARY modulefuzz)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${ROOT_SRCS})
add_library(${TARGET_LIBRARY} SHARED ${SRCS})
# dependent library
add_compile_options(-fPIC)
target_link_libraries(${TARGET_LIBRARY} memory util testutil type gtest ${OMNI_CODEGEN_SO} ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
target_include_directories(${TARGET_LIBRARY} PUBLIC ${SOURCE_ROOT}/test)
install(TARGETS ${TARGET_LIBRARY} LIBRARY DESTINATION ${SOURCE_ROOT}/test/dt/installtree/product/lib/)