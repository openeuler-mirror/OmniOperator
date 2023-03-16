## modulefuzz
set(TARGET_LIBRARY modulefuzz)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${ROOT_SRCS})
add_library(${TARGET_LIBRARY} SHARED ${SRCS})
# dependent library
target_link_libraries(${TARGET_LIBRARY} memory util testutil type ${OMNI_CODEGEN_SO} ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
target_include_directories(${TARGET_LIBRARY} PUBLIC ${PROJECT_SOURCE_DIR}/test)
install(TARGETS ${TARGET_LIBRARY} LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/test/dt/installtree/product/lib/)