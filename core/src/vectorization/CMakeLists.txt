file(GLOB_RECURSE VECTORIZATION_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(VECTORIZATION_TARGET vectorization)
add_library(${VECTORIZATION_TARGET} ${VECTORIZATION_LIST})

find_package(fmt REQUIRED)
target_link_libraries(${VECTORIZATION_TARGET} PRIVATE util expression udf re2::re2 fmt::fmt)
file(GLOB EXPRESSION_HEAD_FILES ${SOURCE_ROOT}/src/vectorization/VectorFunction.h)
install(FILES ${EXPRESSION_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vectorization)