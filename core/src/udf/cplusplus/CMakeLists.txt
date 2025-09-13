aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UDF_LIST)
set(UDF_TARGET udf)
add_library(${UDF_TARGET} ${UDF_LIST})

find_package(JNI REQUIRED)

# dependent include
target_include_directories(${UDF_TARGET} PUBLIC ${JAVA_INCLUDE_PATH})
target_include_directories(${UDF_TARGET} PUBLIC ${JAVA_INCLUDE_PATH2})
target_link_libraries(${UDF_TARGET} PUBLIC ${JAVA_JVM_LIBRARY})