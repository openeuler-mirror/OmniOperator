if(ENABLE_HMPP)
    file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
else()
    file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
    list(REMOVE_ITEM OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/hmpp_hash_util.cpp)
endif()
set(OP_TARGET ${OMNI_OPERATOR_SO})
find_package(nlohmann_json 3.7.3 REQUIRED)
# compile .a file
add_library(${OP_TARGET} SHARED ${OPERATOR_LIST})

#dependent library
if(ENABLE_HMPP)
    target_link_libraries(${OP_TARGET} PUBLIC expression util ${OMNI_CODEGEN_SO} ${OMNI_VECTOR_SO} HMPP_core HMPP_fftf HMPP_fft HMPP_image HMPP_signal)
else()
    target_link_libraries(${OP_TARGET} PUBLIC expression util ${OMNI_CODEGEN_SO} ${OMNI_VECTOR_SO})
endif(ENABLE_HMPP)
install(TARGETS ${OP_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})