aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/src JNI_LIBS_SOURCE)

set(BINDING_TARGET boostkit-omniop-java-binding-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})

add_library(${BINDING_TARGET} SHARED ${JNI_LIBS_SOURCE})

# dependent include
target_include_directories(${BINDING_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${BINDING_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)

target_link_libraries(${BINDING_TARGET} PUBLIC  ${OMNI_OPERATOR_SO} ${OMNI_CODEGEN_SO} ${OMNI_VECTOR_SO})

if (EXISTS $ENV{OMNI_HOME})
    file(REMOVE_RECURSE $ENV{OMNI_HOME}/java-binding)
    set_target_properties(${BINDING_TARGET} PROPERTIES LIBRARY_OUTPUT_DIRECTORY $ENV{OMNI_HOME}/java-binding)
    set_target_properties(${BINDING_TARGET} PROPERTIES SKIP_BUILD_RPATH YES)
endif()


