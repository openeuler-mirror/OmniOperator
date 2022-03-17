aux_source_directory(${CMAKE_CURRENT_LIST_DIR} JNI_LIBS_SOURCE)
set(JNI_TARGET jni)
add_library(${JNI_TARGET} ${JNI_LIBS_SOURCE})

# dependent include
target_include_directories(${JNI_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${JNI_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${JNI_TARGET} PUBLIC "${PROJECT_SOURCE_DIR}")

# dependent library
target_link_libraries(${JNI_TARGET} PUBLIC jit jit_context)
