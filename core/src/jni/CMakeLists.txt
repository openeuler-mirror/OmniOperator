aux_source_directory(${CMAKE_CURRENT_LIST_DIR} JNI_LIBS_SOURCE)
set(JNI_TARGET jni)
add_library(${JNI_TARGET} ${JNI_LIBS_SOURCE})

# dependent include
target_include_directories(${JNI_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${JNI_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${JNI_TARGET} PUBLIC "${PROJECT_SOURCE_DIR}")
target_include_directories(${JNI_TARGET} PUBLIC /usr/lib/llvm-12/include)
