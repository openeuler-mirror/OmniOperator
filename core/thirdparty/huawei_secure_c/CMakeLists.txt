set(CMAKE_C_COMPILER "cc")
set(CMAKE_C_FLAGS_DEBUG "-O0 -fPIC")
set(CMAKE_C_FLAGS_RELEASE "-O2 -fPIC")
include_directories(include)
aux_source_directory(src SECUREC_DIR_SRC)
aux_source_directory(include SECUREC_DIR_HEADER)

set(SECUREC_TARGET securec)
set(WORK_SRC ${SECUREC_DIR_SRC})
set(WORK_HEAD ${SECUREC_DIR_HEADER})
add_library(${SECUREC_TARGET} SHARED ${WORK_SRC} ${WORK_HEAD})

message(STATUS "Install libsecurec.so into ${CMAKE_INSTALL_PREFIX}")
install(TARGETS ${SECUREC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})

