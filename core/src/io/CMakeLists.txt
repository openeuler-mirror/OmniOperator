file(GLOB_RECURSE IO_CORE_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cc)
add_library(io_core ${IO_CORE_LIST})

find_package(Protobuf REQUIRED)
target_include_directories(io_core PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${PROTOBUF_INCLUDE_DIRS})
target_link_libraries(io_core PRIVATE
        boundscheck
        z
        lz4
        snappy
        zstd
        ${PROTOBUF_LIBRARIES}
)
