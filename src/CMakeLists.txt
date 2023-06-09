cmake_minimum_required(VERSION 3.5.1)

project(db C CXX)

set(CMAKE_CXX_STANDARD 17)
include(cmake/common.cmake)
include(CheckFunctionExists)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Proto file
get_filename_component(hw_proto "db.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/db.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/db.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/db.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/db.grpc.pb.h")
add_custom_command(
        OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "."
        --cpp_out "."
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories(".")

# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})


# set(UNRELIABLEFS_SRC db_client.cc wrapper.cc  conf.c unreliablefs.c unreliablefs_errinj.c unreliablefs_ops.c )

add_executable(db_cleaner db_cleaner.cc)
target_link_libraries(db_cleaner PUBLIC leveldb)

add_executable(db_client db_client.cc)
target_link_libraries(db_client 
hw_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})

add_subdirectory(third_party/leveldb)
add_executable(db_server db_server.cc timer.cc)
target_link_libraries(db_server PUBLIC
    "stdc++fs"
    hw_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    leveldb)

target_compile_options(db_client PUBLIC -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-unused-parameter)
target_compile_options(db_client PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17 -D_FILE_OFFSET_BITS=64>)
target_compile_options(db_server PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)

check_function_exists(utimensat HAVE_UTIMENSAT)
check_function_exists(fallocate HAVE_FALLOCATE)
check_function_exists(fallocate HAVE_FLOCK)
if (${HAVE_UTIMENSAT})
  target_compile_definitions(db_client PUBLIC HAVE_UTIMENSAT)
endif ()
if (${HAVE_FALLOCATE})
    target_compile_definitions(db_client PUBLIC HAVE_FALLOCATE)
endif ()
if (${HAVE_FLOCK})
    target_compile_definitions(db_client PUBLIC HAVE_FLOCK)
endif ()

