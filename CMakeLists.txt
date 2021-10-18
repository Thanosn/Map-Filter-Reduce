cmake_minimum_required(VERSION 3.5.1)


project(Hy543_MFR)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Ofast")

find_package(Threads REQUIRED)

# Find Protobuf installation
  # Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
  set(protobuf_MODULE_COMPATIBLE TRUE)
  find_package(Protobuf CONFIG REQUIRED)
  message(STATUS "Using protobuf ${Protobuf_VERSION}")

  set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
  set(_REFLECTION gRPC::grpc++_reflection)
  #py reflection
  set(_PYREFLECTION gRPC::grpcpy_reflection)

  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc> --experimental_allow_proto3_optional)
  # Find gRPC installation
  # Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
  find_package(gRPC CONFIG REQUIRED)
  message(STATUS "Using gRPC ${gRPC_VERSION}")

  set(_GRPC_GRPCPP gRPC::grpc++)
  set(_GRPC_GRPCPY gPRC::grpc_python)
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
  set(_GRPC_PY_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_python_plugin>)


  # Proto file
  get_filename_component(mfr_proto "./protos/mfr.proto" ABSOLUTE)
  get_filename_component(mfr_proto_path "${mfr_proto}" PATH)

  set(mfr_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/mfr.grpc.pb.cc")
  set(mfr_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mfr.grpc.pb.h")
  set(mfr_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/mfr.pb.cc")
  set(mfr_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mfr.pb.h")

  set(mfr_proto_srcs_py "${CMAKE_CURRENT_BINARY_DIR}/mfr_pb2.py") 
  set(mfr_grpc_srcs_py "${CMAKE_CURRENT_BINARY_DIR}/mfr_pb2_grpc.py")

  add_custom_command(
      OUTPUT "${mfr_proto_srcs}" "${mfr_proto_hdrs}" "${mfr_grpc_srcs}" "${mfr_grpc_hdrs}" "${mfr_proto_srcs_py}" "${mfr_grpc_srcs_py}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${mfr_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${mfr_proto}"
      DEPENDS "${mfr_proto}"
      
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_python_out "${CMAKE_CURRENT_BINARY_DIR}"
        --python_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${mfr_proto_path}"
        --plugin=protoc-gen-grpc_python="${_GRPC_PY_PLUGIN_EXECUTABLE}"
        "${mfr_proto}"
      DEPENDS "${mfr_proto}"
      )

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

include_directories("${PROJECT_SOURCE_DIR}/back-end/")
add_subdirectory("${PROJECT_SOURCE_DIR}/back-end/")

# Targets greeter_[async_](client|server)
foreach(_backend_target
  test_backend)
  add_executable(${_backend_target} "back-end/${_backend_target}.cc"
    ${mfr_proto_srcs}
    ${mfr_grpc_srcs})
  target_link_libraries(${_backend_target}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    backEndLib)
    add_dependencies(${_backend_target} backEndLib)
endforeach()

foreach(_backend_target
  master_backend)
  add_executable(${_backend_target} "back-end/${_backend_target}.cc"
    ${mfr_proto_srcs}
    ${mfr_grpc_srcs})
  target_link_libraries(${_backend_target}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    backEndLib)
    add_dependencies(${_backend_target} backEndLib)
endforeach()
foreach(_frontend_target
  source Parser MFRlib executionGraph)
  file(COPY frontend/${_frontend_target}.py DESTINATION .)
endforeach(_frontend_target)
