# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.30.4 FATAL_ERROR)

set(RAPIDS_CMAKE_BRANCH "v26.04.00" CACHE STRING "rapids-cmake branch or tag")
if(RAPIDS_CMAKE_BRANCH MATCHES "^v(.+)")
  set(rapids-cmake-version "${CMAKE_MATCH_1}")
  set(rapids-cmake-tag "${RAPIDS_CMAKE_BRANCH}")
else()
  set(rapids-cmake-branch "${RAPIDS_CMAKE_BRANCH}")
endif()
set(NATIVE_LIBRARY_NAME "rapidsudfjni" CACHE STRING "JNI shared library target name")
set(NATIVE_DEPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../../target/native-deps" CACHE PATH "Directory containing prebuilt libcudf")
set(CUDF_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../../target/cudf-repo/cpp" CACHE PATH "cuDF source directory for headers")
set(GPU_ARCHS "RAPIDS" CACHE STRING "CUDA architectures")

file(DOWNLOAD
  https://raw.githubusercontent.com/rapidsai/rapids-cmake/${RAPIDS_CMAKE_BRANCH}/RAPIDS.cmake
  ${CMAKE_BINARY_DIR}/RAPIDS.cmake
)
include(${CMAKE_BINARY_DIR}/RAPIDS.cmake)

include(rapids-cmake)
include(rapids-cpm)
include(rapids-cuda)

if(DEFINED ENV{CXX} AND NOT "$ENV{CXX}" STREQUAL "")
  set(CMAKE_CXX_COMPILER "$ENV{CXX}" CACHE FILEPATH "C++ compiler" FORCE)
endif()

if(DEFINED GPU_ARCHS)
  set(CMAKE_CUDA_ARCHITECTURES "${GPU_ARCHS}")
endif()
rapids_cuda_init_architectures(RAPIDSUDFJNI)

project(RAPIDSUDFJNI VERSION 26.04.0 LANGUAGES C CXX CUDA)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 20)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -w --expt-extended-lambda --expt-relaxed-constexpr")

option(USE_PREBUILT_CUDF "Use libcudf extracted from the rapids-4-spark jar" ON)
option(PER_THREAD_DEFAULT_STREAM "Build with per-thread default stream" ON)
option(CUDF_ENABLE_ARROW_S3 "Enable Arrow S3 support in source-build mode" OFF)

if(USE_PREBUILT_CUDF)
  if(NOT EXISTS "${NATIVE_DEPS_DIR}")
    message(FATAL_ERROR "NATIVE_DEPS_DIR does not exist: ${NATIVE_DEPS_DIR}")
  endif()
  if(NOT EXISTS "${CUDF_SOURCE_DIR}/include")
    message(FATAL_ERROR "CUDF_SOURCE_DIR headers not found: ${CUDF_SOURCE_DIR}/include")
  endif()

  find_library(CUDF_LIBRARY NAMES cudf PATHS "${NATIVE_DEPS_DIR}" NO_DEFAULT_PATH REQUIRED)

  get_property(rapids-cmake-dir GLOBAL PROPERTY rapids-cmake-dir)
  if(NOT rapids-cmake-dir)
    set(rapids-cmake-dir "${CMAKE_BINARY_DIR}/_deps/rapids-cmake-src")
  endif()

  rapids_cpm_init()
  include("${rapids-cmake-dir}/cpm/cccl.cmake")
  rapids_cpm_cccl()
  include("${rapids-cmake-dir}/cpm/rmm.cmake")
  rapids_cpm_rmm()

  if(NOT TARGET rmm::rmm)
    message(FATAL_ERROR "rmm::rmm target was not created")
  endif()

  get_target_property(RMM_INCLUDE_DIRS rmm::rmm INTERFACE_INCLUDE_DIRECTORIES)

  add_library(cudf_imported SHARED IMPORTED GLOBAL)
  set_target_properties(cudf_imported PROPERTIES IMPORTED_LOCATION "${CUDF_LIBRARY}")
  target_include_directories(cudf_imported INTERFACE
    "${CUDF_SOURCE_DIR}/include"
    ${RMM_INCLUDE_DIRS}
  )
  target_link_libraries(cudf_imported INTERFACE rmm::rmm)
  add_library(cudf::cudf ALIAS cudf_imported)
else()
  rapids_cpm_init()
  rapids_cpm_find(cudf 26.04.00
    CPM_ARGS
      GIT_REPOSITORY https://github.com/rapidsai/cudf.git
      GIT_TAG        ${RAPIDS_CMAKE_BRANCH}
      GIT_SHALLOW    TRUE
      SOURCE_SUBDIR  cpp
      OPTIONS        "BUILD_TESTS OFF"
                     "BUILD_BENCHMARKS OFF"
                     "CUDF_ENABLE_ARROW_S3 ${CUDF_ENABLE_ARROW_S3}"
                     "CUDF_KVIKIO_REMOTE_IO OFF"
                     "DISABLE_DEPRECATION_WARNING ON"
                     "AUTO_DETECT_CUDA_ARCHITECTURES OFF"
  )
endif()

find_package(JNI REQUIRED)

set(SOURCE_FILES
  "src/PlaceholderUDFNameJni.cpp"
  "src/placeholder_udf_name.cu"
)

add_library(${NATIVE_LIBRARY_NAME} SHARED ${SOURCE_FILES})
set_target_properties(${NATIVE_LIBRARY_NAME} PROPERTIES BUILD_RPATH "\$ORIGIN")

if(PER_THREAD_DEFAULT_STREAM)
  target_compile_definitions(${NATIVE_LIBRARY_NAME} PRIVATE CUDA_API_PER_THREAD_DEFAULT_STREAM)
endif()

target_include_directories(${NATIVE_LIBRARY_NAME} PRIVATE ${JNI_INCLUDE_DIRS})
target_compile_definitions(${NATIVE_LIBRARY_NAME} PUBLIC SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_OFF)
target_link_libraries(${NATIVE_LIBRARY_NAME} cudf::cudf)
