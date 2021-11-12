cmake_minimum_required(VERSION 3.16)

project(multiqueue VERSION 2.0.0 LANGUAGES CXX C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

include(cmake/utils.cmake)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CTest)
include(GNUInstallDirs)

discourage_intree_builds()
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Read the system's L1 cache-line size and page size
execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE OUTPUT_VARIABLE DEFAULT_L1_CACHE_LINESIZE OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND getconf PAGESIZE OUTPUT_VARIABLE DEFAULT_PAGESIZE OUTPUT_STRIP_TRAILING_WHITESPACE)

option(multiqueue_BUILD_TESTS "Build unit tests for multiqueue" OFF)
option(multiqueue_BUILD_EXAMPLES "Build examples" OFF)
option(multiqueue_ABORT_MISALIGNMENT "Check if data is not aligned as expected" OFF)
option(multiqueue_ELEMENT_DISTRIBUTION "Support functions to examine element distributions" OFF)
option(multiqueue_BUILD_DOCUMENTATION "Build documentation" OFF)
set(L1_CACHE_LINESIZE ${DEFAULT_L1_CACHE_LINESIZE} CACHE STRING "Specify the assumed L1 cache linesize (bytes)")
set(PAGESIZE ${DEFAULT_PAGESIZE} CACHE STRING "Specify the assumed pagesize (bytes)")

if(NOT "${L1_CACHE_LINESIZE}" MATCHES "^[0-9]+")
  message(SEND_ERROR "Invalid L1 cache linesize")
endif()

if(NOT "${PAGESIZE}" MATCHES "^[0-9]+")
  message(SEND_ERROR "Invalid pagesize")
endif()

# The target to be linked against by other targets. This library is header-only
# and as such does not compile by itself. This target rather sets include
# directories and required compiler flags.
add_library(multiqueue INTERFACE)

# Also include headers generated at build time. When installing the library,
# all files will be copied to the same location such that no additional
# directories have to be included.
target_include_directories(
  multiqueue
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# We require c++17 or above
target_compile_features(
  multiqueue
  INTERFACE
  cxx_std_17
)

target_compile_definitions(multiqueue INTERFACE L1_CACHE_LINESIZE PAGESIZE)
if(multiqueue_ABORT_MISALIGNMENT)
  target_compile_definitions(multiqueue INTERFACE MQ_ABORT_MISALIGNMENT)
endif()
if(multiqueue_ELEMENT_DISTRIBUTION)
  target_compile_definitions(multiqueue INTERFACE MQ_ELEMENT_DISTRIBUTION)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

# The namespace alias can be used as link target if this project is a
# subproject.
add_library("multiqueue::multiqueue" ALIAS multiqueue)

set(EXTERNAL_DIR "${CMAKE_CURRENT_LIST_DIR}/external")

if(multiqueue_BUILD_TESTS)
  if(IS_DIRECTORY "${EXTERNAL_DIR}/Catch2")
    add_subdirectory("${EXTERNAL_DIR}/Catch2" EXCLUDE_FROM_ALL)
    list(APPEND CMAKE_MODULE_PATH "${EXTERNAL_DIR}/Catch2/extras")
  endif()
endif()

find_package(Doxygen)
if(multiqueue_BUILD_DOCUMENTATION)
  if(Doxygen_FOUND)
    doxygen_add_docs(
      doxygen
      "${CMAKE_CURRENT_LIST_DIR}/include/multiqueue"
      COMMENT
      "Generate documentation"
    )
  else()
    message(WARNING "Doxygen not found, documentation cannot be built")
  endif()
endif()

if(multiqueue_BUILD_TESTS)
  set(TEST_RUNNER_PARAMS "" CACHE STRING "Options added to the test runner")
  add_subdirectory(tests)
endif()

if(multiqueue_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(multiqueue_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

include(InstallRequiredSystemLibraries)

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/License.txt")
set(CPACK_RESOURCE_FILE_README ${CMAKE_CURRENT_LIST_DIR}/README.md)
include(CPack)

set(INSTALL_MODULEDIR "${CMAKE_INSTALL_LIBDIR}/cmake")
set(INSTALL_CMAKEDIR "${INSTALL_MODULEDIR}/multiqueue")

configure_package_config_file(
  "${CMAKE_CURRENT_LIST_DIR}/cmake/multiqueueConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfig.cmake"
  INSTALL_DESTINATION
  "${INSTALL_CMAKEDIR}"
  PATH_VARS
  INSTALL_MODULEDIR
)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfigVersion.cmake"
  VERSION
  ${PROJECT_VERSION}
  COMPATIBILITY
  SameMajorVersion
  ARCH_INDEPENDENT
)

# Define the targets to add to the export set. Since this is a header-only
# library, only the usage requirements are propagated
install(
  TARGETS
  multiqueue
  EXPORT
  multiqueueExport
)

# On install, copy the header files to the appropriate location.
install(
  DIRECTORY
  "include/multiqueue/"
  DESTINATION
  "${CMAKE_INSTALL_INCLUDEDIR}/multiqueue"
  COMPONENT
  Development
  FILES_MATCHING PATTERN
  "*.hpp"
)

# Copy the package config file to the appropriate directory so that it can be found by find_package()
install(
  FILES
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfigVersion.cmake"
  DESTINATION
  "${INSTALL_CMAKEDIR}"
)

# Install the export set consisting of the multiqueue target
install(
  EXPORT
  multiqueueExport
  DESTINATION
  "${INSTALL_CMAKEDIR}"
  NAMESPACE
  multiqueue::
  FILE
  multiqueueTargets.cmake
)

if(multiqueue_INSTALL_DOCS)
  install(
    DIRECTORY
    "${CMAKE_CURRENT_BINARY_DIR}/html"
    DESTINATION
    "${CMAKE_INSTALL_DOCDIR}"
  )
endif()

# This target is only used by internal targets that use the multiqueue.
add_library(multiqueue_internal INTERFACE)

string(
  CONCAT WARNING_FLAGS
  "-Wall;"
  "-Wextra;"
  "-Wcast-align;"
  "-Wconversion;"
  "-Wnoexcept;"
  "-Wnon-virtual-dtor;"
  "-Wnull-dereference;"
  "-Woverloaded-virtual;"
  "-Wpedantic;"
  "-Wsuggest-attribute=const;"
  "-Wsuggest-attribute=noreturn;"
  "-Wshadow;"
  "-Wsign-conversion;"
  "-Wsuggest-override;"
  "-Wundef;"
  "-Wunreachable-code;"
  "-Wunused;"
)

target_compile_options(
  multiqueue_internal
  INTERFACE
  ${WARNING_FLAGS}
)

target_link_libraries(
  multiqueue_internal
  INTERFACE
  multiqueue
)
