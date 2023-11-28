cmake_minimum_required(VERSION 3.16)

project(multiqueue VERSION 3.0.0 LANGUAGES CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(utils)
disable_intree_builds()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

set(EXTERNAL_DIR "${CMAKE_CURRENT_LIST_DIR}/third_party")

option(MULTIQUEUE_DOXYGEN "Generate Doxygen target" OFF)
option(MULTIQUEUE_BUILD_EXAMPLES "Build examples" OFF)
option(MULTIQUEUE_BUILD_TESTS "Build unit tests for multiqueue" ON)
option(MULTIQUEUE_BUILD_BENCHMARKS "Build micro benchmarks for multiqueue internals" ON)
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

target_include_directories(
  multiqueue
  SYSTEM INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/multiqueue/third_party/pcg>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/multiqueue/third_party/pcg>
)

# We require c++17 or above
target_compile_features(
  multiqueue
  INTERFACE
  cxx_std_17
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)
find_package(Boost)
find_package(Doxygen)

# The namespace alias can be used as link target if this project is a
# subproject.
add_library("multiqueue::multiqueue" ALIAS multiqueue)

if(NOT TARGET Catch2::Catch2WithMain)
  if(EXISTS "${EXTERNAL_DIR}/Catch2/CMakeLists.txt")
    add_subdirectory("${EXTERNAL_DIR}/Catch2" EXCLUDE_FROM_ALL)
  else()
    message(WARNING "Catch2 not found, building tests and benchmarks not possible")
  endif()
endif()

if(MULTIQUEUE_BUILD_TESTS AND TARGET Catch2::Catch2WithMain)
  include(CTest)
  set(TEST_RUNNER_PARAMS "" CACHE STRING "Options added to the test runner")
  add_subdirectory(tests)
endif()
if(MULTIQUEUE_BUILD_BENCHMARKS AND TARGET Catch2::Catch2WithMain)
  add_subdirectory(benchmarks)
endif()
if(MULTIQUEUE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if (MULTIQUEUE_DOXYGEN)
  if(Doxygen_FOUND)
    doxygen_add_docs(
    doxygen
    "${CMAKE_CURRENT_LIST_DIR}/include/multiqueue"
    COMMENT
    "Generate documentation"
    )
  else()
    message(WARNING "Doxygen not found, skipping documentation generation")
  endif()
endif()

include(InstallRequiredSystemLibraries)

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/License.txt")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_LIST_DIR}/README.md")
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

install(
  DIRECTORY
  "${CMAKE_CURRENT_BINARY_DIR}/html"
  DESTINATION
  "${CMAKE_INSTALL_DOCDIR}"
)

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
