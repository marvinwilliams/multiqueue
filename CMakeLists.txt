cmake_minimum_required(VERSION 3.16)

project(multiqueue VERSION 0.1.0 LANGUAGES CXX C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

include(utils)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CTest)
include(GNUInstallDirs)

prevent_intree_builds()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()

option("multiqueue_BUILD_TESTS" "Build unit tests for multiqueue" OFF)
option("multiqueue_BUILD_BENCHMARKS" "Build micro benchmarks" OFF)
option("multiqueue_BUILD_EXAMPLES" "Build examples" OFF)
option(multiqueue_GENERATE_DOCS "Add a target to generate the documentation with doxygen" OFF)
cmake_dependent_option(multiqueue_INSTALL_DOCS "Add the generated documentation to the install list" ON, multiqueue_GENERATE_DOCS OFF)
option("multiqueue_ABORT_MISALIGNED" "Abort if assumptions about alignment are violated" ON)

execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE OUTPUT_VARIABLE L1_CACHE_LINESIZE OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND getconf PAGESIZE OUTPUT_VARIABLE PAGESIZE OUTPUT_STRIP_TRAILING_WHITESPACE)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

configure_file(
  "version.hpp.in"
  "version.hpp"
)

configure_file(
  "system_config.hpp.in"
  "system_config.hpp"
)

# The target to be linked against by other targets. This library is an
# interface target and as such does not generate any artefacts. It rather sets
# include directories and required compiler flags.
add_library("multiqueue" INTERFACE)

# Include sources generated at build time. When installing the headers/library,
# these sources will be copied to appropriate locations such that no additional
# directories have to be included.
target_include_directories(
  "multiqueue" INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# This interface reflects the compile requirements for targets linking against the multiqueue.
add_library("multiqueue_compile_requirements" INTERFACE)

# We just want c++17 or above
target_compile_features(
  "multiqueue_compile_requirements" INTERFACE
  cxx_std_17
)

find_package(Numa)

if (Numa_FOUND)
  target_compile_definitions("multiqueue_compile_requirements" INTERFACE HAVE_NUMA)
  target_link_libraries("multiqueue_compile_requirements" INTERFACE "numa")
endif()

if (multiqueue_ABORT_MISALIGNED)
  target_compile_definitions("multiqueue_compile_requirements" INTERFACE ABORT_MISALIGNED)
endif()

target_link_libraries("multiqueue" INTERFACE "multiqueue_compile_requirements")

# The namespace alias can be used as link target if this project is a
# subproject.
add_library("multiqueue::multiqueue" ALIAS "multiqueue")

# Not added as link target to avoid propagation of
# flags.  Is used only by internal targets.
add_library("multiqueue_internal" INTERFACE)

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
  "multiqueue_internal" INTERFACE
  $<BUILD_INTERFACE:${WARNING_FLAGS}>
)

set(EXTERNAL_DIR "${CMAKE_CURRENT_LIST_DIR}/external")

if(IS_DIRECTORY "${EXTERNAL_DIR}/Catch2")
  add_subdirectory("${EXTERNAL_DIR}/Catch2" EXCLUDE_FROM_ALL)
  list(APPEND CMAKE_MODULE_PATH "${EXTERNAL_DIR}/Catch2/extras")
  set(Catch2_FOUND TRUE)
else()
  message(WARNING "Could not find Catch2 in ${EXTERNAL_DIR}")
  set(Catch2_FOUND FALSE)
endif()

if(multiqueue_GENERATE_DOCS)
  find_package(Doxygen)
  if(DOXYGEN_FOUND)
    doxygen_add_docs(
      doxygen
      "${CMAKE_CURRENT_LIST_DIR}/include/multiqueue"
      COMMENT "Generate documentation"
    )
  else()
    message(WARNING "Doxygen not found, documentation cannot be built")
  endif()
endif()

if(multiqueue_BUILD_TESTS)
  set(TEST_RUNNER_PARAMS "" CACHE STRING "Options added to the test runner")
  add_subdirectory(unit_tests)
endif()

if(multiqueue_BUILD_BENCHMARKS)
  add_subdirectory(micro_benchmarks)
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
  INSTALL_DESTINATION "${INSTALL_CMAKEDIR}"
  PATH_VARS INSTALL_MODULEDIR
  )

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY
  SameMajorVersion
  ARCH_INDEPENDENT
  )

# On install, copy the header files to the appropriate location.
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/multiqueue/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/multiqueue"
  COMPONENT Development
  FILES_MATCHING PATTERN "*.hpp"
  )

# Also copy the artefacts generated at build time into the include directory.
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/version.hpp" "${CMAKE_CURRENT_BINARY_DIR}/system_config.hpp"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/multiqueue"
  )

install(TARGETS "multiqueue" "multiqueue_compile_requirements" EXPORT "multiqueueExport")

install(
  FILES
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/multiqueueConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/multiqueueConfigVersion.cmake"
  DESTINATION
  "${INSTALL_CMAKEDIR}"
  )


install(
  EXPORT "multiqueueExport"
  DESTINATION "${INSTALL_MODULEDIR}"
  NAMESPACE "multiqueue::"
  FILE "multiqueueTargets.cmake"
  )

if(multiqueue_INSTALL_DOCS)
  install(
    DIRECTORY docs/
    DESTINATION "${CMAKE_INSTALL_DOCDIR}"
    )
endif()
