cmake_minimum_required(VERSION 3.19)

project(multiqueue VERSION 0.1.0 LANGUAGES CXX C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

include(utils)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CTest)
include(GNUInstallDirs)

prevent_intree_builds()

set(EXTERNAL_DIR "${CMAKE_CURRENT_LIST_DIR}/external")

option(${PROJECT_NAME}_USE_DOXYGEN "Add a doxygen target to generate the documentation" ON)
cmake_dependent_option(${PROJECT_NAME}_INSTALL_DOCS "Add the generated documentation to the install list" ON, ${PROJECT_NAME}_USE_DOXYGEN OFF)
option(${PROJECT_NAME}_INSTALL "Add ${PROJECT_NAME} to the install list" ON)
cmake_dependent_option("${PROJECT_NAME}_BUILD_TESTS" "Build tests for ${PROJECT_NAME}" ON "BUILD_TESTING" OFF)
cmake_dependent_option("${PROJECT_NAME}_SYSTEM_CATCH" "Use catch version installed on the system" OFF "${PROJECT_NAME}_BUILD_TESTS" OFF)
set("${PROJECT_NAME}_CATCH_DIR" "${EXTERNAL_DIR}/Catch2" CACHE PATH "Path to the Catch2 source directory")
option("${PROJECT_NAME}_BUILD_BENCHMARKS" "Build examples" OFF)
option("${PROJECT_NAME}_BUILD_EXAMPLES" "Build examples" OFF)
option(${PROJECT_NAME}_USE_DOXYGEN "Add a doxygen target to generate the documentation" ON)

# taken from klsm
execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE OUTPUT_VARIABLE CACHE_LINE_SIZE OUTPUT_STRIP_TRAILING_WHITESPACE)

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
add_library("${PROJECT_NAME}" INTERFACE)

target_include_directories(
  "${PROJECT_NAME}" INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# Include sources generated at build time. When installing the headers/library,
# these sources will be copied to appropriate locations such that no additional
# directories have to be included.
target_include_directories(
  "${PROJECT_NAME}" INTERFACE
  $<BUILD_INTERFACE:${PROJECT_CURRENT_BINARY_DIR}>
)

# This interface reflects the compile requirements for targets linking against this project.
add_library("${PROJECT_NAME}_compile_requirements" INTERFACE)

# We just want c++17 or above
target_compile_features(
  "${PROJECT_NAME}_compile_requirements" INTERFACE
  cxx_std_17
)

# Find the pthread library
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

target_link_libraries("${PROJECT_NAME}" INTERFACE "${PROJECT_NAME}_compile_requirements")

# The namespace alias can be used as link target if this project is a
# subproject.
add_library("${PROJECT_NAME}::${PROJECT_NAME}" ALIAS "${PROJECT_NAME}")

# Not added as link target to avoid propagation of warning
# flags.  Is used only by internal targets that compile the library.
add_library("${PROJECT_NAME}_warnings" INTERFACE)

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
  "${PROJECT_NAME}_warnings" INTERFACE
  $<BUILD_INTERFACE:${WARNING_FLAGS}>
)

add_subdirectory(utils)

if(${PROJECT_NAME}_BUILD_TESTS OR ${PROJECT_NAME}_BUILD_BENCHMARKS)
  if(${PROJECT_NAME}_SYSTEM_CATCH)
    find_package(Catch2 REQUIRED)
  else()
    if(NOT EXISTS "${${PROJECT_NAME}_CATCH_DIR}/CMakeLists.txt")
      message(FATAL_ERROR "Could not find Catch2 in ${${PROJECT_NAME}_CATCH_DIR}")
    endif()
    add_subdirectory("${${PROJECT_NAME}_CATCH_DIR}" EXCLUDE_FROM_ALL)
    list(APPEND CMAKE_MODULE_PATH "${${PROJECT_NAME}_CATCH_DIR}/contrib")
  endif()
  add_subdirectory("${EXTERNAL_DIR}/cxxopts")
  add_subdirectory(wrapper)
endif()

if(${PROJECT_NAME}_BUILD_TESTS)
  set(TEST_RUNNER_PARAMS "" CACHE STRING "Options added to the test runner")
  add_subdirectory(tests)
endif()

if(${PROJECT_NAME}_USE_DOXYGEN)
  find_package(Doxygen)
  if(DOXYGEN_FOUND)
    doxygen_add_docs(
      doxygen
      "${CMAKE_CURRENT_LIST_DIR}/include/${PROJECT_NAME}"
      COMMENT "Generate documentation"
    )
  else()
    message("Doxygen not found, skipping documentation generation")
  endif()
endif()

if(${PROJECT_NAME}_BUILD_BENCHMARKS)
  add_subdirectory(benchmark)
endif()

if(${PROJECT_NAME}_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(${PROJECT_NAME}_INSTALL)
  include(InstallRequiredSystemLibraries)
  set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/License.txt")
  set(CPACK_RESOURCE_FILE_README ${CMAKE_CURRENT_LIST_DIR}/README.md)
  include(CPack)

  set(INSTALL_MODULEDIR "${CMAKE_INSTALL_LIBDIR}/cmake")
  set(INSTALL_CMAKEDIR "${INSTALL_MODULEDIR}/${PROJECT_NAME}")

  configure_package_config_file(
    "${CMAKE_CURRENT_LIST_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${INSTALL_CMAKEDIR}"
    PATH_VARS INSTALL_MODULEDIR
  )

  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY
    SameMajorVersion
    ARCH_INDEPENDENT
  )

  # On install, copy the header files to the appropriate location.
  install(
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/${PROJECT_NAME}/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}"
    FILES_MATCHING PATTERN "*.hpp"
  )

  # Also copy the artefacts generated at build time into the include directory.
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/version.hpp"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}"
  )

  install(TARGETS "${PROJECT_NAME}" "${PROJECT_NAME}_compile_requirements" EXPORT "${PROJECT_NAME}Export")

  install(
    FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION
    "${INSTALL_CMAKEDIR}"
  )


  install(
    EXPORT "${PROJECT_NAME}Export"
    DESTINATION "${INSTALL_MODULEDIR}"
    NAMESPACE "${PROJECT_NAME}::"
    FILE "${PROJECT_NAME}Targets.cmake"
  )

  if(${PROJECT_NAME}_INSTALL_DOCS)
    install(
      DIRECTORY docs/
      DESTINATION "${CMAKE_INSTALL_DOCDIR}"
    )
  endif()
endif()
