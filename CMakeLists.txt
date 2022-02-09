cmake_minimum_required(VERSION 3.16)

include(cmake/utils.cmake)
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

discourage_intree_builds()
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

option(multififo_ABORT_MISALIGNMENT "Check if data is not aligned as expected" OFF)
option(multififo_ELEMENT_DISTRIBUTION "Support functions to examine element distributions" OFF)
set(multififo_DEFAULT_SELECTION_STRATEGY "" CACHE STRING "Default selection strategy")

project(multififo VERSION 1.0.0 LANGUAGES CXX)

# The target to be linked against by other targets. This library is header-only
# and as such does not compile by itself. This target rather sets include
# directories and required compiler flags.
add_library(multififo INTERFACE)

# Also include headers generated at build time. 
target_include_directories(
  multififo
  INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# We require c++17 or above
target_compile_features(
  multififo
  INTERFACE
  cxx_std_17
)

if(multififo_ABORT_MISALIGNMENT)
  target_compile_definitions(multififo INTERFACE MULTIFIFO_ABORT_MISALIGNMENT)
endif()
if(multififo_DEFAULT_SELECTION_STRATEGY)
  target_compile_definitions(multififo INTERFACE MULTIFIFO_DEFAULT_SELECTION_STRATEGY=${multififo_DEFAULT_SELECTION_STRATEGY})
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

# The namespace alias can be used as link target if this project is a
# subproject.
add_library("multififo::multififo" ALIAS multififo)
