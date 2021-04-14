#[=======================================================================[.rst:
FindNuma
-------

Finds the Numa library.

Imported Targets
^^^^^^^^^^^^^^^^

This module provides the following imported targets, if found:

``Numa::Numa``

Result Variables
^^^^^^^^^^^^^^^^

This will define the following variables:

``Numa_FOUND``
  True if the system has the Numa library.
``Numa_INCLUDE_DIRS``
  Include directories needed to use libnuma.
``Numa_LIBRARIES``
  Libraries needed to link to numa.

Cache Variables
^^^^^^^^^^^^^^^

The following cache variables may also be set:

``Foo_INCLUDE_DIR``
  The directory containing ``numa.h``.
``Foo_LIBRARY``
  The path to the Foo library.

#]=======================================================================]

find_package(PkgConfig)
pkg_check_modules(PC_Numa QUIET numa)

find_path(Numa_INCLUDE_DIR
  NAMES numa.h
  PATHS ${PC_Numa_INCLUDE_DIRS}
  PATH_SUFFIXES Numa
)

find_library(Numa_LIBRARY
  NAMES numa
  PATHS ${PC_Numa_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Numa
  FOUND_VAR
    Numa_FOUND
  REQUIRED_VARS
    Numa_LIBRARY
    Numa_INCLUDE_DIR
)

if(Numa_FOUND)
  set(Numa_LIBRARIES ${Numa_LIBRARY})
  set(Numa_INCLUDE_DIRS ${Numa_INCLUDE_DIR})
  set(Numa_DEFINITIONS ${PC_Numa_CFLAGS_OTHER})
endif()

if(Numa_FOUND AND NOT TARGET Numa::Numa)
  add_library(Numa::Numa UNKNOWN IMPORTED)
  set_target_properties(Numa::Numa PROPERTIES
    IMPORTED_LOCATION "${Numa_LIBRARY}"
    INTERFACE_COMPILE_OPTIONS "${PC_Numa_CFLAGS_OTHER}"
    INTERFACE_INCLUDE_DIRECTORIES "${Numa_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(
  Numa_INCLUDE_DIR
  Numa_LIBRARY
)
