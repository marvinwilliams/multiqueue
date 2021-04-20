set_directory_properties(PROPERTIES EXCLUDE_FROM_ALL TRUE)

add_library(wrapper INTERFACE)
target_include_directories(wrapper INTERFACE "${CMAKE_CURRENT_LIST_DIR}")

add_library(
  capq_wrapper STATIC
  ${EXTERNAL_DIR}/klsm/lib/capq/capq.c
  ${EXTERNAL_DIR}/klsm/lib/capq/fat_skiplist.c
  ${EXTERNAL_DIR}/klsm/lib/capq/qdlocksrc/locks/qd_lock.c
  ${EXTERNAL_DIR}/klsm/lib/capq/qdlocksrc/locks/tatas_lock.c
  ${EXTERNAL_DIR}/klsm/lib/capq/gc/gc.c
  ${EXTERNAL_DIR}/klsm/lib/capq/gc/ptst.c
  capq.cpp
)
target_link_libraries(capq_wrapper PUBLIC wrapper)
target_compile_options(capq_wrapper PRIVATE -w -std=gnu++11)
target_compile_definitions(capq_wrapper PRIVATE INTEL CACHE_LINE_SIZE=${L1_CACHE_LINESIZE})
target_include_directories(
  capq_wrapper PRIVATE
  ${EXTERNAL_DIR}/klsm/lib
  ${EXTERNAL_DIR}/klsm/lib/capq
  ${EXTERNAL_DIR}/klsm/lib/capq/qdlocksrc
)
set_property(TARGET capq_wrapper PROPERTY pq_name wrapper_capq)
target_compile_definitions(capq_wrapper INTERFACE PQ_CAPQ)

add_library(
  klsm_wrapper STATIC
  ${EXTERNAL_DIR}/klsm/src/util/thread_local_ptr.cpp
)
target_link_libraries(klsm_wrapper INTERFACE wrapper)
target_include_directories(
  klsm_wrapper SYSTEM INTERFACE
  ${EXTERNAL_DIR}/klsm/src
  ${EXTERNAL_DIR}/klsm/src/util
)
add_library(klsm256_wrapper INTERFACE)
target_link_libraries(klsm256_wrapper INTERFACE klsm_wrapper)
set_property(TARGET klsm256_wrapper PROPERTY pq_name wrapper_klsm256)
target_compile_definitions(klsm256_wrapper INTERFACE PQ_KLSM256)
add_library(klsm1024_wrapper INTERFACE)
target_link_libraries(klsm1024_wrapper INTERFACE klsm_wrapper)
set_property(TARGET klsm1024_wrapper PROPERTY pq_name wrapper_klsm1024)
target_compile_definitions(klsm1024_wrapper INTERFACE PQ_KLSM1024)

add_library(
  dlsm_wrapper STATIC
  ${EXTERNAL_DIR}/klsm/src/util/thread_local_ptr.cpp
)
target_link_libraries(dlsm_wrapper INTERFACE wrapper)
target_include_directories(
  dlsm_wrapper SYSTEM INTERFACE
  ${EXTERNAL_DIR}/klsm/src
  ${EXTERNAL_DIR}/klsm/src/util
)
set_property(TARGET dlsm_wrapper PROPERTY pq_name wrapper_dlsm)
target_compile_definitions(dlsm_wrapper INTERFACE PQ_DLSM)

add_library(
  linden_wrapper STATIC
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/linden_common.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/linden.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/gc/gc.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/gc/ptst.c
  linden.cpp
)
target_link_libraries(linden_wrapper PUBLIC wrapper)
target_compile_options(linden_wrapper PRIVATE -w -std=gnu++11)
target_compile_definitions(linden_wrapper PRIVATE INTEL CACHE_LINE_SIZE=${L1_CACHE_LINESIZE})
target_include_directories(
  linden_wrapper PRIVATE
  ${EXTERNAL_DIR}/klsm/lib
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/atomic_ops
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/include
)
set_property(TARGET linden_wrapper PROPERTY pq_name wrapper_linden)
target_compile_definitions(linden_wrapper INTERFACE PQ_LINDEN)

add_library(
  spraylist_wrapper STATIC
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/fraser.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/intset.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/linden_common.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/linden.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/measurements.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/pqueue.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/skiplist.c
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/ssalloc.c
  spraylist.cpp
)
target_link_libraries(spraylist_wrapper PUBLIC wrapper)
target_compile_options(spraylist_wrapper PRIVATE -w -std=gnu++11)
target_compile_definitions(spraylist_wrapper PRIVATE INTEL PRIVATE LOCKFREE PRIVATE CACHE_LINE_SIZE=${L1_CACHE_LINESIZE})
target_include_directories(
  spraylist_wrapper PRIVATE
  ${EXTERNAL_DIR}/klsm/lib
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/atomic_ops
  ${EXTERNAL_DIR}/klsm/lib/spraylist_linden/include
)
set_property(TARGET spraylist_wrapper PROPERTY pq_name wrapper_spraylist)
target_compile_definitions(spraylist_wrapper INTERFACE PQ_SPRAYLIST)
