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
