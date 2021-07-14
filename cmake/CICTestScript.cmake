set(CTEST_SOURCE_DIRECTORY "./")
set(CTEST_BINARY_DIRECTORY "./ctest_build")
set(CTEST_BUILD_TARGET "tests")
set(CTEST_PROJECT_NAME "multiqueue")

if(NOT DEFINED CDASH_TOKEN)
  message(FATAL_ERROR "CDASH_TOKEN not defined but required to upload test results to CDash!")
endif()

ctest_start("Continuous")
ctest_configure()
ctest_build()
ctest_test(RETURN_VALUE TEST_RETURN_VALUE)
#ctest_memcheck()
ctest_submit(HTTPHEADER "Authorization: Bearer ${CDASH_TOKEN}")
if(TEST_RETURN_VALUE)
  message(FATAL_ERROR "CI tests failed!")
endif()
