#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#ifdef NDBEBUG
  #error Unit tests must not have NDEBUG defined
#endif
