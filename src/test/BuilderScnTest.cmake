add_executable(BuilderScnTest test/BuilderScnTest.cpp)
target_include_directories(BuilderScnTest PUBLIC "${PROJECT_BINARY_DIR}")
olr_test_link(BuilderScnTest)
add_test(NAME BuilderScnTest COMMAND BuilderScnTest)
