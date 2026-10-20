add_executable(BuilderStartScnTest test/BuilderStartScnTest.cpp)
target_include_directories(BuilderStartScnTest PUBLIC "${PROJECT_BINARY_DIR}")
olr_test_link(BuilderStartScnTest)
add_test(NAME BuilderStartScnTest COMMAND BuilderStartScnTest)
