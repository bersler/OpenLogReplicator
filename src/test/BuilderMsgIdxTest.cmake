add_executable(BuilderMsgIdxTest test/BuilderMsgIdxTest.cpp)
target_include_directories(BuilderMsgIdxTest PUBLIC "${PROJECT_BINARY_DIR}")
olr_test_link(BuilderMsgIdxTest)
add_test(NAME BuilderMsgIdxTest COMMAND BuilderMsgIdxTest)
