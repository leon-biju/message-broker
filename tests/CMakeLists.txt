enable_testing()

add_executable(broker_tests
    protocol_test.cpp
    router_test.cpp
    outbound_message_test.cpp
)

target_link_libraries(broker_tests PRIVATE
    broker_lib
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(broker_tests)
