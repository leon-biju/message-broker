cmake_minimum_required(VERSION 4.1)

#Disable in-source builds
if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
    message(FATAL_ERROR
            "In-source builds are not allowed. Use a build directory")
endif()

project(message_broker
    VERSION 0.1.0
    DESCRIPTION "A high-performance message broker with subscribe model"
    LANGUAGES CXX
)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Dependencies
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
include(cmake/dependencies.cmake)

#Core library (buddy add src files here)
add_library(broker_lib
    src/protocol.cpp
    src/tcp_gateway.cpp
)

target_include_directories(broker_lib PUBLIC include) #Including headers as well

#Remember to link crucial non bench/test only 3rd party libraries here
target_link_libraries(broker_lib PUBLIC
    concurrentqueue
)

# Main executable
add_executable(broker src/main.cpp)
target_link_libraries(broker PRIVATE broker_lib)


# Tools and tests TODO
#add_subdirectory(tools/load_gen)
add_subdirectory(tools)
add_subdirectory(tests)

