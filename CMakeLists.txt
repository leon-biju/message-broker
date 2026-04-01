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

option(ENABLE_ASAN "Build with AddressSanitizer" OFF)
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

# Dependencies
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
include(cmake/dependencies.cmake)

#Core library (buddy add src files here)
add_library(broker_lib
    src/protocol.cpp
    src/tcp_gateway.cpp
    src/router.cpp
)

target_include_directories(broker_lib PUBLIC include) #Including headers as well

#Remember to link crucial non bench/test only 3rd party libraries here
target_link_libraries(broker_lib PUBLIC
    concurrentqueue
    spdlog::spdlog
    prometheus-cpp::pull
)

# Main executable
add_executable(broker src/main.cpp)
target_link_libraries(broker PRIVATE broker_lib tomlplusplus::tomlplusplus)


# Tools and tests TODO
add_subdirectory(tools)
add_subdirectory(tests)

