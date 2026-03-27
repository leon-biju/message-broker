#Fetch content stuff for all third party libraries

include(FetchContent)

FetchContent_Declare(concurrent_queue
    GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
    GIT_TAG        v1.0.4)

FetchContent_Declare(google_test
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0)

FetchContent_Declare(google_benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.9.5)

FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.15.3)

FetchContent_Declare(prometheus_cpp
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
    GIT_TAG        v1.3.0)

FetchContent_Declare(tomlplusplus
    GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
    GIT_TAG        v3.4.0)


FetchContent_MakeAvailable(
        concurrent_queue
        google_test
        google_benchmark
        spdlog
)

FetchContent_MakeAvailable(tomlplusplus)

# Disable prometheus-cpp's bundled test deps avoid gmock target collision
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(prometheus_cpp)

get_directory_property(_targets BUILDSYSTEM_TARGETS)
message(STATUS "Targets: ${_targets}")