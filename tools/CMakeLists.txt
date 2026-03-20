# Demo client
add_executable(client tools/client.cpp)
target_link_libraries(client PRIVATE broker_lib)