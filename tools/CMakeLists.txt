# Demo client
add_executable(client client.cpp)
target_link_libraries(client PRIVATE broker_lib)

# Spam publisher — connects and publishes at a configurable rate
add_executable(publisher publisher.cpp)
target_link_libraries(publisher PRIVATE broker_lib)

# Subscriber — subscribes to a topic and prints/counts received messages
add_executable(subscriber subscriber.cpp)
target_link_libraries(subscriber PRIVATE broker_lib)

# Load generator — multi-publisher/subscriber benchmark with latency histogram
add_executable(load_gen load_gen.cpp)
target_link_libraries(load_gen PRIVATE broker_lib tomlplusplus::tomlplusplus)