#include <iostream>
#include <broker/protocol.hpp>
#include <concurrentqueue.h>

int main() {
    moodycamel::ConcurrentQueue<int> queue;
    queue.enqueue(1);
    std::cout << "Hello, World!" << std::endl;
    return 0;
}