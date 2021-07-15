# Multiqueue

The multiqueue is a concurrent, relaxed priority queue.
The idea is that internally there are $`c*p`$ priority queues, where $`p`$ is the
number of threads and $`c`$ is a tuning parameter. Elements are inserted into a
random queue while deletions delete the minimum of two random queues.

# Usage

Clone the repository with
```bash
git clone https://github.com/marvinwilliams/multiqueue
```

You have two options on how to use this library
## (a) Install
Then you can either build and install with
```bash
cmake -B build
cmake --build build
cmake --install build --prefix <prefix>
```

and include it in your cmake project with
```cmake
find_package(multiqueue)
target_link_libaries(target PRIVATE multiqueue::multiqueue)
```

## (b) Subdirectory
Copy the project into your source tree and include it as a subdirectory in cmake
```cmake
add_subdirectory(multiqueue)
target_link_libraries(target PRIVATE multiqueue::multiqueue)
```

# Remarks

The implementation is subject of experimantation and thus has more
customization points than practically desireable.
